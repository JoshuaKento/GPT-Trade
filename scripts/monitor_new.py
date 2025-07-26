#!/usr/bin/env python3
"""Enhanced CLI to monitor EDGAR for new filings and upload to S3."""
import argparse
import asyncio
import aiohttp
import os
import sys
import logging
from typing import List, Dict, Set
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from edgar.client_new import EdgarClient, ClientConfig
from edgar.filing_processor import FilingProcessor, AsyncFilingProcessor, RateLimiter, monitor_cik_sync
from edgar.s3_manager import S3Manager, S3CredentialsError, S3AccessError, S3NotFoundError
from edgar.config_manager import ConfigManager
from edgar.state import load_state, save_state
from edgar.urls import validate_cik, CIKValidationError
from edgar.logging_config import setup_logging


async def monitor_ciks_async(
    ciks: List[str],
    bucket: str,
    prefix: str,
    state_file: str,
    manifest_key: str,
    config: Dict
) -> None:
    """Monitor multiple CIKs asynchronously for new filings.
    
    Args:
        ciks: List of CIK codes to monitor
        bucket: S3 bucket name
        prefix: S3 key prefix
        state_file: Path to state file
        manifest_key: S3 key for manifest file
        config: Configuration dictionary
    """
    logger = logging.getLogger("monitor")
    
    # Load state
    state = load_state(state_file)
    
    # Initialize S3 manager
    try:
        s3_manager = S3Manager(
            region_name=config.get('s3_region'),
            max_pool_connections=config.get('max_pool_connections', 50)
        )
    except S3CredentialsError as e:
        logger.error(f"S3 credentials error: {e}")
        raise
    
    # Load manifest if specified
    manifest = []
    if manifest_key:
        try:
            manifest = s3_manager.load_json_manifest(bucket, manifest_key)
            logger.info(f"Loaded manifest with {len(manifest)} existing entries")
        except Exception as e:
            logger.warning(f"Could not load manifest: {e}")
    
    # Create rate limiter and processor
    rate_limiter = RateLimiter(int(config.get('rate_limit_per_sec', 6)))
    async_processor = AsyncFilingProcessor(rate_limiter, s3_manager, logger)
    
    # Create semaphore for concurrency control
    max_concurrent = int(config.get('num_workers', 6))
    semaphore = asyncio.Semaphore(max_concurrent)
    
    # HTTP headers for requests
    headers = {
        "User-Agent": config.get('user_agent', 'GPT-Trade-Agent (your-email@example.com)')
    }
    
    form_types = config.get('form_types', [])
    if form_types:
        logger.info(f"Filtering for form types: {', '.join(form_types)}")
    
    # Process each CIK
    timeout = aiohttp.ClientTimeout(total=int(config.get('timeout', 30)))
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = []
        
        for cik in ciks:
            try:
                validate_cik(cik)
                task = monitor_cik_async(
                    session, headers, async_processor, semaphore,
                    cik, bucket, prefix, state, manifest, form_types
                )
                tasks.append(task)
            except CIKValidationError as e:
                logger.error(f"Invalid CIK {cik}: {e}")
                continue
        
        if not tasks:
            logger.error("No valid CIKs to process")
            return
        
        # Execute all monitoring tasks
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"Error during monitoring: {e}")
            raise
    
    # Save state and manifest
    try:
        save_state(state, state_file)
        logger.info(f"Saved state to {state_file}")
        
        if manifest_key and manifest:
            s3_manager.save_json_manifest(manifest, bucket, manifest_key)
            logger.info(f"Saved manifest with {len(manifest)} entries")
            
    except Exception as e:
        logger.error(f"Failed to save state/manifest: {e}")


async def monitor_cik_async(
    session: aiohttp.ClientSession,
    headers: Dict[str, str],
    processor: AsyncFilingProcessor,
    semaphore: asyncio.Semaphore,
    cik: str,
    bucket: str,
    prefix: str,
    state: Dict[str, Set[str]],
    manifest: List[Dict],
    form_types: List[str]
) -> None:
    """Monitor a single CIK asynchronously.
    
    Args:
        session: aiohttp session
        headers: HTTP headers
        processor: Async filing processor
        semaphore: Concurrency limiter
        cik: Central Index Key
        bucket: S3 bucket name
        prefix: S3 key prefix
        state: Processing state
        manifest: Manifest list
        form_types: Form types to filter
    """
    logger = logging.getLogger("monitor")
    logger.info(f"Checking CIK {cik}")
    
    try:
        # Get recent filings using synchronous client (for simplicity)
        config_manager = ConfigManager()
        config = config_manager.get_config()
        
        client_config = ClientConfig(
            rate_limit_per_sec=config.rate_limit_per_sec,
            timeout=config.timeout,
            user_agent=config.user_agent
        )
        
        with EdgarClient(client_config) as client:
            sync_processor = FilingProcessor(client, None, logger)
            processed = state.setdefault(cik, set())
            new_filings = sync_processor.get_new_filings(cik, processed, form_types)
        
        if not new_filings:
            logger.info(f"No new filings for {cik}")
            return
        
        logger.info(f"Processing {len(new_filings)} new filings for CIK {cik}")
        
        # Process filings asynchronously
        filing_tasks = []
        for filing in new_filings:
            task = processor.process_filing_async(
                session, headers, cik, filing, bucket, prefix, semaphore
            )
            filing_tasks.append(task)
        
        # Wait for all filings to complete
        results = await asyncio.gather(*filing_tasks, return_exceptions=True)
        
        # Process results
        successful = 0
        total_docs = 0
        
        for filing, result in zip(new_filings, results):
            if isinstance(result, Exception):
                logger.error(f"Filing {filing.accession} failed: {result}")
                continue
            
            if result.success:
                processed.add(result.accession)
                successful += 1
                total_docs += result.documents_processed
                
                # Add to manifest
                for uploaded_file in result.uploaded_files or []:
                    manifest.append({
                        "cik": cik,
                        "accession": result.accession,
                        "form": filing.form,
                        "key": uploaded_file,
                        "filing_date": filing.filing_date
                    })
                
                logger.info(f"Processed {result.accession}: {result.documents_processed} documents")
            else:
                logger.error(f"Failed {result.accession}: {result.error}")
        
        logger.info(f"CIK {cik}: {successful}/{len(new_filings)} filings, {total_docs} documents")
        
    except Exception as e:
        logger.error(f"Error monitoring CIK {cik}: {e}")


def monitor_ciks_sync(
    ciks: List[str],
    bucket: str,
    prefix: str,
    state_file: str,
    manifest_key: str,
    config: Dict
) -> None:
    """Monitor multiple CIKs synchronously (fallback method).
    
    Args:
        ciks: List of CIK codes to monitor
        bucket: S3 bucket name
        prefix: S3 key prefix
        state_file: Path to state file
        manifest_key: S3 key for manifest file
        config: Configuration dictionary
    """
    logger = logging.getLogger("monitor")
    
    # Load state
    state = load_state(state_file)
    
    # Initialize managers
    try:
        s3_manager = S3Manager(
            region_name=config.get('s3_region'),
            max_pool_connections=config.get('max_pool_connections', 50)
        )
    except S3CredentialsError as e:
        logger.error(f"S3 credentials error: {e}")
        raise
    
    client_config = ClientConfig(
        rate_limit_per_sec=config.get('rate_limit_per_sec', 6.0),
        timeout=config.get('timeout', 30),
        user_agent=config.get('user_agent', 'GPT-Trade-Agent (your-email@example.com)'),
        max_retries=config.get('max_retries', 3),
        backoff_factor=config.get('backoff_factor', 0.5)
    )
    
    form_types = config.get('form_types', [])
    
    with EdgarClient(client_config) as client:
        processor = FilingProcessor(client, s3_manager, logger)
        
        for cik in ciks:
            try:
                validate_cik(cik)
                monitor_cik_sync(processor, cik, bucket, prefix, state, form_types)
            except CIKValidationError as e:
                logger.error(f"Invalid CIK {cik}: {e}")
                continue
            except Exception as e:
                logger.error(f"Error processing CIK {cik}: {e}")
                continue
    
    # Save state
    try:
        save_state(state, state_file)
        logger.info(f"Saved state to {state_file}")
    except Exception as e:
        logger.error(f"Failed to save state: {e}")


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Monitor EDGAR for new filings and upload to S3",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s 0000320193 --bucket my-bucket
  %(prog)s 0000320193 0000789019 --bucket my-bucket --prefix edgar-data
  %(prog)s 0000320193 --bucket my-bucket --manifest manifests/manifest.json
  %(prog)s 0000320193 --bucket my-bucket --config config.json --async
        """
    )
    
    parser.add_argument(
        "ciks",
        nargs='+',
        help="CIK codes to monitor"
    )
    
    parser.add_argument(
        "--bucket",
        required=True,
        help="Destination S3 bucket"
    )
    
    parser.add_argument(
        "--prefix",
        help="S3 key prefix (default from config)"
    )
    
    parser.add_argument(
        "--state",
        default="edgar_state.json",
        help="Path to state file (default: edgar_state.json)"
    )
    
    parser.add_argument(
        "--manifest",
        metavar="KEY",
        help="S3 key for manifest JSON file"
    )
    
    parser.add_argument(
        "--config",
        help="Path to configuration JSON file"
    )
    
    parser.add_argument(
        "--async",
        action="store_true",
        dest="use_async",
        help="Use asynchronous processing (faster)"
    )
    
    parser.add_argument(
        "--sync",
        action="store_false",
        dest="use_async",
        help="Use synchronous processing (more stable)"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be processed without uploading"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = "DEBUG" if args.verbose else "INFO"
    os.environ.setdefault("LOG_LEVEL", log_level)
    setup_logging()
    
    logger = logging.getLogger("monitor")
    
    try:
        # Load configuration
        config_manager = ConfigManager()
        if args.config:
            config_manager.load_config(args.config)
        config = config_manager.get_config().to_dict()
        
        # Override prefix if specified
        if args.prefix:
            config['s3_prefix'] = args.prefix
        
        prefix = config.get('s3_prefix', 'edgar')
        
        # Validate bucket access if not dry run
        if not args.dry_run:
            try:
                s3_manager = S3Manager()
                # Test bucket access
                s3_manager.list_objects(args.bucket, max_keys=1)
                logger.info(f"Confirmed access to S3 bucket: {args.bucket}")
            except S3NotFoundError:
                logger.error(f"S3 bucket '{args.bucket}' does not exist")
                sys.exit(1)
            except S3AccessError:
                logger.error(f"Access denied to S3 bucket '{args.bucket}'")
                sys.exit(1)
            except S3CredentialsError as e:
                logger.error(f"S3 credentials error: {e}")
                sys.exit(1)
        
        # Show configuration
        logger.info(f"Configuration:")
        logger.info(f"  Bucket: {args.bucket}")
        logger.info(f"  Prefix: {prefix}")
        logger.info(f"  Rate limit: {config.get('rate_limit_per_sec', 6)}/sec")
        logger.info(f"  Workers: {config.get('num_workers', 6)}")
        logger.info(f"  Form types: {config.get('form_types', 'all')}")
        logger.info(f"  Processing mode: {'async' if args.use_async else 'sync'}")
        
        if args.dry_run:
            logger.info("DRY RUN MODE - No files will be uploaded")
            return
        
        # Run monitoring
        if args.use_async:
            asyncio.run(monitor_ciks_async(
                args.ciks, args.bucket, prefix, args.state, args.manifest, config
            ))
        else:
            monitor_ciks_sync(
                args.ciks, args.bucket, prefix, args.state, args.manifest, config
            )
        
        logger.info("Monitoring completed successfully")
        
    except KeyboardInterrupt:
        logger.info("Monitoring interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Monitoring failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()