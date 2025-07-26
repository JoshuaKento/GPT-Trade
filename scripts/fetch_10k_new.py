#!/usr/bin/env python3
"""Enhanced CLI to download the latest 10-K filing with improved error handling."""
import os
import sys
import argparse
import logging
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from edgar.client_new import EdgarClient, ClientConfig
from edgar.filing_processor import FilingProcessor
from edgar.s3_manager import S3Manager
from edgar.config_manager import ConfigManager
from edgar.urls import validate_cik, CIKValidationError
from edgar.logging_config import setup_logging


def download_latest_10k(cik: str, download_dir: str = "10k") -> str:
    """Download the latest 10-K filing for a CIK.
    
    Args:
        cik: Central Index Key
        download_dir: Directory to save the filing
        
    Returns:
        Path to downloaded file
        
    Raises:
        ValueError: If no 10-K filing found
        RuntimeError: If download fails
    """
    logger = logging.getLogger("fetch_10k")
    
    # Load configuration
    config_manager = ConfigManager()
    config = config_manager.get_config()
    
    # Create client
    client_config = ClientConfig(
        rate_limit_per_sec=config.rate_limit_per_sec,
        timeout=config.timeout,
        user_agent=config.user_agent,
        max_retries=config.max_retries,
        backoff_factor=config.backoff_factor
    )
    
    with EdgarClient(client_config) as client:
        # Create processor (no S3 needed for direct download)
        processor = FilingProcessor(client, None, logger)
        
        try:
            # Get recent filings
            filings = processor.get_recent_filings(cik)
            
            # Find latest 10-K
            ten_k_filing = None
            for filing in filings:
                if filing.form == "10-K":
                    ten_k_filing = filing
                    break
            
            if not ten_k_filing:
                raise ValueError(f"No 10-K filing found for CIK {cik}")
            
            logger.info(f"Found 10-K filing: {ten_k_filing.accession}")
            
            # Create download directory
            download_path = Path(download_dir)
            download_path.mkdir(parents=True, exist_ok=True)
            
            # Get primary document URL and download
            from edgar.urls import URLBuilder
            primary_doc = ten_k_filing.primary_document
            doc_url = URLBuilder.document_url(cik, ten_k_filing.accession, primary_doc)
            
            response = client.get(doc_url)
            
            # Save to file
            local_path = download_path / primary_doc
            with open(local_path, 'wb') as f:
                f.write(response.content)
            
            logger.info(f"Downloaded {primary_doc} ({len(response.content)} bytes)")
            return str(local_path)
            
        except CIKValidationError as e:
            logger.error(f"Invalid CIK: {e}")
            raise ValueError(f"Invalid CIK: {e}")
        except Exception as e:
            logger.error(f"Download failed: {e}")
            raise RuntimeError(f"Download failed: {e}")


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Download the latest 10-K filing for a company",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s 0000320193                    # Download Apple's latest 10-K
  %(prog)s 0000320193 --dir apple_10k    # Save to specific directory
  %(prog)s 0000320193 --config myconfig.json  # Use custom config
        """
    )
    
    parser.add_argument(
        "cik",
        help="Company Central Index Key (CIK)"
    )
    
    parser.add_argument(
        "--dir",
        default="10k",
        help="Directory to save the filing (default: 10k)"
    )
    
    parser.add_argument(
        "--config",
        help="Path to configuration JSON file"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = "DEBUG" if args.verbose else "INFO"
    os.environ.setdefault("LOG_LEVEL", log_level)
    setup_logging()
    
    logger = logging.getLogger("fetch_10k")
    
    try:
        # Validate CIK early
        validate_cik(args.cik)
        
        # Load configuration if specified
        if args.config:
            config_manager = ConfigManager()
            config_manager.load_config(args.config)
        
        # Download filing
        path = download_latest_10k(args.cik, args.dir)
        logger.info(f"Successfully saved latest 10-K to {path}")
        print(f"Downloaded: {path}")
        
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Download interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()