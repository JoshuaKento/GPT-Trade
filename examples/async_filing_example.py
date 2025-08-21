#!/usr/bin/env python3
"""
Example demonstrating proper async filing processor usage with resource management.

This example shows how to use the improved AsyncFilingProcessor with proper
session lifecycle management and connection leak prevention.
"""
import asyncio
import logging
import os
import sys
from typing import List

import aiohttp

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from edgar.filing_processor import AsyncFilingProcessor, FilingInfo, RateLimiter
from edgar.s3_manager import S3Manager


async def process_filings_example():
    """Example of processing filings with proper resource management."""

    # Setup logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Mock some filing data for demonstration
    sample_filings = [
        FilingInfo(
            form="10-K",
            accession="0000123456-23-000001",
            primary_document="filing.htm",
            filing_date="2023-01-15",
            report_date="2022-12-31",
        ),
        FilingInfo(
            form="10-Q",
            accession="0000123456-23-000002",
            primary_document="filing.htm",
            filing_date="2023-04-15",
            report_date="2023-03-31",
        ),
    ]

    # Configuration
    rate_limiter = RateLimiter(6)  # 6 requests per second
    s3_manager = S3Manager()  # Uses default AWS credentials

    headers = {"User-Agent": "GPT-Trade-Example (contact@example.com)"}

    timeout = aiohttp.ClientTimeout(
        total=30,  # Total timeout
        connect=10,  # Connection timeout
        sock_read=15,  # Socket read timeout
    )

    # Example 1: Using the factory method (recommended)
    logger.info("Example 1: Using factory method for automatic resource management")

    async with AsyncFilingProcessor.create_processor(
        rate_limiter=rate_limiter,
        s3_manager=s3_manager,
        headers=headers,
        timeout=timeout,
        logger=logger,
    ) as processor:

        # Create concurrency semaphore
        semaphore = asyncio.Semaphore(3)  # Max 3 concurrent operations

        # Process filings
        tasks = []
        for filing in sample_filings:
            task = processor.process_filing_async(
                cik="0000123456",
                filing=filing,
                bucket="my-edgar-bucket",  # Replace with your bucket
                prefix="edgar-data",
                semaphore=semaphore,
            )
            tasks.append(task)

        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        for filing, result in zip(sample_filings, results):
            if isinstance(result, Exception):
                logger.error(f"Filing {filing.accession} failed: {result}")
            else:
                logger.info(
                    f"Filing {filing.accession}: {result.success}, "
                    f"documents: {result.documents_processed}"
                )

    logger.info("Example 1 completed - resources automatically cleaned up")

    # Example 2: Manual context management (advanced usage)
    logger.info("\nExample 2: Manual context management")

    processor = AsyncFilingProcessor(
        rate_limiter=rate_limiter,
        s3_manager=s3_manager,
        headers=headers,
        timeout=timeout,
        logger=logger,
    )

    try:
        async with processor:
            # Session is now available
            assert processor.session is not None

            # Use processor for operations...
            logger.info("Processor session initialized manually")

    except Exception as e:
        logger.error(f"Error in manual context management: {e}")
    finally:
        # Cleanup is automatic via __aexit__, but can be called explicitly
        await processor.cleanup()

    logger.info("Example 2 completed - manual cleanup performed")


async def demonstrate_error_handling():
    """Demonstrate proper error handling and resource cleanup."""

    logger = logging.getLogger(__name__)
    logger.info("\nDemonstrating error handling with resource cleanup")

    rate_limiter = RateLimiter(6)
    s3_manager = S3Manager()

    try:
        async with AsyncFilingProcessor.create_processor(
            rate_limiter=rate_limiter, s3_manager=s3_manager, logger=logger
        ) as processor:

            # Simulate an error scenario
            logger.info("Simulating an error...")
            raise ValueError("Simulated processing error")

    except ValueError as e:
        logger.info(f"Caught expected error: {e}")
        logger.info("Resources were automatically cleaned up despite the error")

    logger.info("Error handling demonstration completed")


async def main():
    """Run all examples."""
    print("AsyncFilingProcessor Resource Management Examples")
    print("=" * 50)

    try:
        await process_filings_example()
        await demonstrate_error_handling()

        print("\n" + "=" * 50)
        print("All examples completed successfully!")
        print("\nKey benefits of the improved implementation:")
        print("- Automatic session lifecycle management")
        print("- Proper connection cleanup prevents leaks")
        print("- Context manager ensures resources are freed")
        print("- Configurable timeouts and connection limits")
        print("- Exception-safe resource handling")

    except Exception as e:
        print(f"Example failed: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
