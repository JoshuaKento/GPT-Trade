#!/usr/bin/env python3
"""CLI to download the latest 10-K filing (MIGRATED TO NEW ARCHITECTURE)."""
import logging
import os
import sys
import warnings

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Issue deprecation warning
warnings.warn(
    "This script is deprecated. Use 'scripts/fetch_10k_new.py' for enhanced features and better error handling.",
    DeprecationWarning,
    stacklevel=2,
)

from pathlib import Path

# Import new architecture components
from edgar.client_new import ClientConfig, EdgarClient
from edgar.config_manager import ConfigManager
from edgar.filing_processor import FilingProcessor
from edgar.logging_config import setup_logging
from edgar.urls import CIKValidationError, URLBuilder, validate_cik


def main() -> None:
    """Main entry point using new architecture."""
    setup_logging()
    logger = logging.getLogger("fetch_10k")

    if len(sys.argv) < 2:
        print("Usage: fetch_10k.py <CIK>")
        print("Note: This script is deprecated. Use fetch_10k_new.py instead.")
        sys.exit(1)

    cik = sys.argv[1]

    try:
        # Validate CIK early
        validate_cik(cik)

        # Load configuration and create client
        config_manager = ConfigManager()
        config = config_manager.get_config()

        client_config = ClientConfig(
            rate_limit_per_sec=config.rate_limit_per_sec,
            timeout=config.timeout,
            user_agent=config.user_agent,
        )

        # Use new architecture to fetch filing
        with EdgarClient(client_config) as client:
            processor = FilingProcessor(client, None, logger)

            # Get recent filings and find latest 10-K
            filings = processor.get_recent_filings(cik)
            ten_k_filing = None

            for filing in filings:
                if filing.form == "10-K":
                    ten_k_filing = filing
                    break

            if not ten_k_filing:
                logger.warning("No 10-K filing found.")
                return

            # Download primary document using new URL builder
            download_dir = Path("10k")
            download_dir.mkdir(parents=True, exist_ok=True)

            doc_url = URLBuilder.document_url(
                cik, ten_k_filing.accession, ten_k_filing.primary_document
            )
            response = client.get(doc_url)

            local_path = download_dir / ten_k_filing.primary_document
            with open(local_path, "wb") as f:
                f.write(response.content)

            logger.info("Saved latest 10-K to %s", local_path)

    except CIKValidationError as e:
        logger.error("Invalid CIK: %s", e)
        sys.exit(1)
    except Exception as e:
        logger.error("Error: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
