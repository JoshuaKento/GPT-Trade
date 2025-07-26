#!/usr/bin/env python3
"""CLI to list files in the latest 10-K filing (MIGRATED TO NEW ARCHITECTURE)."""
import argparse
import json
import os
import sys
import logging
import warnings

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Issue deprecation warning
warnings.warn(
    "This script is deprecated. Use 'scripts/list_files_new.py' for enhanced features and better error handling.",
    DeprecationWarning,
    stacklevel=2
)

# Import new architecture components
from edgar.client_new import EdgarClient, ClientConfig
from edgar.filing_processor import FilingProcessor
from edgar.config_manager import ConfigManager
from edgar.urls import validate_cik, CIKValidationError
from edgar.logging_config import setup_logging


def main() -> None:
    """Main entry point using new architecture."""
    setup_logging()
    logger = logging.getLogger("list_files")
    
    parser = argparse.ArgumentParser(description="List filing files")
    parser.add_argument("cik", help="Company CIK")
    parser.add_argument("--json-out", metavar="PATH", help="Write JSON output")
    args = parser.parse_args()

    print("Note: This script is deprecated. Use list_files_new.py for enhanced features.")
    
    try:
        # Validate CIK early
        validate_cik(args.cik)
        
        # Load configuration and create client
        config_manager = ConfigManager()
        config = config_manager.get_config()
        
        client_config = ClientConfig(
            rate_limit_per_sec=config.rate_limit_per_sec,
            timeout=config.timeout,
            user_agent=config.user_agent
        )
        
        # Use new architecture to list files
        with EdgarClient(client_config) as client:
            processor = FilingProcessor(client, None, logger)
            
            # Get recent filings and find latest 10-K
            filings = processor.get_recent_filings(args.cik)
            ten_k_filing = None
            
            for filing in filings:
                if filing.form == "10-K":
                    ten_k_filing = filing
                    break
            
            if not ten_k_filing:
                logger.warning("No 10-K filing found")
                return
            
            # Get documents in the filing
            files = processor.get_filing_documents(args.cik, ten_k_filing.accession)
            
            if args.json_out:
                with open(args.json_out, "w") as fp:
                    json.dump(files, fp, indent=2)
                logger.info("Wrote JSON output to %s", args.json_out)
            else:
                for f in files:
                    print(f["document"], f["description"], f["type"], f["size"])
                    
    except CIKValidationError as e:
        logger.error("Invalid CIK: %s", e)
        sys.exit(1)
    except Exception as e:
        logger.error("Error: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
