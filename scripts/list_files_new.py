#!/usr/bin/env python3
"""Enhanced CLI to list files in the latest 10-K filing with improved error handling."""
import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from edgar.client_new import ClientConfig, EdgarClient
from edgar.config_manager import ConfigManager
from edgar.filing_processor import FilingProcessor
from edgar.logging_config import setup_logging
from edgar.urls import CIKValidationError, validate_cik


def list_filing_files(
    cik: str, form_type: str = "10-K", output_format: str = "table"
) -> List[Dict[str, str]]:
    """List files in the latest filing of specified type.

    Args:
        cik: Central Index Key
        form_type: Type of filing to find (default: 10-K)
        output_format: Output format ('table' or 'json')

    Returns:
        List of file information dictionaries

    Raises:
        ValueError: If no filing found or invalid parameters
        RuntimeError: If API request fails
    """
    logger = logging.getLogger("list_files")

    # Load configuration
    config_manager = ConfigManager()
    config = config_manager.get_config()

    # Create client
    client_config = ClientConfig(
        rate_limit_per_sec=config.rate_limit_per_sec,
        timeout=config.timeout,
        user_agent=config.user_agent,
        max_retries=config.max_retries,
        backoff_factor=config.backoff_factor,
    )

    with EdgarClient(client_config) as client:
        # Create processor
        processor = FilingProcessor(client, None, logger)

        try:
            # Get recent filings
            filings = processor.get_recent_filings(cik)

            # Find latest filing of specified type
            target_filing = None
            for filing in filings:
                if filing.form.upper() == form_type.upper():
                    target_filing = filing
                    break

            if not target_filing:
                raise ValueError(f"No {form_type} filing found for CIK {cik}")

            logger.info(f"Found {form_type} filing: {target_filing.accession}")

            # Get documents in the filing
            documents = processor.get_filing_documents(cik, target_filing.accession)

            if not documents:
                logger.warning("No documents found in filing")
                return []

            logger.info(f"Found {len(documents)} documents in filing")
            return documents

        except CIKValidationError as e:
            logger.error(f"Invalid CIK: {e}")
            raise ValueError(f"Invalid CIK: {e}")
        except Exception as e:
            logger.error(f"Failed to list files: {e}")
            raise RuntimeError(f"Failed to list files: {e}")


def format_output(
    documents: List[Dict[str, str]],
    output_format: str,
    output_file: Optional[str] = None,
) -> None:
    """Format and output the document list.

    Args:
        documents: List of document information
        output_format: Format type ('table', 'json', 'csv')
        output_file: Optional output file path
    """
    if not documents:
        print("No documents found.")
        return

    if output_format.lower() == "json":
        output_data = json.dumps(documents, indent=2)
    elif output_format.lower() == "csv":
        import csv
        import io

        output = io.StringIO()
        fieldnames = ["sequence", "description", "document", "type", "size"]
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()

        for doc in documents:
            row = {key: doc.get(key, "") for key in fieldnames}
            writer.writerow(row)

        output_data = output.getvalue()
    else:  # table format
        # Calculate column widths
        headers = ["Seq", "Description", "Document", "Type", "Size"]
        col_widths = [len(h) for h in headers]

        for doc in documents:
            col_widths[0] = max(col_widths[0], len(doc.get("sequence", "")))
            col_widths[1] = max(col_widths[1], len(doc.get("description", "")))
            col_widths[2] = max(col_widths[2], len(doc.get("document", "")))
            col_widths[3] = max(col_widths[3], len(doc.get("type", "")))
            col_widths[4] = max(col_widths[4], len(doc.get("size", "")))

        # Create table
        lines = []

        # Header
        header_line = " | ".join(
            header.ljust(width) for header, width in zip(headers, col_widths)
        )
        lines.append(header_line)
        lines.append("-" * len(header_line))

        # Data rows
        for doc in documents:
            row_data = [
                doc.get("sequence", "").ljust(col_widths[0]),
                doc.get("description", "").ljust(col_widths[1]),
                doc.get("document", "").ljust(col_widths[2]),
                doc.get("type", "").ljust(col_widths[3]),
                doc.get("size", "").ljust(col_widths[4]),
            ]
            lines.append(" | ".join(row_data))

        output_data = "\\n".join(lines)

    # Output to file or stdout
    if output_file:
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(output_data)
        print(f"Output saved to {output_file}")
    else:
        print(output_data)


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="List files in the latest SEC filing for a company",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s 0000320193                        # List files in latest 10-K
  %(prog)s 0000320193 --form 10-Q            # List files in latest 10-Q
  %(prog)s 0000320193 --json                 # Output as JSON
  %(prog)s 0000320193 --output files.json    # Save to file
  %(prog)s 0000320193 --csv --output data.csv # Save as CSV
        """,
    )

    parser.add_argument("cik", help="Company Central Index Key (CIK)")

    parser.add_argument(
        "--form", default="10-K", help="Filing form type to search for (default: 10-K)"
    )

    parser.add_argument(
        "--json",
        action="store_const",
        const="json",
        dest="format",
        help="Output as JSON",
    )

    parser.add_argument(
        "--csv", action="store_const", const="csv", dest="format", help="Output as CSV"
    )

    parser.add_argument(
        "--format",
        choices=["table", "json", "csv"],
        default="table",
        help="Output format (default: table)",
    )

    parser.add_argument("--output", "-o", help="Output file path")

    parser.add_argument("--config", help="Path to configuration JSON file")

    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    # Setup logging
    log_level = "DEBUG" if args.verbose else "INFO"
    os.environ.setdefault("LOG_LEVEL", log_level)
    setup_logging()

    logger = logging.getLogger("list_files")

    try:
        # Validate CIK early
        validate_cik(args.cik)

        # Load configuration if specified
        if args.config:
            config_manager = ConfigManager()
            config_manager.load_config(args.config)

        # Get filing files
        documents = list_filing_files(args.cik, args.form, args.format)

        # Format and output results
        format_output(documents, args.format, args.output)

        logger.info(f"Listed {len(documents)} documents from {args.form} filing")

    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Operation interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
