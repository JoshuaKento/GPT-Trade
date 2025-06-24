#!/usr/bin/env python3
"""CLI to list files in the latest 10-K filing."""
import argparse
import json
import os
import sys
import logging
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from edgar.filings import list_recent_filings, get_filing_files
from edgar.logging_config import setup_logging


def main() -> None:
    setup_logging()
    parser = argparse.ArgumentParser(description="List filing files")
    parser.add_argument("cik", help="Company CIK")
    parser.add_argument("--json-out", metavar="PATH", help="Write JSON output")
    args = parser.parse_args()

    filings = list_recent_filings(args.cik)
    accession = next((f["accession"] for f in filings if f["form"] == "10-K"), None)
    logger = logging.getLogger("list_files")
    if not accession:
        logger.warning("No filing found")
        return
    files = get_filing_files(args.cik, accession)
    if args.json_out:
        with open(args.json_out, "w") as fp:
            json.dump(files, fp, indent=2)
        logger.info("Wrote JSON output to %s", args.json_out)
    else:
        for f in files:
            print(f["document"], f["description"], f["type"], f["size"])


if __name__ == "__main__":
    main()
