#!/usr/bin/env python3
"""CLI to list all companies with CIK codes."""
import argparse
import os
import sys
from typing import Optional, Sequence

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from edgar.companies import fetch_cik_company_list
from edgar.logging_config import setup_logging


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser."""
    return argparse.ArgumentParser(
        description="List all SEC companies with CIK codes.",
    )


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = build_parser()
    parser.parse_args(argv)

    setup_logging()
    comps = fetch_cik_company_list()
    try:
        for c in comps:
            print(c["cik"], c["name"])
    except BrokenPipeError:
        pass


if __name__ == "__main__":
    main()
