#!/usr/bin/env python3
"""CLI to download the latest 10-K filing."""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from edgar.filings import fetch_latest_10k


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: fetch_10k.py <CIK>")
        sys.exit(1)
    cik = sys.argv[1]
    path = fetch_latest_10k(cik)
    if path:
        print(f"Saved latest 10-K to {path}")
    else:
        print("No 10-K filing found.")


if __name__ == "__main__":
    main()
