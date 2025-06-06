#!/usr/bin/env python3
"""CLI to list all companies with CIK codes."""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from edgar.companies import fetch_cik_company_list


def main() -> None:
    comps = fetch_cik_company_list()
    try:
        for c in comps:
            print(c["cik"], c["name"])
    except BrokenPipeError:
        pass


if __name__ == "__main__":
    main()
