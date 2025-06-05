# EDGAR 10-K Fetcher

This repository provides simple Python utilities for working with the SEC EDGAR system.  It includes a script to download the latest 10‑K filing and another to list the files available for that filing.

## Requirements

- Python 3.11+
- `requests` library

Install dependencies:

```bash
pip install requests beautifulsoup4
```

## Usage

Run the script with a company CIK (Central Index Key):

```bash
python edgar_fetcher.py <CIK>
```

The latest 10‑K document will be saved in the `10k/` directory. Replace `<CIK>` with a valid CIK number (e.g., Apple Inc. is `0000320193`).

To list the files available in the latest 10‑K filing:

```bash
python edgar_files.py <CIK>
```

This prints each file name along with its description, form type, and size.

## Example

```bash
python edgar_fetcher.py 0000320193
```

This command downloads the most recent 10‑K for Apple and stores it locally.
