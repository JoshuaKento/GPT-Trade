# EDGAR 10-K Fetcher

This repository provides a simple Python module to download the latest 10‑K filing from the SEC EDGAR system.

## Requirements

- Python 3.11+
- `requests` library

Install dependencies:

```bash
pip install requests
```

## Usage

Run the script with a company CIK (Central Index Key):

```bash
python edgar_fetcher.py <CIK>
```

The latest 10‑K document will be saved in the `10k/` directory. Replace `<CIK>` with a valid CIK number (e.g., Apple Inc. is `0000320193`).

## Example

```bash
python edgar_fetcher.py 0000320193
```

This command downloads the most recent 10‑K for Apple and stores it locally.
