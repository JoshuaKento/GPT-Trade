# EDGAR 10-K Fetcher

This repository provides simple Python utilities for working with the SEC EDGAR system.  It includes a script to download the latest 10‑K filing and another to list the files available for that filing.  A third helper retrieves the master list of company CIKs and names.

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

Use `--json-out <path>` to write the list to a JSON file instead of printing it.

To download a list of all company CIKs and names:

```bash
python edgar_companies.py > companies.txt
```

The script outputs each CIK and company name on a single line, which can be
redirected to a file for later reference.

## Example

```bash
python edgar_fetcher.py 0000320193
```

This command downloads the most recent 10‑K for Apple and stores it locally.

## Monitoring and S3 Upload

Use `edgar_monitor.py` to check for new filings and upload their documents to an S3 bucket.

```bash
python edgar_monitor.py <CIK> [<CIK> ...] --bucket <bucket-name> [--prefix path/] [--state state.json]
```

The script keeps track of processed accession numbers in the specified state file and uploads each document from new filings to the given S3 bucket.
