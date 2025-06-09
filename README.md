# EDGAR 10-K Fetcher

This repository provides Python utilities for working with the SEC EDGAR system.
It now exposes a package in `edgar/` and command-line tools under `scripts/`.
The tools can download the latest 10‑K filing, list its files, fetch the company
CIK list, and monitor EDGAR for new filings while uploading them to S3.

All scripts throttle requests to a configurable rate (default **six per second**) to remain within the SEC guidelines.

Set the environment variable `SEC_USER_AGENT` to a string containing your contact email. The SEC requires a valid User-Agent header for automated requests.

Optional settings can be placed in a JSON file (default `config.json`). Use the
`--config` option in `monitor.py` to load it. Supported keys include
`rate_limit_per_sec`, `num_workers`, and `s3_prefix`.

Logging output defaults to INFO level; set `LOG_LEVEL=DEBUG` for verbose logs.

## Requirements

- Python 3.11+
- `requests` library
- `beautifulsoup4`
- `boto3`
- `aiohttp`
- `tqdm` (optional, provides progress bars)

Install dependencies:

```bash
pip install requests beautifulsoup4 boto3 aiohttp tqdm
```

Alternatively install the package and scripts with:

```bash
pip install -e .
```

If `tqdm` is missing, progress bars are disabled but the scripts still run.

## Usage

Run the script with a company CIK (Central Index Key):

```bash
python scripts/fetch_10k.py <CIK>
```

The latest 10‑K document will be saved in the `10k/` directory. Replace `<CIK>` with a valid CIK number (e.g., Apple Inc. is `0000320193`).

To list the files available in the latest 10‑K filing:

```bash
python scripts/list_files.py <CIK>
```
This prints each file name along with its description, form type, and size.

Use `--json-out <path>` to write the list to a JSON file instead of printing it.

To download a list of all company CIKs and names:

```bash
python scripts/companies.py > companies.txt
```

The script outputs each CIK and company name on a single line, which can be
redirected to a file for later reference.

## Example

```bash
python scripts/fetch_10k.py 0000320193
```

This command downloads the most recent 10‑K for Apple and stores it locally.

## Monitoring and S3 Upload

Use `scripts/monitor.py` to check for new filings and upload their documents to an S3 bucket.

```bash
python scripts/monitor.py <CIK> [<CIK> ...] --bucket <bucket-name> [--prefix path/] \
                         [--state state.json] [--manifest manifest.json]
```

The script keeps track of processed accession numbers in the specified state file and uploads each document from new filings to the given S3 bucket.
Downloads use `aiohttp` with an asynchronous rate limiter so multiple files are fetched in parallel while respecting the configured requests-per-second limit. A single progress bar shows overall progress across all documents and displays the most recently handled file name.

If you pass `--manifest`, the JSON file at that S3 key is read at startup,
updated with any newly uploaded documents, and written back when the run
finishes. This serves as a catalog of everything retrieved so far.

Example:

```bash
python scripts/monitor.py 0000320193 --bucket my-bucket --manifest manifests/apple.json
```

```
 /\\_/\\
(=^.^=)
 /     \
JoshuaKent
```