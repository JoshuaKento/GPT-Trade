import json
import os
from typing import List, Dict, Optional, Set

import boto3
import requests

from edgar_files import parse_file_list
from edgar_fetcher import cik_to_10digit, get_submissions, HEADERS

SEC_ARCHIVES = "https://www.sec.gov/Archives"


def load_state(path: str) -> Dict[str, Set[str]]:
    if not os.path.exists(path):
        return {}
    with open(path, "r") as fp:
        data = json.load(fp)
    return {cik: set(vals) for cik, vals in data.items()}


def save_state(state: Dict[str, Set[str]], path: str) -> None:
    data = {cik: sorted(list(vals)) for cik, vals in state.items()}
    with open(path, "w") as fp:
        json.dump(data, fp, indent=2)


def list_recent_filings(cik: str) -> List[Dict[str, str]]:
    data = get_submissions(cik)
    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    accession_numbers = recent.get("accessionNumber", [])
    primary_docs = recent.get("primaryDocument", [])
    filings = []
    for form, accession, doc in zip(forms, accession_numbers, primary_docs):
        filings.append({
            "form": form,
            "accession": accession,
            "doc": doc,
        })
    return filings


def get_filing_files(cik: str, accession_number: str) -> List[Dict[str, str]]:
    cik_num = int(cik)
    acc_no_nodash = accession_number.replace('-', '')
    url = f"{SEC_ARCHIVES}/edgar/data/{cik_num}/{acc_no_nodash}/{accession_number}-index.html"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    return parse_file_list(resp.text)


def download_file(url: str) -> bytes:
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    return resp.content


def upload_bytes_to_s3(data: bytes, bucket: str, key: str) -> None:
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=data)


def process_filing(cik: str, filing: Dict[str, str], bucket: str, prefix: str) -> None:
    accession = filing["accession"]
    cik_num = int(cik)
    acc_no_nodash = accession.replace('-', '')
    files = get_filing_files(cik, accession)
    for f in files:
        doc_name = f["document"]
        if not doc_name:
            continue
        file_url = f"{SEC_ARCHIVES}/edgar/data/{cik_num}/{acc_no_nodash}/{doc_name}"
        data = download_file(file_url)
        key = f"{prefix}/{cik}/{accession}/{doc_name}"
        upload_bytes_to_s3(data, bucket, key)


def monitor_cik(cik: str, bucket: str, prefix: str, state: Dict[str, Set[str]]) -> None:
    filings = list_recent_filings(cik)
    processed = state.setdefault(cik, set())
    for filing in filings:
        accession = filing["accession"]
        if accession in processed:
            continue
        process_filing(cik, filing, bucket, prefix)
        processed.add(accession)


def main(ciks: List[str], bucket: str, prefix: str, state_path: str) -> None:
    state = load_state(state_path)
    for cik in ciks:
        monitor_cik(cik, bucket, prefix, state)
    save_state(state, state_path)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Monitor EDGAR for new filings and upload to S3")
    parser.add_argument("ciks", nargs='+', help="CIK codes to monitor")
    parser.add_argument("--bucket", required=True, help="Destination S3 bucket")
    parser.add_argument("--prefix", default="edgar", help="S3 key prefix")
    parser.add_argument("--state", default="edgar_state.json", help="Path to state file")
    args = parser.parse_args()

    main(args.ciks, args.bucket, args.prefix, args.state)
