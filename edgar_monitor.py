import json
import os
from typing import List, Dict, Optional, Set
from concurrent.futures import ThreadPoolExecutor, wait

from tqdm import tqdm

import boto3
from edgar_fetcher import sec_get

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
    resp = sec_get(url)
    resp.raise_for_status()
    return parse_file_list(resp.text)


def download_file(url: str) -> bytes:
    resp = sec_get(url)
    resp.raise_for_status()
    return resp.content


def upload_bytes_to_s3(data: bytes, bucket: str, key: str) -> None:
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=data)


def download_and_upload_file(
    cik: str,
    accession: str,
    doc_name: str,
    bucket: str,
    prefix: str,
    bar: tqdm,
) -> None:
    """Download a single document and upload it to S3."""
    cik_num = int(cik)
    acc_no_nodash = accession.replace("-", "")
    file_url = f"{SEC_ARCHIVES}/edgar/data/{cik_num}/{acc_no_nodash}/{doc_name}"
    try:
        data = download_file(file_url)
    except Exception as exc:
        tqdm.write(f"Failed to download {file_url}: {exc}")
        bar.update(1)
        return
    key = f"{prefix}/{cik}/{accession}/{doc_name}"
    try:
        upload_bytes_to_s3(data, bucket, key)
    except Exception as exc:
        tqdm.write(f"Failed to upload {key} to S3: {exc}")
    bar.set_postfix(file=doc_name)
    bar.update(1)




def monitor_cik(
    cik: str,
    bucket: str,
    prefix: str,
    state: Dict[str, Set[str]],
) -> None:
    filings = list_recent_filings(cik)
    processed = state.setdefault(cik, set())

    # Collect file lists first to determine overall progress
    filing_files: List[Dict[str, List[str]]] = []
    total_files = 0
    for filing in filings:
        accession = filing["accession"]
        if accession in processed:
            continue
        files = get_filing_files(cik, accession)
        doc_names = [f.get("document") for f in files if f.get("document")]
        if not doc_names:
            processed.add(accession)
            continue
        filing_files.append({"accession": accession, "docs": doc_names})
        total_files += len(doc_names)

    if total_files == 0:
        return

    with tqdm(total=total_files, unit="file", desc=f"CIK {cik}") as bar:
        with ThreadPoolExecutor(max_workers=6) as executor:
            for ff in filing_files:
                accession = ff["accession"]
                for doc_name in ff["docs"]:
                    executor.submit(
                        download_and_upload_file,
                        cik,
                        accession,
                        doc_name,
                        bucket,
                        prefix,
                        bar,
                    )
            executor.shutdown(wait=True)
    for ff in filing_files:
        processed.add(ff["accession"])


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
