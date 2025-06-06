import json
import os
from typing import List, Dict, Optional, Set
from concurrent.futures import ThreadPoolExecutor
import threading

from tqdm import tqdm

import boto3
from edgar_fetcher import sec_get, get_submissions, SEC_ARCHIVES
from edgar_files import parse_file_list


manifest_lock = threading.Lock()


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


def load_manifest(bucket: str, key: str) -> List[Dict[str, str]]:
    """Load manifest JSON from S3 or return empty list if missing."""
    s3 = boto3.client("s3")
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        data = resp["Body"].read()
        return json.loads(data)
    except Exception:
        return []


def save_manifest(manifest: List[Dict[str, str]], bucket: str, key: str) -> None:
    s3 = boto3.client("s3")
    body = json.dumps(manifest, indent=2).encode("utf-8")
    s3.put_object(Bucket=bucket, Key=key, Body=body)


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
    manifest: Optional[List[Dict[str, str]]] = None,
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
    else:
        if manifest is not None:
            with manifest_lock:
                manifest.append({
                    "cik": cik,
                    "accession": accession,
                    "document": doc_name,
                    "key": key,
                })
    bar.set_postfix(file=doc_name)
    bar.update(1)


def monitor_cik(
    cik: str,
    bucket: str,
    prefix: str,
    state: Dict[str, Set[str]],
    manifest: Optional[List[Dict[str, str]]] = None,
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
                        manifest,
                    )
            executor.shutdown(wait=True)
    for ff in filing_files:
        processed.add(ff["accession"])


def main(
    ciks: List[str],
    bucket: str,
    prefix: str,
    state_path: str,
    manifest_key: Optional[str] = None,
) -> None:
    state = load_state(state_path)
    manifest: List[Dict[str, str]] = []
    if manifest_key:
        manifest = load_manifest(bucket, manifest_key)
    for cik in ciks:
        monitor_cik(cik, bucket, prefix, state, manifest)
    save_state(state, state_path)
    if manifest_key:
        save_manifest(manifest, bucket, manifest_key)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Monitor EDGAR for new filings and upload to S3")
    parser.add_argument("ciks", nargs='+', help="CIK codes to monitor")
    parser.add_argument("--bucket", required=True,
                        help="Destination S3 bucket")
    parser.add_argument("--prefix", default="edgar", help="S3 key prefix")
    parser.add_argument("--state", default="edgar_state.json",
                        help="Path to state file")
    parser.add_argument(
        "--manifest",
        metavar="KEY",
        help="S3 key for JSON manifest of downloaded files",
    )
    args = parser.parse_args()

    main(args.ciks, args.bucket, args.prefix, args.state, args.manifest)
