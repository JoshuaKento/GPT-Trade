#!/usr/bin/env python3
"""CLI to monitor EDGAR for new filings and upload to S3."""
import argparse
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor
import threading
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from edgar.progress import tqdm
from edgar.client import SEC_ARCHIVES, sec_get
from edgar.filings import list_recent_filings, get_filing_files
from edgar.s3util import upload_bytes_to_s3, load_manifest, save_manifest
from edgar.state import load_state, save_state

manifest_lock = threading.Lock()


def download_file(url: str) -> bytes:
    resp = sec_get(url)
    resp.raise_for_status()
    return resp.content


def download_and_upload_file(cik: str, accession: str, doc_name: str, bucket: str, prefix: str,
                              bar: tqdm, manifest: List[Dict[str, str]]) -> None:
    cik_num = int(cik)
    acc_no_nodash = accession.replace('-', '')
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
                manifest.append({"cik": cik, "accession": accession, "document": doc_name, "key": key})
    bar.set_postfix(file=doc_name)
    bar.update(1)


def monitor_cik(cik: str, bucket: str, prefix: str, state: Dict[str, set], manifest: List[Dict[str, str]]) -> None:
    filings = list_recent_filings(cik)
    processed = state.setdefault(cik, set())
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
                    executor.submit(download_and_upload_file, cik, accession, doc_name, bucket, prefix, bar, manifest)
            executor.shutdown(wait=True)
    for ff in filing_files:
        processed.add(ff["accession"])


def main() -> None:
    parser = argparse.ArgumentParser(description="Monitor EDGAR and upload files to S3")
    parser.add_argument("ciks", nargs='+', help="CIK codes to monitor")
    parser.add_argument("--bucket", required=True, help="Destination S3 bucket")
    parser.add_argument("--prefix", default="edgar", help="S3 key prefix")
    parser.add_argument("--state", default="edgar_state.json", help="Path to state file")
    parser.add_argument("--manifest", metavar="KEY", help="S3 key for manifest JSON")
    args = parser.parse_args()

    state = load_state(args.state)
    manifest: List[Dict[str, str]] = []
    if args.manifest:
        manifest = load_manifest(args.bucket, args.manifest)
    for cik in args.ciks:
        monitor_cik(cik, args.bucket, args.prefix, state, manifest)
    save_state(state, args.state)
    if args.manifest:
        save_manifest(manifest, args.bucket, args.manifest)


if __name__ == "__main__":
    main()
