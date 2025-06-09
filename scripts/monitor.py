#!/usr/bin/env python3
"""CLI to monitor EDGAR for new filings and upload to S3."""
import argparse
from typing import List, Dict
import asyncio
import aiohttp
import os
import sys
import logging
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from edgar.progress import tqdm
from edgar.client import SEC_ARCHIVES, HEADERS, cik_to_10digit
from edgar.parser import parse_file_list
from edgar.s3util import upload_bytes_to_s3, load_manifest, save_manifest
from edgar.state import load_state, save_state
from edgar.logging_config import setup_logging
from edgar.config import load_config, get_config


logger = logging.getLogger("monitor")


class RateLimiter:
    """Asynchronous token bucket rate limiter."""

    def __init__(self, rate: int) -> None:
        self._rate = rate
        self._sem = asyncio.BoundedSemaphore(rate)
        self._loop = asyncio.get_event_loop()

    async def acquire(self) -> None:
        await self._sem.acquire()
        self._loop.call_later(1, self._sem.release)


async def download_file(session: aiohttp.ClientSession, url: str, limiter: RateLimiter) -> bytes:
    await limiter.acquire()
    async with session.get(url, headers=HEADERS) as resp:
        resp.raise_for_status()
        return await resp.read()


async def download_and_upload_file(
    session: aiohttp.ClientSession,
    limiter: RateLimiter,
    sem: asyncio.Semaphore,
    cik: str,
    accession: str,
    doc_name: str,
    bucket: str,
    prefix: str,
    bar: tqdm,
    manifest: List[Dict[str, str]],
) -> None:
    async with sem:
        cik_num = int(cik)
        acc_no_nodash = accession.replace('-', '')
        file_url = f"{SEC_ARCHIVES}/edgar/data/{cik_num}/{acc_no_nodash}/{doc_name}"
        try:
            data = await download_file(session, file_url, limiter)
        except Exception as exc:  # pragma: no cover - network
            logger.error("Failed to download %s: %s", file_url, exc)
            bar.update(1)
            return
        key = f"{prefix}/{cik}/{accession}/{doc_name}"
        try:
            await asyncio.to_thread(upload_bytes_to_s3, data, bucket, key)
        except Exception as exc:  # pragma: no cover - network
            logger.error("Failed to upload %s to S3: %s", key, exc)
        else:
            if manifest is not None:
                manifest.append({"cik": cik, "accession": accession, "document": doc_name, "key": key})
        bar.set_postfix(file=doc_name)
        bar.update(1)


async def get_filing_files_async(session: aiohttp.ClientSession, limiter: RateLimiter, cik: str, accession: str) -> List[Dict[str, str]]:
    acc_no_nodash = accession.replace('-', '')
    url = f"{SEC_ARCHIVES}/edgar/data/{int(cik):d}/{acc_no_nodash}/{accession}-index.html"
    try:
        data = await download_file(session, url, limiter)
    except Exception as exc:  # pragma: no cover - network
        logger.error("Failed to fetch index %s: %s", url, exc)
        return []
    return parse_file_list(data.decode("utf-8", errors="replace"))


async def list_recent_filings_async(session: aiohttp.ClientSession, limiter: RateLimiter, cik: str) -> List[Dict[str, str]]:
    cik10 = cik_to_10digit(cik)
    url = f"https://data.sec.gov/submissions/CIK{cik10}.json"
    try:
        await limiter.acquire()
        async with session.get(url, headers=HEADERS) as resp:
            resp.raise_for_status()
            data = await resp.json()
    except Exception as exc:  # pragma: no cover - network
        logger.error("Failed to fetch submissions for %s: %s", cik, exc)
        return []
    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    accession_numbers = recent.get("accessionNumber", [])
    primary_docs = recent.get("primaryDocument", [])
    filings = []
    for form, accession, doc in zip(forms, accession_numbers, primary_docs):
        filings.append({"form": form, "accession": accession, "doc": doc})
    return filings


async def monitor_cik(
    session: aiohttp.ClientSession,
    limiter: RateLimiter,
    sem: asyncio.Semaphore,
    cik: str,
    bucket: str,
    prefix: str,
    state: Dict[str, set],
    manifest: List[Dict[str, str]],
    form_types: List[str] | None,
) -> None:
    filings = await list_recent_filings_async(session, limiter, cik)
    if form_types:
        forms_set = {f.upper() for f in form_types}
        filings = [f for f in filings if f["form"].upper() in forms_set]
    processed = state.setdefault(cik, set())
    filing_files: List[Dict[str, List[str]]] = []
    total_files = 0
    for filing in filings:
        accession = filing["accession"]
        if accession in processed:
            continue
        files = await get_filing_files_async(session, limiter, cik, accession)
        doc_names = [f.get("document") for f in files if f.get("document")]
        if not doc_names:
            processed.add(accession)
            continue
        filing_files.append({"accession": accession, "docs": doc_names})
        total_files += len(doc_names)
    if total_files == 0:
        return
    with tqdm(total=total_files, unit="file", desc=f"CIK {cik}") as bar:
        tasks = []
        for ff in filing_files:
            accession = ff["accession"]
            for doc_name in ff["docs"]:
                tasks.append(
                    asyncio.create_task(
                        download_and_upload_file(
                            session,
                            limiter,
                            sem,
                            cik,
                            accession,
                            doc_name,
                            bucket,
                            prefix,
                            bar,
                            manifest,
                        )
                    )
                )
        await asyncio.gather(*tasks)
    for ff in filing_files:
        processed.add(ff["accession"])


async def main_async(args: argparse.Namespace) -> None:
    setup_logging()
    load_config(args.config)
    cfg = get_config()
    prefix = args.prefix or cfg.get("s3_prefix", "edgar")
    rate = int(cfg.get("rate_limit_per_sec", 6))
    workers = int(cfg.get("num_workers", 6))
    forms = cfg.get("form_types", [])

    state = load_state(args.state)
    manifest: List[Dict[str, str]] = []
    if args.manifest:
        manifest = load_manifest(args.bucket, args.manifest)

    limiter = RateLimiter(rate)
    sem = asyncio.Semaphore(workers)
    async with aiohttp.ClientSession() as session:
        for cik in args.ciks:
            await monitor_cik(session, limiter, sem, cik, args.bucket, prefix, state, manifest, forms)

    save_state(state, args.state)
    if args.manifest:
        save_manifest(manifest, args.bucket, args.manifest)


def main() -> None:
    parser = argparse.ArgumentParser(description="Monitor EDGAR and upload files to S3")
    parser.add_argument("ciks", nargs='+', help="CIK codes to monitor")
    parser.add_argument("--bucket", required=True, help="Destination S3 bucket")
    parser.add_argument("--prefix", help="S3 key prefix")
    parser.add_argument("--state", default="edgar_state.json", help="Path to state file")
    parser.add_argument("--manifest", metavar="KEY", help="S3 key for manifest JSON")
    parser.add_argument("--config", help="Path to config JSON")
    args = parser.parse_args()

    asyncio.run(main_async(args))



if __name__ == "__main__":
    main()
