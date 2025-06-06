"""High level filing helpers."""
from typing import List, Dict, Optional
import os

from .client import sec_get, SEC_ARCHIVES, get_submissions
from .parser import parse_file_list


def fetch_latest_10k(cik: str, download_dir: str = "10k") -> Optional[str]:
    """Download the latest 10-K filing and save it to disk."""
    data = get_submissions(cik)
    filings = data.get("filings", {}).get("recent", {})
    forms = filings.get("form", [])
    accession_numbers = filings.get("accessionNumber", [])
    primary_docs = filings.get("primaryDocument", [])

    for form, accession, doc in zip(forms, accession_numbers, primary_docs):
        if form == "10-K":
            acc_no_nodash = accession.replace('-', '')
            os.makedirs(download_dir, exist_ok=True)
            url = f"{SEC_ARCHIVES}/edgar/data/{int(cik):d}/{acc_no_nodash}/{doc}"
            local_path = os.path.join(download_dir, doc)
            resp = sec_get(url)
            resp.raise_for_status()
            with open(local_path, 'wb') as f:
                f.write(resp.content)
            return local_path
    return None


def list_recent_filings(cik: str) -> List[Dict[str, str]]:
    """Return recent filings for a company."""
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
    """Return file details for a specific filing."""
    acc_no_nodash = accession_number.replace('-', '')
    url = f"{SEC_ARCHIVES}/edgar/data/{int(cik):d}/{acc_no_nodash}/{accession_number}-index.html"
    resp = sec_get(url)
    resp.raise_for_status()
    return parse_file_list(resp.text)
