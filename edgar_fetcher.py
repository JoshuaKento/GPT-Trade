import requests
import os
from typing import Optional
import time
import threading

SEC_BASE = "https://data.sec.gov"
SEC_ARCHIVES = "https://www.sec.gov/Archives"

# Set your user agent via environment variable `SEC_USER_AGENT`.
# A valid contact address is required by the SEC.
USER_AGENT = os.environ.get("SEC_USER_AGENT", "GPT-Trade-Agent (your-email@example.com)")
HEADERS = {"User-Agent": USER_AGENT}
    with _rate_lock:
        wait = _MIN_INTERVAL - (time.time() - _last_sec_request)
        if wait > 0:
            time.sleep(wait)
        _last_sec_request = time.time()


            url = f"{SEC_ARCHIVES}/edgar/data/{int(cik):d}/{accession_no_dashless}/{doc}"
    if wait > 0:
        time.sleep(wait)
    resp = requests.get(url, headers=HEADERS, **kwargs)
    _last_sec_request = time.time()
    return resp

def cik_to_10digit(cik: str) -> str:
    """Normalize CIK to 10 digit string."""
    cik = cik.strip().lstrip('0')
    return f"{int(cik):010d}"


def get_submissions(cik: str) -> dict:
    """Fetch submission data for a given CIK."""
    cik10 = cik_to_10digit(cik)
    url = f"{SEC_BASE}/submissions/CIK{cik10}.json"
    resp = sec_get(url)
    resp.raise_for_status()
    return resp.json()


def fetch_latest_10k(cik: str, download_dir: str = "10k") -> Optional[str]:
    """Fetch the latest 10-K filing for the company and save it.

    Returns path to saved file or None if not found.
    """
    data = get_submissions(cik)
    filings = data.get("filings", {}).get("recent", {})
    forms = filings.get("form", [])
    accession_numbers = filings.get("accessionNumber", [])
    primary_docs = filings.get("primaryDocument", [])

    for form, accession, doc in zip(forms, accession_numbers, primary_docs):
        if form == "10-K":
            # Build URL
            accession_no_dashless = accession.replace('-', '')
            url = (
                f"https://www.sec.gov/Archives/edgar/data/"
                f"{int(cik):d}/{accession_no_dashless}/{doc}"
            )
            os.makedirs(download_dir, exist_ok=True)
            local_path = os.path.join(download_dir, doc)
            resp = sec_get(url)
            resp.raise_for_status()
            with open(local_path, 'wb') as f:
                f.write(resp.content)
            return local_path
    return None

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python edgar_fetcher.py <CIK>")
        sys.exit(1)
    cik = sys.argv[1]
    path = fetch_latest_10k(cik)
    if path:
        print(f"Saved latest 10-K to {path}")
    else:
        print("No 10-K filing found.")
