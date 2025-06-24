"""Helpers for retrieving company tickers list."""
from typing import List, Dict

from .client import sec_get

COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"


def fetch_cik_company_list() -> List[Dict[str, str]]:
    """Download mapping of CIK to company names."""
    resp = sec_get(COMPANY_TICKERS_URL)
    resp.raise_for_status()
    data = resp.json()
    companies = []
    for entry in data.values():
        cik = f"{int(entry['cik_str']):010d}"
        name = entry.get('title', '').title()
        companies.append({"cik": cik, "name": name})
    return companies
