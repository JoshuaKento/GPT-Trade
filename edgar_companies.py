from typing import List, Dict

from edgar_fetcher import HEADERS, sec_get

COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"


def fetch_cik_company_list() -> List[Dict[str, str]]:
    """Download list of companies with CIK and name."""
    resp = sec_get(COMPANY_TICKERS_URL)
    resp.raise_for_status()
    data = resp.json()
    companies = []
    for entry in data.values():
        cik = f"{int(entry['cik_str']):010d}"
        name = entry.get('title', '').title()
        companies.append({"cik": cik, "name": name})
    return companies


if __name__ == "__main__":
    comps = fetch_cik_company_list()
    try:
        for c in comps:
            print(c["cik"], c["name"])
    except BrokenPipeError:
        pass
