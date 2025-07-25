from bs4 import BeautifulSoup
from typing import List, Dict, Optional
import json

from edgar_fetcher import cik_to_10digit, get_submissions, sec_get, SEC_ARCHIVES


def get_latest_10k_accession(cik: str) -> Optional[str]:
    """Return the accession number for the latest 10-K filing."""
    data = get_submissions(cik)
    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    accession_numbers = recent.get("accessionNumber", [])
    for form, accession in zip(forms, accession_numbers):
        if form == "10-K":
            return accession
    return None


def get_filing_index_html(cik: str, accession_number: str) -> str:
    cik_num = int(cik)
    acc_no_nodash = accession_number.replace('-', '')
    url = f"{SEC_ARCHIVES}/edgar/data/{cik_num}/{acc_no_nodash}/{accession_number}-index.html"
    resp = sec_get(url)
    resp.raise_for_status()
    return resp.text


def parse_file_list(html: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="tableFile")
    if not table:
        return []
    files = []
    rows = table.find_all("tr")[1:]  # skip header
    for row in rows:
        cols = [c.get_text(strip=True) for c in row.find_all("td")]
        if len(cols) < 5:
            continue
        link = row.find("a")
        document = link.get("href", "").rsplit('/', 1)[-1] if link else ""
        files.append({
            "sequence": cols[0],
            "description": cols[1],
            "document": document,
            "type": cols[3],
            "size": cols[4],
        })
    return files


def list_latest_10k_files(cik: str) -> List[Dict[str, str]]:
    accession = get_latest_10k_accession(cik)
    if not accession:
        return []
    html = get_filing_index_html(cik, accession)
    return parse_file_list(html)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="List files in the latest 10-K filing for a company"
    )
    parser.add_argument("cik", help="Company CIK")
    parser.add_argument(
        "--json-out",
        metavar="PATH",
        help="Optional path to write the file list as JSON",
    )
    args = parser.parse_args()

    files = list_latest_10k_files(args.cik)

    if args.json_out:
        with open(args.json_out, "w") as fp:
            json.dump(files, fp, indent=2)

    if not args.json_out:
        if not files:
            print("No filing files found.")
        else:
            for f in files:
                print(f["document"], f["description"], f["type"], f["size"])
