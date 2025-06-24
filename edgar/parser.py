"""HTML parsing helpers for EDGAR filings."""
from typing import List, Dict
from bs4 import BeautifulSoup


def parse_file_list(html: str) -> List[Dict[str, str]]:
    """Parse filing index HTML into a list of document info."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="tableFile")
    if not table:
        return []
    files = []
    rows = table.find_all("tr")[1:]
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
