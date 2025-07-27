"""HTML parsing helpers for EDGAR filings with enhanced error handling."""

import logging
from typing import Dict, List, Optional

from bs4 import BeautifulSoup, Tag


def parse_file_list(html: str) -> List[Dict[str, str]]:
    """Parse filing index HTML into a list of document info.

    Args:
        html: HTML content containing filing index table

    Returns:
        List of document information dictionaries

    Raises:
        ValueError: If HTML is invalid or empty
        RuntimeError: If parsing fails
    """
    if not html or not html.strip():
        raise ValueError("HTML content cannot be empty")

    logger = logging.getLogger(__name__)

    try:
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table", class_="tableFile")

        if not table:
            logger.warning("No tableFile found in HTML - may be an empty filing")
            return []

        files = []
        rows = table.find_all("tr")

        if len(rows) <= 1:  # Only header row or no rows
            logger.info("Filing index table has no data rows")
            return []

        # Skip header row
        for i, row in enumerate(rows[1:], 1):
            try:
                cols = [c.get_text(strip=True) for c in row.find_all("td")]

                if len(cols) < 5:
                    logger.debug(
                        f"Row {i} has insufficient columns ({len(cols)}), skipping"
                    )
                    continue

                # Extract document link
                link = row.find("a")
                if link and link.get("href"):
                    href = link.get("href", "")
                    document = href.rsplit("/", 1)[-1] if href else ""
                else:
                    document = ""

                # Validate required fields
                if not cols[0]:  # sequence number
                    logger.debug(f"Row {i} missing sequence number, skipping")
                    continue

                file_info = {
                    "sequence": cols[0],
                    "description": cols[1],
                    "document": document,
                    "type": cols[3],
                    "size": cols[4],
                }

                files.append(file_info)

            except Exception as e:
                logger.warning(f"Error parsing row {i}: {e}")
                continue

        logger.debug(f"Parsed {len(files)} documents from filing index")
        return files

    except Exception as e:
        logger.error(f"Failed to parse filing index HTML: {e}")
        raise RuntimeError(f"Failed to parse filing index HTML: {e}")
