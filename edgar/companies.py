"""Helpers for retrieving company tickers list."""

import logging
from typing import Dict, List

from .client_new import ClientConfig, EdgarClient
from .config_manager import ConfigManager

COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"


def fetch_cik_company_list() -> List[Dict[str, str]]:
    """Download mapping of CIK to company names.

    Returns:
        List of dictionaries with 'cik' and 'name' keys

    Raises:
        RuntimeError: If download fails
    """
    logger = logging.getLogger(__name__)

    try:
        # Use new architecture
        config_manager = ConfigManager()
        config = config_manager.get_config()

        client_config = ClientConfig(
            rate_limit_per_sec=config.rate_limit_per_sec,
            timeout=config.timeout,
            user_agent=config.user_agent,
        )

        with EdgarClient(client_config) as client:
            logger.debug(f"Fetching company tickers from {COMPANY_TICKERS_URL}")
            resp = client.get(COMPANY_TICKERS_URL)
            resp.raise_for_status()
            data = resp.json()

            companies = []
            for entry in data.values():
                cik = f"{int(entry['cik_str']):010d}"
                name = entry.get("title", "").title()
                companies.append({"cik": cik, "name": name})

            logger.info(f"Retrieved {len(companies)} company records")
            return companies

    except Exception as e:
        logger.error(f"Failed to fetch company list: {e}")
        raise RuntimeError(f"Failed to fetch company list: {e}")
