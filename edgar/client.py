# Helper utilities for communicating with the SEC
import os
import time
import threading
from typing import Dict

from .config import get_config

import requests

SEC_BASE = "https://data.sec.gov"
SEC_ARCHIVES = "https://www.sec.gov/Archives"

USER_AGENT = os.environ.get("SEC_USER_AGENT", "GPT-Trade-Agent (your-email@example.com)")
HEADERS: Dict[str, str] = {"User-Agent": USER_AGENT}

_last_sec_request = 0.0
_rate_lock = threading.Lock()


def sec_get(url: str, **kwargs) -> requests.Response:
    """GET request to SEC with simple global rate limiting."""
    cfg = get_config()
    interval = 1 / float(cfg.get("rate_limit_per_sec", 6))
    global _last_sec_request
    with _rate_lock:
        wait = interval - (time.time() - _last_sec_request)
        if wait > 0:
            time.sleep(wait)
        _last_sec_request = time.time()
    resp = requests.get(url, headers=HEADERS, **kwargs)
    return resp


def cik_to_10digit(cik: str) -> str:
    """Normalize CIK to 10 digit format."""
    return f"{int(cik.strip()):010d}"


def get_submissions(cik: str) -> Dict:
    """Fetch submissions JSON for a CIK."""
    cik10 = cik_to_10digit(cik)
    url = f"{SEC_BASE}/submissions/CIK{cik10}.json"
    resp = sec_get(url)
    resp.raise_for_status()
    return resp.json()
