"""Configuration management for edgar tools."""
from __future__ import annotations
import os
import json
from typing import Any, Dict

_DEFAULT_CONFIG: Dict[str, Any] = {
    "rate_limit_per_sec": 6,
    "num_workers": 6,
    "s3_prefix": "edgar",
    "form_types": [],  # empty list means all filings
}

_CONFIG: Dict[str, Any] | None = None


def load_config(path: str | None = None) -> Dict[str, Any]:
    """Load configuration from JSON file if present."""
    global _CONFIG
    if _CONFIG is not None:
        return _CONFIG
    path = path or os.environ.get("EDGAR_CONFIG", "config.json")
    if os.path.exists(path):
        with open(path, "r") as fp:
            data = json.load(fp)
        cfg = _DEFAULT_CONFIG.copy()
        cfg.update(data)
        _CONFIG = cfg
    else:
        _CONFIG = _DEFAULT_CONFIG.copy()
    return _CONFIG


def get_config() -> Dict[str, Any]:
    return load_config()
