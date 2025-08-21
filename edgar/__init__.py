"""EDGAR toolkit for fetching SEC filings.

This package provides both legacy and new architecture components:

New Architecture (Recommended):
    - EdgarClient: Thread-safe HTTP client with rate limiting
    - S3Manager: S3 operations with connection pooling
    - FilingProcessor: Business logic for processing filings
    - ConfigManager: Type-safe configuration management
    - URLBuilder: Centralized URL construction with validation

Legacy Components (Deprecated):
    - client: Legacy HTTP client with global state
    - s3util: Basic S3 utilities
    - config: Legacy configuration management
"""

# New architecture imports (recommended)
from .client_new import ClientConfig, EdgarClient
from .config_manager import ConfigManager, EdgarConfig
from .filing_processor import FilingProcessor, ProcessingResult
from .filings import (  # Enhanced versions
    fetch_latest_10k,
    get_filing_files,
    list_recent_filings,
)

# Enhanced modules
from .parser import parse_file_list
from .s3_manager import S3AccessError, S3CredentialsError, S3Manager, S3NotFoundError
from .state import load_state, save_state

# Legacy imports (backward compatibility) - now from enhanced modules
# Exception types
from .urls import cik_to_10digit  # Moved to urls module
from .urls import (
    AccessionValidationError,
    CIKValidationError,
    URLBuilder,
    validate_accession_number,
    validate_cik,
)

__version__ = "0.2.0"

__all__ = [
    # New architecture (recommended)
    "EdgarClient",
    "ClientConfig",
    "S3Manager",
    "FilingProcessor",
    "ProcessingResult",
    "ConfigManager",
    "EdgarConfig",
    "URLBuilder",
    "validate_cik",
    "validate_accession_number",
    # Enhanced modules
    "parse_file_list",
    "load_state",
    "save_state",
    # Exception types
    "CIKValidationError",
    "AccessionValidationError",
    "S3CredentialsError",
    "S3AccessError",
    "S3NotFoundError",
    # Legacy functions (now enhanced)
    "cik_to_10digit",
    "fetch_latest_10k",
    "list_recent_filings",
    "get_filing_files",
]
