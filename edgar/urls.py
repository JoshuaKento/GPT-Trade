"""URL construction and validation utilities for SEC EDGAR."""

import re
from typing import Optional


class CIKValidationError(ValueError):
    """Raised when CIK format is invalid."""

    pass


class AccessionValidationError(ValueError):
    """Raised when accession number format is invalid."""

    pass


def validate_cik(cik: str) -> str:
    """Validate and normalize CIK format.

    Args:
        cik: Central Index Key as string

    Returns:
        Normalized 10-digit CIK string

    Raises:
        CIKValidationError: If CIK format is invalid
    """
    if not cik or not cik.strip():
        raise CIKValidationError("CIK cannot be empty")

    # Remove leading zeros and validate numeric
    try:
        cik_int = int(cik.strip())
        if cik_int < 0:
            raise CIKValidationError("CIK must be positive")
        if cik_int > 9999999999:  # Max 10 digits
            raise CIKValidationError("CIK cannot exceed 10 digits")
    except ValueError:
        raise CIKValidationError(f"CIK must be numeric: {cik}")

    return f"{cik_int:010d}"


def validate_accession_number(accession: str) -> str:
    """Validate SEC accession number format.

    Args:
        accession: Accession number in format NNNNNNNNNN-NN-NNNNNN

    Returns:
        Validated accession number

    Raises:
        AccessionValidationError: If format is invalid
    """
    if not accession or not accession.strip():
        raise AccessionValidationError("Accession number cannot be empty")

    accession = accession.strip()
    pattern = r"^\d{10}-\d{2}-\d{6}$"
    if not re.match(pattern, accession):
        raise AccessionValidationError(
            f"Invalid accession number format. Expected NNNNNNNNNN-NN-NNNNNN, got: {accession}"
        )
    return accession


def validate_document_name(document: str) -> str:
    """Validate document filename with comprehensive security checks.

    Args:
        document: Document filename

    Returns:
        Validated document name

    Raises:
        ValueError: If document name is invalid or contains security risks
    """
    if not document or not document.strip():
        raise ValueError("Document name cannot be empty")

    document = document.strip()

    # Normalize path to detect encoded traversal attempts
    import os.path
    import urllib.parse

    # URL decode to catch encoded path traversal attempts
    try:
        decoded = urllib.parse.unquote(document)
    except Exception:
        decoded = document

    # Normalize path to resolve any relative path components
    normalized = os.path.normpath(decoded)

    # Security checks for path traversal
    dangerous_patterns = [
        "..",  # Directory traversal
        "/",  # Unix path separator
        "\\",  # Windows path separator
        ":",  # Drive letters or alternate data streams
        "\x00",  # Null byte injection
        "\r",  # Carriage return
        "\n",  # Newline
    ]

    for pattern in dangerous_patterns:
        if pattern in normalized:
            raise ValueError(
                f"Invalid document name (contains dangerous pattern '{pattern}'): {document}"
            )

    # Check for URL-encoded dangerous characters
    encoded_dangerous = ["%2e%2e", "%2f", "%5c", "%00", "%3a"]
    document_lower = document.lower()
    for pattern in encoded_dangerous:
        if pattern in document_lower:
            raise ValueError(
                f"Invalid document name (contains encoded dangerous pattern): {document}"
            )

    # Restrict to alphanumeric, dots, hyphens, underscores and common file extensions
    if not re.match(r"^[a-zA-Z0-9._-]+$", normalized):
        raise ValueError(
            f"Invalid document name (contains illegal characters): {document}"
        )

    # Prevent files starting with dot (hidden files) except for common extensions
    if normalized.startswith(".") and not normalized.startswith(
        (".htm", ".html", ".txt", ".xml")
    ):
        raise ValueError(f"Invalid document name (hidden file pattern): {document}")

    # Length validation (prevent excessively long names)
    if len(normalized) > 255:
        raise ValueError(
            f"Document name too long (max 255 characters): {len(normalized)}"
        )

    # Prevent common dangerous filenames
    dangerous_names = {
        "con",
        "prn",
        "aux",
        "nul",
        "com1",
        "com2",
        "com3",
        "com4",
        "com5",
        "com6",
        "com7",
        "com8",
        "com9",
        "lpt1",
        "lpt2",
        "lpt3",
        "lpt4",
        "lpt5",
        "lpt6",
        "lpt7",
        "lpt8",
        "lpt9",
    }

    name_without_ext = normalized.split(".")[0].lower()
    if name_without_ext in dangerous_names:
        raise ValueError(f"Invalid document name (reserved system name): {document}")

    return normalized


class URLBuilder:
    """Centralized URL construction for SEC EDGAR endpoints."""

    SEC_BASE = "https://data.sec.gov"
    SEC_ARCHIVES = "https://www.sec.gov/Archives"

    @classmethod
    def submissions_url(cls, cik: str) -> str:
        """Build submissions JSON URL.

        Args:
            cik: Central Index Key

        Returns:
            URL for submissions JSON endpoint
        """
        cik10 = validate_cik(cik)
        return f"{cls.SEC_BASE}/submissions/CIK{cik10}.json"

    @classmethod
    def filing_index_url(cls, cik: str, accession: str) -> str:
        """Build filing index HTML URL.

        Args:
            cik: Central Index Key
            accession: Filing accession number

        Returns:
            URL for filing index page
        """
        cik10 = validate_cik(cik)
        accession = validate_accession_number(accession)
        acc_no_dash = accession.replace("-", "")
        cik_int = int(cik10)
        return f"{cls.SEC_ARCHIVES}/edgar/data/{cik_int}/{acc_no_dash}/{accession}-index.html"

    @classmethod
    def document_url(cls, cik: str, accession: str, document: str) -> str:
        """Build document download URL.

        Args:
            cik: Central Index Key
            accession: Filing accession number
            document: Document filename

        Returns:
            URL for document download
        """
        cik10 = validate_cik(cik)
        accession = validate_accession_number(accession)
        document = validate_document_name(document)
        acc_no_dash = accession.replace("-", "")
        cik_int = int(cik10)
        return f"{cls.SEC_ARCHIVES}/edgar/data/{cik_int}/{acc_no_dash}/{document}"

    @classmethod
    def company_tickers_url(cls) -> str:
        """Build company tickers JSON URL.

        Returns:
            URL for company tickers endpoint
        """
        return f"{cls.SEC_BASE}/files/company_tickers.json"


def cik_to_10digit(cik: str) -> str:
    """Legacy function for backward compatibility.

    Args:
        cik: Central Index Key

    Returns:
        10-digit normalized CIK
    """
    return validate_cik(cik)
