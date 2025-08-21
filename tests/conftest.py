"""Pytest configuration and shared fixtures for EDGAR toolkit tests."""

import json
import logging
import os

# Add parent directory to path for imports
import sys
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from edgar.client_new import ClientConfig, EdgarClient
from edgar.config_manager import EdgarConfig
from edgar.filing_processor import FilingInfo, FilingProcessor
from edgar.s3_manager import S3Manager

# Configure test logging
logging.basicConfig(level=logging.DEBUG)


@pytest.fixture
def sample_config():
    """Provide a sample EdgarConfig for testing."""
    return EdgarConfig(
        rate_limit_per_sec=10.0,
        num_workers=4,
        timeout=30,
        user_agent="Test Agent (test@example.com)",
        form_types=["10-K", "10-Q", "8-K"],
        max_retries=3,
    )


@pytest.fixture
def sample_client_config():
    """Provide a sample ClientConfig for testing."""
    return ClientConfig(
        rate_limit_per_sec=10.0,
        timeout=30,
        user_agent="Test Agent (test@example.com)",
        max_retries=3,
    )


@pytest.fixture
def mock_edgar_client():
    """Provide a mocked EdgarClient."""
    client = Mock(spec=EdgarClient)
    client.__enter__ = Mock(return_value=client)
    client.__exit__ = Mock(return_value=None)
    return client


@pytest.fixture
def mock_s3_manager():
    """Provide a mocked S3Manager."""
    return Mock(spec=S3Manager)


@pytest.fixture
def sample_filing_info():
    """Provide sample FilingInfo objects."""
    return [
        FilingInfo(
            form="10-K",
            accession="0000320193-23-000106",
            primary_document="aapl-20230930.htm",
            filing_date="2023-11-02",
            report_date="2023-09-30",
        ),
        FilingInfo(
            form="10-Q",
            accession="0000320193-23-000064",
            primary_document="aapl-20230701.htm",
            filing_date="2023-08-04",
            report_date="2023-06-30",
        ),
        FilingInfo(
            form="8-K",
            accession="0000320193-23-000077",
            primary_document="aapl-20230727.htm",
            filing_date="2023-07-27",
            report_date="2023-07-27",
        ),
    ]


@pytest.fixture
def sample_submissions_data():
    """Provide realistic SEC submissions API response."""
    return {
        "cik": 320193,
        "entityType": "operating",
        "sic": "3571",
        "sicDescription": "Electronic Computers",
        "name": "Apple Inc.",
        "tickers": ["AAPL"],
        "exchanges": ["Nasdaq"],
        "ein": "942404110",
        "description": "Technology",
        "website": "https://www.apple.com",
        "investorWebsite": "https://investor.apple.com",
        "category": "Large accelerated filer",
        "fiscalYearEnd": "0930",
        "stateOfIncorporation": "DE",
        "stateOfIncorporationDescription": "DE",
        "addresses": {
            "mailing": {
                "street1": "ONE APPLE PARK WAY",
                "city": "CUPERTINO",
                "stateOrCountry": "CA",
                "zipCode": "95014",
                "stateOrCountryDescription": "CA",
            },
            "business": {
                "street1": "ONE APPLE PARK WAY",
                "city": "CUPERTINO",
                "stateOrCountry": "CA",
                "zipCode": "95014",
                "stateOrCountryDescription": "CA",
            },
        },
        "phone": "4089961010",
        "filings": {
            "recent": {
                "accessionNumber": [
                    "0000320193-23-000106",
                    "0000320193-23-000064",
                    "0000320193-23-000077",
                ],
                "filingDate": ["2023-11-02", "2023-08-04", "2023-07-27"],
                "reportDate": ["2023-09-30", "2023-06-30", "2023-07-27"],
                "acceptanceDateTime": [
                    "2023-11-02T18:08:00.000Z",
                    "2023-08-04T18:04:00.000Z",
                    "2023-07-27T18:02:00.000Z",
                ],
                "act": ["34", "34", "34"],
                "form": ["10-K", "10-Q", "8-K"],
                "fileNumber": ["001-36743", "001-36743", "001-36743"],
                "filmNumber": ["231417988", "231174193", "231158109"],
                "items": [
                    "1A,1B,2,3,4,5,6,7,7A,8,9,9A,9B,10,11,12,13,14,15",
                    "2,3",
                    "2.02,9.01",
                ],
                "size": [19876482, 8806340, 7628395],
                "isXBRL": [1, 1, 1],
                "isInlineXBRL": [1, 1, 1],
                "primaryDocument": [
                    "aapl-20230930.htm",
                    "aapl-20230701.htm",
                    "aapl-20230727.htm",
                ],
                "primaryDocDescription": ["10-K", "10-Q", "8-K"],
            }
        },
    }


@pytest.fixture
def sample_company_tickers():
    """Provide sample company tickers data."""
    return {
        "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
        "1": {"cik_str": 789019, "ticker": "MSFT", "title": "MICROSOFT CORP"},
        "2": {"cik_str": 1652044, "ticker": "GOOGL", "title": "Alphabet Inc."},
    }


@pytest.fixture
def sample_filing_index_html():
    """Provide sample filing index HTML."""
    return """
    <!DOCTYPE html>
    <html>
    <head><title>Filing Details</title></head>
    <body>
        <table class="tableFile" summary="Document Format Files">
            <tr>
                <th scope="col">Seq</th>
                <th scope="col">Description</th>
                <th scope="col">Document</th>
                <th scope="col">Type</th>
                <th scope="col">Size</th>
            </tr>
            <tr>
                <td scope="row">1</td>
                <td>FORM 10-K</td>
                <td><a href="/Archives/edgar/data/320193/000032019323000106/aapl-20230930.htm">aapl-20230930.htm</a></td>
                <td>10-K</td>
                <td>19876482</td>
            </tr>
            <tr>
                <td scope="row">2</td>
                <td>EXHIBIT 21.1</td>
                <td><a href="/Archives/edgar/data/320193/000032019323000106/exhibit211subsidiaries.htm">exhibit211subsidiaries.htm</a></td>
                <td>EX-21.1</td>
                <td>7184</td>
            </tr>
            <tr>
                <td scope="row">3</td>
                <td>EXHIBIT 23.1</td>
                <td><a href="/Archives/edgar/data/320193/000032019323000106/exhibit231consent.htm">exhibit231consent.htm</a></td>
                <td>EX-23.1</td>
                <td>5237</td>
            </tr>
        </table>
    </body>
    </html>
    """


@pytest.fixture
def sample_parsed_documents():
    """Provide sample parsed document list."""
    return [
        {
            "sequence": "1",
            "description": "FORM 10-K",
            "document": "aapl-20230930.htm",
            "type": "10-K",
            "size": "19876482",
        },
        {
            "sequence": "2",
            "description": "EXHIBIT 21.1",
            "document": "exhibit211subsidiaries.htm",
            "type": "EX-21.1",
            "size": "7184",
        },
        {
            "sequence": "3",
            "description": "EXHIBIT 23.1",
            "document": "exhibit231consent.htm",
            "type": "EX-23.1",
            "size": "5237",
        },
    ]


@pytest.fixture
def temp_state_file():
    """Provide temporary state file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        temp_path = f.name

    yield temp_path

    # Cleanup
    try:
        os.unlink(temp_path)
    except FileNotFoundError:
        pass


@pytest.fixture
def temp_download_dir():
    """Provide temporary download directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def filing_processor(mock_edgar_client, mock_s3_manager):
    """Provide a FilingProcessor instance with mocked dependencies."""
    return FilingProcessor(edgar_client=mock_edgar_client, s3_manager=mock_s3_manager)


@pytest.fixture(autouse=True)
def mock_logging():
    """Mock logging to reduce test output noise."""
    with patch("logging.getLogger") as mock_logger:
        mock_logger_instance = Mock()
        mock_logger_instance.info = Mock()
        mock_logger_instance.debug = Mock()
        mock_logger_instance.warning = Mock()
        mock_logger_instance.error = Mock()
        mock_logger.return_value = mock_logger_instance
        yield mock_logger_instance


# Pytest markers for test categorization
pytest.mark.unit = pytest.mark.unit
pytest.mark.integration = pytest.mark.integration
pytest.mark.slow = pytest.mark.slow
pytest.mark.network = pytest.mark.network


# Test configuration
def pytest_configure(config):
    """Configure pytest with custom settings."""
    config.addinivalue_line("markers", "unit: mark test as a unit test")
    config.addinivalue_line("markers", "integration: mark test as an integration test")
    config.addinivalue_line("markers", "slow: mark test as slow running")
    config.addinivalue_line("markers", "network: mark test as requiring network access")


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers automatically."""
    for item in items:
        # Mark all tests in test_filing_processor.py as unit tests
        if "test_filing_processor" in item.nodeid:
            item.add_marker(pytest.mark.unit)

        # Mark all tests in test_filings.py as unit tests
        if "test_filings" in item.nodeid:
            item.add_marker(pytest.mark.unit)

        # Mark all tests in test_companies.py as unit tests
        if "test_companies" in item.nodeid:
            item.add_marker(pytest.mark.unit)

        # Mark async tests
        if "async" in item.name:
            item.add_marker(pytest.mark.asyncio)
