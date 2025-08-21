"""Comprehensive tests for company data operations."""

import json
import os
import sys
from unittest.mock import Mock, patch

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from edgar.client_new import ClientConfig, EdgarClient
from edgar.companies import COMPANY_TICKERS_URL, fetch_cik_company_list
from edgar.config_manager import ConfigManager, EdgarConfig


class TestFetchCikCompanyList:
    """Test fetch_cik_company_list function."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_config = EdgarConfig(
            rate_limit_per_sec=10.0,
            num_workers=4,
            timeout=30,
            user_agent="Test Agent (test@example.com)",
        )

        # Sample company tickers data from SEC API
        self.sample_tickers_data = {
            "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
            "1": {"cik_str": 789019, "ticker": "MSFT", "title": "MICROSOFT CORP"},
            "2": {"cik_str": 1652044, "ticker": "GOOGL", "title": "Alphabet Inc."},
            "3": {"cik_str": 1018724, "ticker": "AMZN", "title": "AMAZON COM INC"},
            "4": {"cik_str": 1045810, "ticker": "NVDA", "title": "NVIDIA CORP"},
        }

        self.expected_companies = [
            {"cik": "0000320193", "name": "Apple Inc."},
            {"cik": "0000789019", "name": "Microsoft Corp"},
            {"cik": "0001652044", "name": "Alphabet Inc."},
            {"cik": "0001018724", "name": "Amazon Com Inc"},
            {"cik": "0001045810", "name": "Nvidia Corp"},
        ]

    @patch("edgar.companies.ConfigManager")
    @patch("edgar.companies.EdgarClient")
    def test_fetch_cik_company_list_success(
        self, mock_client_class, mock_config_manager
    ):
        """Test successful retrieval of company list."""
        # Setup mocks
        mock_config_manager.return_value.get_config.return_value = self.mock_config

        mock_client = Mock(spec=EdgarClient)
        mock_response = Mock()
        mock_response.raise_for_status = Mock()
        mock_response.json.return_value = self.sample_tickers_data
        mock_client.get.return_value = mock_response
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=None)
        mock_client_class.return_value = mock_client

        # Call function
        result = fetch_cik_company_list()

        # Verify result
        assert len(result) == 5
        assert result == self.expected_companies

        # Verify method calls
        mock_config_manager.assert_called_once()
        mock_client.get.assert_called_once_with(COMPANY_TICKERS_URL)
        mock_response.raise_for_status.assert_called_once()
        mock_response.json.assert_called_once()

    @patch("edgar.companies.ConfigManager")
    @patch("edgar.companies.EdgarClient")
    def test_fetch_cik_company_list_empty_response(
        self, mock_client_class, mock_config_manager
    ):
        """Test handling of empty company tickers response."""
        mock_config_manager.return_value.get_config.return_value = self.mock_config

        mock_client = Mock(spec=EdgarClient)
        mock_response = Mock()
        mock_response.raise_for_status = Mock()
        mock_response.json.return_value = {}  # Empty response
        mock_client.get.return_value = mock_response
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=None)
        mock_client_class.return_value = mock_client

        result = fetch_cik_company_list()

        assert result == []

    @patch("edgar.companies.ConfigManager")
    @patch("edgar.companies.EdgarClient")
    def test_fetch_cik_company_list_http_error(
        self, mock_client_class, mock_config_manager
    ):
        """Test handling of HTTP errors."""
        mock_config_manager.return_value.get_config.return_value = self.mock_config

        mock_client = Mock(spec=EdgarClient)
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = Exception("HTTP 404")
        mock_client.get.return_value = mock_response
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=None)
        mock_client_class.return_value = mock_client

        with pytest.raises(RuntimeError) as exc_info:
            fetch_cik_company_list()

        assert "Failed to fetch company list" in str(exc_info.value)
        assert "HTTP 404" in str(exc_info.value)

    @patch("edgar.companies.ConfigManager")
    @patch("edgar.companies.EdgarClient")
    def test_fetch_cik_company_list_json_decode_error(
        self, mock_client_class, mock_config_manager
    ):
        """Test handling of JSON decode errors."""
        mock_config_manager.return_value.get_config.return_value = self.mock_config

        mock_client = Mock(spec=EdgarClient)
        mock_response = Mock()
        mock_response.raise_for_status = Mock()
        mock_response.json.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)
        mock_client.get.return_value = mock_response
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=None)
        mock_client_class.return_value = mock_client

        with pytest.raises(RuntimeError) as exc_info:
            fetch_cik_company_list()

        assert "Failed to fetch company list" in str(exc_info.value)

    @patch("edgar.companies.ConfigManager")
    @patch("edgar.companies.EdgarClient")
    def test_fetch_cik_company_list_network_error(
        self, mock_client_class, mock_config_manager
    ):
        """Test handling of network errors."""
        mock_config_manager.return_value.get_config.return_value = self.mock_config

        mock_client = Mock(spec=EdgarClient)
        mock_client.get.side_effect = Exception("Network timeout")
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=None)
        mock_client_class.return_value = mock_client

        with pytest.raises(RuntimeError) as exc_info:
            fetch_cik_company_list()

        assert "Failed to fetch company list" in str(exc_info.value)
        assert "Network timeout" in str(exc_info.value)

    @patch("edgar.companies.ConfigManager")
    @patch("edgar.companies.EdgarClient")
    def test_fetch_cik_company_list_cik_formatting(
        self, mock_client_class, mock_config_manager
    ):
        """Test proper CIK formatting with different input formats."""
        # Test data with various CIK formats
        test_tickers_data = {
            "0": {
                "cik_str": 320193,  # 6 digits
                "ticker": "AAPL",
                "title": "Apple Inc.",
            },
            "1": {
                "cik_str": 1,  # 1 digit
                "ticker": "TEST1",
                "title": "Test Company 1",
            },
            "2": {
                "cik_str": 12345,  # 5 digits
                "ticker": "TEST2",
                "title": "Test Company 2",
            },
            "3": {
                "cik_str": 1234567890,  # 10 digits
                "ticker": "TEST3",
                "title": "Test Company 3",
            },
        }

        expected_formatted = [
            {"cik": "0000320193", "name": "Apple Inc."},
            {"cik": "0000000001", "name": "Test Company 1"},
            {"cik": "0000012345", "name": "Test Company 2"},
            {"cik": "1234567890", "name": "Test Company 3"},
        ]

        mock_config_manager.return_value.get_config.return_value = self.mock_config

        mock_client = Mock(spec=EdgarClient)
        mock_response = Mock()
        mock_response.raise_for_status = Mock()
        mock_response.json.return_value = test_tickers_data
        mock_client.get.return_value = mock_response
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=None)
        mock_client_class.return_value = mock_client

        result = fetch_cik_company_list()

        assert result == expected_formatted

    @patch("edgar.companies.ConfigManager")
    @patch("edgar.companies.EdgarClient")
    def test_fetch_cik_company_list_title_case_formatting(
        self, mock_client_class, mock_config_manager
    ):
        """Test proper title case formatting of company names."""
        test_tickers_data = {
            "0": {
                "cik_str": 320193,
                "ticker": "AAPL",
                "title": "APPLE INC.",  # All caps
            },
            "1": {
                "cik_str": 789019,
                "ticker": "MSFT",
                "title": "microsoft corp",  # All lowercase
            },
            "2": {
                "cik_str": 1652044,
                "ticker": "GOOGL",
                "title": "Alphabet Inc.",  # Already proper case
            },
            "3": {
                "cik_str": 1018724,
                "ticker": "AMZN",
                "title": "amazon.com, inc.",  # Mixed with punctuation
            },
        }

        expected_formatted = [
            {"cik": "0000320193", "name": "Apple Inc."},
            {"cik": "0000789019", "name": "Microsoft Corp"},
            {"cik": "0001652044", "name": "Alphabet Inc."},
            {"cik": "0001018724", "name": "Amazon.Com, Inc."},
        ]

        mock_config_manager.return_value.get_config.return_value = self.mock_config

        mock_client = Mock(spec=EdgarClient)
        mock_response = Mock()
        mock_response.raise_for_status = Mock()
        mock_response.json.return_value = test_tickers_data
        mock_client.get.return_value = mock_response
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=None)
        mock_client_class.return_value = mock_client

        result = fetch_cik_company_list()

        assert result == expected_formatted

    @patch("edgar.companies.ConfigManager")
    @patch("edgar.companies.EdgarClient")
    def test_fetch_cik_company_list_missing_title(
        self, mock_client_class, mock_config_manager
    ):
        """Test handling of entries with missing title field."""
        test_tickers_data = {
            "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
            "1": {
                "cik_str": 789019,
                "ticker": "MSFT",
                # Missing title field
            },
            "2": {"cik_str": 1652044, "ticker": "GOOGL", "title": ""},  # Empty title
        }

        expected_formatted = [
            {"cik": "0000320193", "name": "Apple Inc."},
            {"cik": "0000789019", "name": ""},  # Missing title becomes empty string
            {"cik": "0001652044", "name": ""},  # Empty title stays empty
        ]

        mock_config_manager.return_value.get_config.return_value = self.mock_config

        mock_client = Mock(spec=EdgarClient)
        mock_response = Mock()
        mock_response.raise_for_status = Mock()
        mock_response.json.return_value = test_tickers_data
        mock_client.get.return_value = mock_response
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=None)
        mock_client_class.return_value = mock_client

        result = fetch_cik_company_list()

        assert result == expected_formatted

    @patch("edgar.companies.ConfigManager")
    @patch("edgar.companies.EdgarClient")
    def test_fetch_cik_company_list_malformed_entry(
        self, mock_client_class, mock_config_manager
    ):
        """Test handling of malformed entries."""
        test_tickers_data = {
            "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
            "1": {
                "cik_str": "not_a_number",  # Invalid CIK
                "ticker": "INVALID",
                "title": "Invalid Company",
            },
            "2": {
                # Missing cik_str field entirely
                "ticker": "MISSING",
                "title": "Missing CIK Company",
            },
        }

        mock_config_manager.return_value.get_config.return_value = self.mock_config

        mock_client = Mock(spec=EdgarClient)
        mock_response = Mock()
        mock_response.raise_for_status = Mock()
        mock_response.json.return_value = test_tickers_data
        mock_client.get.return_value = mock_response
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=None)
        mock_client_class.return_value = mock_client

        # Function should handle errors gracefully and fail with RuntimeError
        with pytest.raises(RuntimeError) as exc_info:
            fetch_cik_company_list()

        assert "Failed to fetch company list" in str(exc_info.value)

    @patch("edgar.companies.ConfigManager")
    def test_fetch_cik_company_list_config_error(self, mock_config_manager):
        """Test handling of configuration errors."""
        mock_config_manager.side_effect = Exception("Config error")

        with pytest.raises(RuntimeError) as exc_info:
            fetch_cik_company_list()

        assert "Failed to fetch company list" in str(exc_info.value)
        assert "Config error" in str(exc_info.value)

    @patch("edgar.companies.ConfigManager")
    @patch("edgar.companies.EdgarClient")
    def test_fetch_cik_company_list_client_config_creation(
        self, mock_client_class, mock_config_manager
    ):
        """Test that ClientConfig is properly created from EdgarConfig."""
        mock_config = EdgarConfig(
            rate_limit_per_sec=5.0,
            num_workers=8,
            timeout=60,
            user_agent="Custom Agent (custom@example.com)",
        )
        mock_config_manager.return_value.get_config.return_value = mock_config

        mock_client = Mock(spec=EdgarClient)
        mock_response = Mock()
        mock_response.raise_for_status = Mock()
        mock_response.json.return_value = {}
        mock_client.get.return_value = mock_response
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=None)
        mock_client_class.return_value = mock_client

        fetch_cik_company_list()

        # Verify ClientConfig was created with correct parameters
        mock_client_class.assert_called_once()
        call_args = mock_client_class.call_args[0][
            0
        ]  # First positional argument (ClientConfig)

        assert call_args.rate_limit_per_sec == 5.0
        assert call_args.timeout == 60
        assert call_args.user_agent == "Custom Agent (custom@example.com)"

    def test_company_tickers_url_constant(self):
        """Test that the company tickers URL constant is correct."""
        assert COMPANY_TICKERS_URL == "https://www.sec.gov/files/company_tickers.json"

    @patch("edgar.companies.ConfigManager")
    @patch("edgar.companies.EdgarClient")
    def test_fetch_cik_company_list_large_dataset(
        self, mock_client_class, mock_config_manager
    ):
        """Test performance and correctness with large dataset."""
        # Generate large test dataset
        large_tickers_data = {}
        expected_large_companies = []

        for i in range(1000):  # Simulate 1000 companies
            cik_num = i + 1
            large_tickers_data[str(i)] = {
                "cik_str": cik_num,
                "ticker": f"TKR{i}",
                "title": f"TEST COMPANY {i}",
            }
            expected_large_companies.append(
                {
                    "cik": f"{cik_num:010d}",  # Format as 10-digit CIK
                    "name": f"Test Company {i}",
                }
            )

        mock_config_manager.return_value.get_config.return_value = self.mock_config

        mock_client = Mock(spec=EdgarClient)
        mock_response = Mock()
        mock_response.raise_for_status = Mock()
        mock_response.json.return_value = large_tickers_data
        mock_client.get.return_value = mock_response
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=None)
        mock_client_class.return_value = mock_client

        result = fetch_cik_company_list()

        assert len(result) == 1000
        assert result == expected_large_companies


class TestCompaniesIntegration:
    """Integration tests for companies module."""

    @patch("edgar.companies.ConfigManager")
    @patch("edgar.companies.EdgarClient")
    def test_realistic_sec_api_response(self, mock_client_class, mock_config_manager):
        """Test with realistic SEC API response structure."""
        # Based on actual SEC company_tickers.json response format
        realistic_response = {
            "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
            "1": {"cik_str": 789019, "ticker": "MSFT", "title": "MICROSOFT CORP"},
            "2": {"cik_str": 1652044, "ticker": "GOOGL", "title": "Alphabet Inc."},
            "3": {"cik_str": 1018724, "ticker": "AMZN", "title": "AMAZON COM INC"},
            "4": {"cik_str": 1045810, "ticker": "NVDA", "title": "NVIDIA CORP"},
            "5": {
                "cik_str": 1326801,
                "ticker": "META",
                "title": "Meta Platforms, Inc.",
            },
            "6": {
                "cik_str": 1067983,
                "ticker": "BRK-B",
                "title": "BERKSHIRE HATHAWAY INC",
            },
            "7": {
                "cik_str": 51143,
                "ticker": "IBM",
                "title": "INTERNATIONAL BUSINESS MACHINES CORP",
            },
        }

        mock_config = EdgarConfig(
            rate_limit_per_sec=10.0,
            num_workers=4,
            timeout=30,
            user_agent="Test Agent (test@example.com)",
        )
        mock_config_manager.return_value.get_config.return_value = mock_config

        mock_client = Mock(spec=EdgarClient)
        mock_response = Mock()
        mock_response.raise_for_status = Mock()
        mock_response.json.return_value = realistic_response
        mock_client.get.return_value = mock_response
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=None)
        mock_client_class.return_value = mock_client

        result = fetch_cik_company_list()

        # Verify structure and data transformation
        assert len(result) == 8

        # Check specific transformations
        apple_entry = next(c for c in result if "Apple" in c["name"])
        assert apple_entry["cik"] == "0000320193"
        assert apple_entry["name"] == "Apple Inc."

        microsoft_entry = next(c for c in result if "Microsoft" in c["name"])
        assert microsoft_entry["cik"] == "0000789019"
        assert microsoft_entry["name"] == "Microsoft Corp"

        # Verify all entries have required structure
        for company in result:
            assert "cik" in company
            assert "name" in company
            assert len(company["cik"]) == 10  # 10-digit CIK format
            assert company["cik"].isdigit()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
