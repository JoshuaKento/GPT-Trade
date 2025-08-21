"""Tests for refactored components to ensure they work correctly."""

import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from edgar.client_new import ClientConfig, EdgarClient
from edgar.config_manager import ConfigManager, EdgarConfig
from edgar.parser import parse_file_list
from edgar.s3_manager import (
    S3AccessError,
    S3CredentialsError,
    S3Manager,
    S3NotFoundError,
)
from edgar.state import load_state, save_state, validate_state
from edgar.urls import (
    AccessionValidationError,
    CIKValidationError,
    URLBuilder,
    validate_accession_number,
    validate_cik,
    validate_document_name,
)


class TestURLValidation:
    """Test URL validation and construction."""

    def test_validate_cik_valid(self):
        """Test CIK validation with valid inputs."""
        assert validate_cik("320193") == "0000320193"
        assert validate_cik("0000320193") == "0000320193"
        assert validate_cik("  320193  ") == "0000320193"
        assert validate_cik("1") == "0000000001"

    def test_validate_cik_invalid(self):
        """Test CIK validation with invalid inputs."""
        with pytest.raises(CIKValidationError):
            validate_cik("")

        with pytest.raises(CIKValidationError):
            validate_cik("   ")

        with pytest.raises(CIKValidationError):
            validate_cik("abc123")

        with pytest.raises(CIKValidationError):
            validate_cik("-123")

        with pytest.raises(CIKValidationError):
            validate_cik("12345678901")  # Too many digits

    def test_validate_accession_number_valid(self):
        """Test accession number validation with valid inputs."""
        valid = "0000320193-23-000006"
        assert validate_accession_number(valid) == valid
        assert validate_accession_number("  " + valid + "  ") == valid

    def test_validate_accession_number_invalid(self):
        """Test accession number validation with invalid inputs."""
        with pytest.raises(AccessionValidationError):
            validate_accession_number("")

        with pytest.raises(AccessionValidationError):
            validate_accession_number("invalid-format")

        with pytest.raises(AccessionValidationError):
            validate_accession_number("123456789-23-000006")  # Wrong length

    def test_url_builder(self):
        """Test URL building functionality."""
        cik = "0000320193"
        accession = "0000320193-23-000006"
        document = "aapl-20230930.htm"

        # Test submissions URL
        expected_submissions = "https://data.sec.gov/submissions/CIK0000320193.json"
        assert URLBuilder.submissions_url(cik) == expected_submissions

        # Test filing index URL
        expected_index = "https://www.sec.gov/Archives/edgar/data/320193/000032019323000006/0000320193-23-000006-index.html"
        assert URLBuilder.filing_index_url(cik, accession) == expected_index

        # Test document URL
        expected_doc = "https://www.sec.gov/Archives/edgar/data/320193/000032019323000006/aapl-20230930.htm"
        assert URLBuilder.document_url(cik, accession, document) == expected_doc


class TestEdgarClient:
    """Test EdgarClient functionality."""

    def test_client_config(self):
        """Test client configuration."""
        config = ClientConfig(
            rate_limit_per_sec=10.0, timeout=60, user_agent="Test Agent (test@example.com)"
        )

        assert config.rate_limit_per_sec == 10.0
        assert config.timeout == 60
        assert config.user_agent == "Test Agent (test@example.com)"

    @patch("edgar.client_new.requests.Session")
    @patch.dict(os.environ, {"SEC_USER_AGENT": "Test Agent (test@example.com)"})
    def test_client_initialization(self, mock_session_class):
        """Test client initialization with environment variables."""
        mock_session = Mock()
        mock_session_class.return_value = mock_session

        client = EdgarClient()

        # Verify session was configured
        assert mock_session.headers.update.called

        # Verify user agent from environment
        assert client.config.user_agent == "Test Agent (test@example.com)"

    @patch("edgar.client_new.requests.Session")
    def test_client_rate_limiting(self, mock_session_class):
        """Test that rate limiting is applied."""
        import time

        mock_session = Mock()
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        config = ClientConfig(rate_limit_per_sec=10.0, user_agent="Test Agent (test@example.com)")  # Very fast for testing
        client = EdgarClient(config)

        start_time = time.time()
        client.get("http://test1.com")
        client.get("http://test2.com")
        end_time = time.time()

        # Should have waited at least 0.1 seconds between requests
        assert end_time - start_time >= 0.09  # Account for timing precision


class TestConfigManager:
    """Test configuration management."""

    def test_edgar_config_validation(self):
        """Test configuration validation."""
        # Valid config
        config = EdgarConfig(
            rate_limit_per_sec=6.0,
            num_workers=4,
            user_agent="Test Agent (test@example.com)",
        )
        assert config.rate_limit_per_sec == 6.0

        # Invalid rate limit
        with pytest.raises(ValueError):
            EdgarConfig(rate_limit_per_sec=-1.0)

        # Invalid num_workers
        with pytest.raises(ValueError):
            EdgarConfig(num_workers=0)

        # Empty user agent
        with pytest.raises(ValueError):
            EdgarConfig(user_agent="")

    def test_config_from_dict(self):
        """Test configuration creation from dictionary."""
        data = {
            "rate_limit_per_sec": "8.0",  # String should be converted
            "num_workers": "10",  # String should be converted
            "form_types": "10-K,10-Q",  # Comma-separated should be split
            "user_agent": "Test Agent (test@example.com)",
        }

        config = EdgarConfig.from_dict(data)

        assert config.rate_limit_per_sec == 8.0
        assert config.num_workers == 10
        assert config.form_types == ["10-K", "10-Q"]
        assert config.user_agent == "Test Agent (test@example.com)"

    def test_config_manager_loading(self):
        """Test configuration loading from file."""
        config_data = {
            "rate_limit_per_sec": 5.0,
            "num_workers": 8,
            "user_agent": "Test Agent (test@example.com)",
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(config_data, f)
            temp_path = f.name

        try:
            manager = ConfigManager()
            config = manager.load_config(temp_path)

            assert config.rate_limit_per_sec == 5.0
            assert config.num_workers == 8
            assert config.user_agent == "Test Agent (test@example.com)"
        finally:
            os.unlink(temp_path)


class TestStateManagement:
    """Test state file management."""

    def test_save_load_state(self):
        """Test saving and loading state."""
        state_data = {
            "0000320193": {"acc1", "acc2", "acc3"},
            "0000789019": {"acc4", "acc5"},
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            temp_path = f.name

        try:
            # Save state
            save_state(state_data, temp_path)

            # Load state
            loaded_state = load_state(temp_path)

            assert loaded_state == state_data
            assert isinstance(loaded_state["0000320193"], set)
            assert "acc1" in loaded_state["0000320193"]
        finally:
            os.unlink(temp_path)

    def test_load_nonexistent_state(self):
        """Test loading from nonexistent file."""
        state = load_state("/nonexistent/file.json")
        assert state == {}

    def test_validate_state(self):
        """Test state validation."""
        # Valid state
        valid_state = {"0000320193": {"acc1", "acc2"}}
        validate_state(valid_state)  # Should not raise

        # Invalid state - not a dict
        with pytest.raises(ValueError):
            validate_state("not a dict")

        # Invalid state - CIK not a string
        with pytest.raises(ValueError):
            validate_state({123: {"acc1"}})

        # Invalid state - accessions not a set
        with pytest.raises(ValueError):
            validate_state({"0000320193": ["acc1", "acc2"]})


class TestParser:
    """Test HTML parsing functionality."""

    def test_parse_file_list_valid(self):
        """Test parsing valid HTML."""
        html = """
        <table class="tableFile">
            <tr><th>Seq</th><th>Description</th><th>Document</th><th>Type</th><th>Size</th></tr>
            <tr><td>1</td><td>Main Document</td><td><a href="/test.htm">test.htm</a></td><td>10-K</td><td>100 KB</td></tr>
            <tr><td>2</td><td>Exhibit</td><td><a href="/exhibit.htm">exhibit.htm</a></td><td>EX-99</td><td>50 KB</td></tr>
        </table>
        """

        files = parse_file_list(html)

        assert len(files) == 2
        assert files[0]["sequence"] == "1"
        assert files[0]["document"] == "test.htm"
        assert files[0]["type"] == "10-K"
        assert files[1]["sequence"] == "2"
        assert files[1]["document"] == "exhibit.htm"

    def test_parse_file_list_empty(self):
        """Test parsing HTML with no file table."""
        html = "<html><body>No table here</body></html>"
        files = parse_file_list(html)
        assert files == []

    def test_parse_file_list_invalid_html(self):
        """Test parsing invalid HTML."""
        with pytest.raises(ValueError):
            parse_file_list("")

        with pytest.raises(ValueError):
            parse_file_list("   ")


# Mock S3 tests (since we can't test real S3 without credentials)
class TestS3Manager:
    """Test S3Manager functionality (mocked)."""

    @patch("edgar.s3_manager.boto3.client")
    def test_s3_manager_initialization(self, mock_boto_client):
        """Test S3Manager initialization."""
        mock_client = Mock()
        mock_client.list_buckets.return_value = {}
        mock_boto_client.return_value = mock_client

        manager = S3Manager()

        # Verify boto3 client was created
        mock_boto_client.assert_called_once()
        mock_client.list_buckets.assert_called_once()

    @patch("edgar.s3_manager.boto3.client")
    def test_s3_credentials_error(self, mock_boto_client):
        """Test S3 credentials error handling."""
        from botocore.exceptions import NoCredentialsError

        mock_boto_client.side_effect = NoCredentialsError()

        with pytest.raises(S3CredentialsError):
            S3Manager()

    @patch("edgar.s3_manager.boto3.client")
    def test_s3_upload_bytes(self, mock_boto_client):
        """Test S3 bytes upload."""
        mock_client = Mock()
        mock_client.list_buckets.return_value = {}
        mock_boto_client.return_value = mock_client

        manager = S3Manager()
        data = b"test data"

        manager.upload_bytes(data, "test-bucket", "test-key")

        mock_client.put_object.assert_called_once_with(
            Bucket="test-bucket", Key="test-key", Body=data
        )


if __name__ == "__main__":
    # Run tests if script is executed directly
    pytest.main([__file__, "-v"])
