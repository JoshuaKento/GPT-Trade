"""Comprehensive tests for high-level filing helper functions."""

import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, mock_open, patch

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from edgar.client_new import ClientConfig, EdgarClient
from edgar.config_manager import ConfigManager, EdgarConfig
from edgar.filings import fetch_latest_10k, get_filing_files, list_recent_filings
from edgar.urls import AccessionValidationError, CIKValidationError


class TestFetchLatest10K:
    """Test fetch_latest_10k function."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_config = EdgarConfig(
            rate_limit_per_sec=10.0,
            num_workers=4,
            timeout=30,
            user_agent="Test Agent (test@example.com)",
        )

        # Sample submissions data
        self.sample_submissions = {
            "filings": {
                "recent": {
                    "form": ["10-K", "10-Q", "8-K", "10-K"],
                    "accessionNumber": ["acc1", "acc2", "acc3", "acc4"],
                    "primaryDocument": ["doc1.htm", "doc2.htm", "doc3.htm", "doc4.htm"],
                }
            }
        }

    @patch("edgar.filings.ConfigManager")
    @patch("edgar.filings.EdgarClient")
    @patch("edgar.filings.URLBuilder.submissions_url")
    @patch("edgar.filings.URLBuilder.document_url")
    @patch("builtins.open", new_callable=mock_open)
    @patch("edgar.filings.Path")
    def test_fetch_latest_10k_success(
        self,
        mock_path,
        mock_file,
        mock_doc_url,
        mock_sub_url,
        mock_client_class,
        mock_config_manager,
    ):
        """Test successful 10-K download."""
        # Setup mocks
        mock_config_manager.return_value.get_config.return_value = self.mock_config
        mock_sub_url.return_value = "http://submissions.url"
        mock_doc_url.return_value = "http://document.url"

        mock_client = Mock(spec=EdgarClient)
        mock_client.get_json.return_value = self.sample_submissions
        mock_response = Mock()
        mock_response.content = b"10-K document content"
        mock_client.get.return_value = mock_response
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=None)
        mock_client_class.return_value = mock_client

        # Mock path operations
        mock_path_instance = Mock()
        mock_path_instance.mkdir = Mock()
        mock_path_instance.__truediv__ = Mock(return_value="download_dir/doc1.htm")
        mock_path.return_value = mock_path_instance

        # Call function
        result = fetch_latest_10k("320193", "download_dir")

        # Verify result
        assert result == "download_dir/doc1.htm"

        # Verify calls
        mock_config_manager.assert_called_once()
        mock_client.get_json.assert_called_once_with("http://submissions.url")
        mock_client.get.assert_called_once_with("http://document.url")
        mock_file.assert_called_once()
        mock_path_instance.mkdir.assert_called_once_with(parents=True, exist_ok=True)

    @patch("edgar.filings.ConfigManager")
    @patch("edgar.filings.EdgarClient")
    @patch("edgar.filings.URLBuilder.submissions_url")
    def test_fetch_latest_10k_no_10k_found(
        self, mock_sub_url, mock_client_class, mock_config_manager
    ):
        """Test when no 10-K filing is found."""
        # Setup mocks for no 10-K case
        no_10k_submissions = {
            "filings": {
                "recent": {
                    "form": ["10-Q", "8-K"],
                    "accessionNumber": ["acc1", "acc2"],
                    "primaryDocument": ["doc1.htm", "doc2.htm"],
                }
            }
        }

        mock_config_manager.return_value.get_config.return_value = self.mock_config
        mock_sub_url.return_value = "http://submissions.url"

        mock_client = Mock(spec=EdgarClient)
        mock_client.get_json.return_value = no_10k_submissions
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=None)
        mock_client_class.return_value = mock_client

        # Call function
        result = fetch_latest_10k("320193")

        # Verify no file downloaded
        assert result is None
        mock_client.get.assert_not_called()  # Document download not attempted

    def test_fetch_latest_10k_invalid_cik(self):
        """Test with invalid CIK format."""
        with pytest.raises(CIKValidationError):
            fetch_latest_10k("invalid_cik")

    @patch("edgar.filings.ConfigManager")
    @patch("edgar.filings.EdgarClient")
    @patch("edgar.filings.URLBuilder.submissions_url")
    def test_fetch_latest_10k_api_error(
        self, mock_sub_url, mock_client_class, mock_config_manager
    ):
        """Test API request failure."""
        mock_config_manager.return_value.get_config.return_value = self.mock_config
        mock_sub_url.return_value = "http://submissions.url"

        mock_client = Mock(spec=EdgarClient)
        mock_client.get_json.side_effect = Exception("API Error")
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=None)
        mock_client_class.return_value = mock_client

        with pytest.raises(RuntimeError) as exc_info:
            fetch_latest_10k("320193")

        assert "Failed to fetch 10-K" in str(exc_info.value)
        assert "API Error" in str(exc_info.value)

    @patch("edgar.filings.ConfigManager")
    @patch("edgar.filings.EdgarClient")
    @patch("edgar.filings.URLBuilder.submissions_url")
    @patch("edgar.filings.URLBuilder.document_url")
    @patch("builtins.open", new_callable=mock_open)
    @patch("edgar.filings.Path")
    def test_fetch_latest_10k_download_error(
        self,
        mock_path,
        mock_file,
        mock_doc_url,
        mock_sub_url,
        mock_client_class,
        mock_config_manager,
    ):
        """Test document download failure."""
        mock_config_manager.return_value.get_config.return_value = self.mock_config
        mock_sub_url.return_value = "http://submissions.url"
        mock_doc_url.return_value = "http://document.url"

        mock_client = Mock(spec=EdgarClient)
        mock_client.get_json.return_value = self.sample_submissions
        mock_client.get.side_effect = Exception("Download failed")
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=None)
        mock_client_class.return_value = mock_client

        mock_path_instance = Mock()
        mock_path_instance.mkdir = Mock()
        mock_path.return_value = mock_path_instance

        with pytest.raises(RuntimeError) as exc_info:
            fetch_latest_10k("320193")

        assert "Failed to fetch 10-K" in str(exc_info.value)

    @patch("edgar.filings.ConfigManager")
    @patch("edgar.filings.EdgarClient")
    @patch("edgar.filings.URLBuilder.submissions_url")
    @patch("edgar.filings.URLBuilder.document_url")
    @patch("builtins.open", new_callable=mock_open)
    @patch("edgar.filings.Path")
    def test_fetch_latest_10k_custom_directory(
        self,
        mock_path,
        mock_file,
        mock_doc_url,
        mock_sub_url,
        mock_client_class,
        mock_config_manager,
    ):
        """Test 10-K download with custom directory."""
        mock_config_manager.return_value.get_config.return_value = self.mock_config
        mock_sub_url.return_value = "http://submissions.url"
        mock_doc_url.return_value = "http://document.url"

        mock_client = Mock(spec=EdgarClient)
        mock_client.get_json.return_value = self.sample_submissions
        mock_response = Mock()
        mock_response.content = b"content"
        mock_client.get.return_value = mock_response
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=None)
        mock_client_class.return_value = mock_client

        mock_path_instance = Mock()
        mock_path_instance.mkdir = Mock()
        mock_path_instance.__truediv__ = Mock(return_value="custom_dir/doc1.htm")
        mock_path.return_value = mock_path_instance

        result = fetch_latest_10k("320193", "custom_dir")

        assert result == "custom_dir/doc1.htm"
        mock_path.assert_called_once_with("custom_dir")


class TestListRecentFilings:
    """Test list_recent_filings function."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_config = EdgarConfig(
            rate_limit_per_sec=10.0,
            num_workers=4,
            timeout=30,
            user_agent="Test Agent (test@example.com)",
        )

        self.sample_submissions = {
            "filings": {
                "recent": {
                    "form": ["10-K", "10-Q", "8-K"],
                    "accessionNumber": ["acc1", "acc2", "acc3"],
                    "primaryDocument": ["doc1.htm", "doc2.htm", "doc3.htm"],
                }
            }
        }

    @patch("edgar.filings.ConfigManager")
    @patch("edgar.filings.EdgarClient")
    @patch("edgar.filings.URLBuilder.submissions_url")
    def test_list_recent_filings_success(
        self, mock_sub_url, mock_client_class, mock_config_manager
    ):
        """Test successful listing of recent filings."""
        mock_config_manager.return_value.get_config.return_value = self.mock_config
        mock_sub_url.return_value = "http://submissions.url"

        mock_client = Mock(spec=EdgarClient)
        mock_client.get_json.return_value = self.sample_submissions
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=None)
        mock_client_class.return_value = mock_client

        result = list_recent_filings("320193")

        expected = [
            {"form": "10-K", "accession": "acc1", "doc": "doc1.htm"},
            {"form": "10-Q", "accession": "acc2", "doc": "doc2.htm"},
            {"form": "8-K", "accession": "acc3", "doc": "doc3.htm"},
        ]

        assert result == expected
        mock_client.get_json.assert_called_once_with("http://submissions.url")

    def test_list_recent_filings_invalid_cik(self):
        """Test with invalid CIK format."""
        with pytest.raises(CIKValidationError):
            list_recent_filings("invalid_cik")

    @patch("edgar.filings.ConfigManager")
    @patch("edgar.filings.EdgarClient")
    @patch("edgar.filings.URLBuilder.submissions_url")
    def test_list_recent_filings_api_error(
        self, mock_sub_url, mock_client_class, mock_config_manager
    ):
        """Test API request failure."""
        mock_config_manager.return_value.get_config.return_value = self.mock_config
        mock_sub_url.return_value = "http://submissions.url"

        mock_client = Mock(spec=EdgarClient)
        mock_client.get_json.side_effect = Exception("API Error")
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=None)
        mock_client_class.return_value = mock_client

        with pytest.raises(RuntimeError) as exc_info:
            list_recent_filings("320193")

        assert "Failed to get recent filings" in str(exc_info.value)
        assert "API Error" in str(exc_info.value)

    @patch("edgar.filings.ConfigManager")
    @patch("edgar.filings.EdgarClient")
    @patch("edgar.filings.URLBuilder.submissions_url")
    def test_list_recent_filings_empty_response(
        self, mock_sub_url, mock_client_class, mock_config_manager
    ):
        """Test with empty filings response."""
        empty_submissions = {
            "filings": {
                "recent": {"form": [], "accessionNumber": [], "primaryDocument": []}
            }
        }

        mock_config_manager.return_value.get_config.return_value = self.mock_config
        mock_sub_url.return_value = "http://submissions.url"

        mock_client = Mock(spec=EdgarClient)
        mock_client.get_json.return_value = empty_submissions
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=None)
        mock_client_class.return_value = mock_client

        result = list_recent_filings("320193")

        assert result == []

    @patch("edgar.filings.ConfigManager")
    @patch("edgar.filings.EdgarClient")
    @patch("edgar.filings.URLBuilder.submissions_url")
    def test_list_recent_filings_malformed_response(
        self, mock_sub_url, mock_client_class, mock_config_manager
    ):
        """Test with malformed API response."""
        malformed_submissions = {
            "filings": {
                "recent": {
                    "form": ["10-K", "10-Q"],
                    "accessionNumber": ["acc1"],  # Mismatched length
                    "primaryDocument": [
                        "doc1.htm",
                        "doc2.htm",
                        "doc3.htm",
                    ],  # Different length
                }
            }
        }

        mock_config_manager.return_value.get_config.return_value = self.mock_config
        mock_sub_url.return_value = "http://submissions.url"

        mock_client = Mock(spec=EdgarClient)
        mock_client.get_json.return_value = malformed_submissions
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=None)
        mock_client_class.return_value = mock_client

        result = list_recent_filings("320193")

        # Should only process the minimum common length
        assert len(result) == 1
        assert result[0]["form"] == "10-K"
        assert result[0]["accession"] == "acc1"


class TestGetFilingFiles:
    """Test get_filing_files function."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_config = EdgarConfig(
            rate_limit_per_sec=10.0,
            num_workers=4,
            timeout=30,
            user_agent="Test Agent (test@example.com)",
        )

        self.sample_filing_html = """
        <table class="tableFile">
            <tr><th>Seq</th><th>Description</th><th>Document</th><th>Type</th><th>Size</th></tr>
            <tr><td>1</td><td>Main Document</td><td><a href="/doc1.htm">doc1.htm</a></td><td>10-K</td><td>100 KB</td></tr>
            <tr><td>2</td><td>Exhibit</td><td><a href="/doc2.htm">doc2.htm</a></td><td>EX-99</td><td>50 KB</td></tr>
        </table>
        """

        self.sample_parsed_files = [
            {
                "sequence": "1",
                "description": "Main Document",
                "document": "doc1.htm",
                "type": "10-K",
                "size": "100 KB",
            },
            {
                "sequence": "2",
                "description": "Exhibit",
                "document": "doc2.htm",
                "type": "EX-99",
                "size": "50 KB",
            },
        ]

    @patch("edgar.filings.ConfigManager")
    @patch("edgar.filings.EdgarClient")
    @patch("edgar.filings.URLBuilder.filing_index_url")
    @patch("edgar.filings.parse_file_list")
    def test_get_filing_files_success(
        self, mock_parse, mock_index_url, mock_client_class, mock_config_manager
    ):
        """Test successful retrieval of filing files."""
        mock_config_manager.return_value.get_config.return_value = self.mock_config
        mock_index_url.return_value = "http://index.url"
        mock_parse.return_value = self.sample_parsed_files

        mock_client = Mock(spec=EdgarClient)
        mock_response = Mock()
        mock_response.text = self.sample_filing_html
        mock_client.get.return_value = mock_response
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=None)
        mock_client_class.return_value = mock_client

        result = get_filing_files("320193", "0000320193-23-000006")

        assert result == self.sample_parsed_files
        mock_client.get.assert_called_once_with("http://index.url")
        mock_parse.assert_called_once_with(self.sample_filing_html)

    def test_get_filing_files_invalid_cik(self):
        """Test with invalid CIK format."""
        with pytest.raises(CIKValidationError):
            get_filing_files("invalid_cik", "0000320193-23-000006")

    def test_get_filing_files_invalid_accession(self):
        """Test with invalid accession number format."""
        with pytest.raises(AccessionValidationError):
            get_filing_files("320193", "invalid_accession")

    @patch("edgar.filings.ConfigManager")
    @patch("edgar.filings.EdgarClient")
    @patch("edgar.filings.URLBuilder.filing_index_url")
    def test_get_filing_files_api_error(
        self, mock_index_url, mock_client_class, mock_config_manager
    ):
        """Test API request failure."""
        mock_config_manager.return_value.get_config.return_value = self.mock_config
        mock_index_url.return_value = "http://index.url"

        mock_client = Mock(spec=EdgarClient)
        mock_client.get.side_effect = Exception("Network Error")
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=None)
        mock_client_class.return_value = mock_client

        with pytest.raises(RuntimeError) as exc_info:
            get_filing_files("320193", "0000320193-23-000006")

        assert "Failed to get filing files" in str(exc_info.value)
        assert "Network Error" in str(exc_info.value)

    @patch("edgar.filings.ConfigManager")
    @patch("edgar.filings.EdgarClient")
    @patch("edgar.filings.URLBuilder.filing_index_url")
    @patch("edgar.filings.parse_file_list")
    def test_get_filing_files_no_files(
        self, mock_parse, mock_index_url, mock_client_class, mock_config_manager
    ):
        """Test when no files are found in filing."""
        mock_config_manager.return_value.get_config.return_value = self.mock_config
        mock_index_url.return_value = "http://index.url"
        mock_parse.return_value = []  # No files found

        mock_client = Mock(spec=EdgarClient)
        mock_response = Mock()
        mock_response.text = "<html><body>No table</body></html>"
        mock_client.get.return_value = mock_response
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=None)
        mock_client_class.return_value = mock_client

        result = get_filing_files("320193", "0000320193-23-000006")

        assert result == []

    @patch("edgar.filings.ConfigManager")
    @patch("edgar.filings.EdgarClient")
    @patch("edgar.filings.URLBuilder.filing_index_url")
    @patch("edgar.filings.parse_file_list")
    def test_get_filing_files_parse_error(
        self, mock_parse, mock_index_url, mock_client_class, mock_config_manager
    ):
        """Test when file parsing fails."""
        mock_config_manager.return_value.get_config.return_value = self.mock_config
        mock_index_url.return_value = "http://index.url"
        mock_parse.side_effect = Exception("Parse error")

        mock_client = Mock(spec=EdgarClient)
        mock_response = Mock()
        mock_response.text = "invalid html"
        mock_client.get.return_value = mock_response
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=None)
        mock_client_class.return_value = mock_client

        with pytest.raises(RuntimeError) as exc_info:
            get_filing_files("320193", "0000320193-23-000006")

        assert "Failed to get filing files" in str(exc_info.value)

    def test_get_filing_files_validation_preserves_original_exceptions(self):
        """Test that validation exceptions are not wrapped."""
        # CIK validation error should be preserved
        with pytest.raises(CIKValidationError):
            get_filing_files("", "0000320193-23-000006")

        # Accession validation error should be preserved
        with pytest.raises(AccessionValidationError):
            get_filing_files("320193", "")


class TestIntegration:
    """Integration tests using real data structures."""

    @pytest.fixture
    def temp_download_dir(self):
        """Create temporary download directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir

    @patch("edgar.filings.ConfigManager")
    @patch("edgar.filings.EdgarClient")
    @patch("edgar.filings.URLBuilder.submissions_url")
    @patch("edgar.filings.URLBuilder.document_url")
    @patch("builtins.open", new_callable=mock_open)
    def test_fetch_10k_integration(
        self,
        mock_file,
        mock_doc_url,
        mock_sub_url,
        mock_client_class,
        mock_config_manager,
        temp_download_dir,
    ):
        """Test fetch_latest_10k with realistic data flow."""
        # Setup realistic submissions data
        realistic_submissions = {
            "cik": 320193,
            "entityType": "operating",
            "sic": "3571",
            "sicDescription": "Electronic Computers",
            "ownerOrg": None,
            "filings": {
                "recent": {
                    "accessionNumber": [
                        "0000320193-23-000106",
                        "0000320193-23-000064",
                        "0000320193-23-000077",
                    ],
                    "filingDate": ["2023-11-02", "2023-08-04", "2023-07-27"],
                    "reportDate": ["2023-09-30", "2023-06-30", "2023-06-30"],
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

        mock_config = EdgarConfig(
            rate_limit_per_sec=10.0,
            num_workers=4,
            timeout=30,
            user_agent="Test Agent (test@example.com)",
        )

        mock_config_manager.return_value.get_config.return_value = mock_config
        mock_sub_url.return_value = (
            "https://data.sec.gov/submissions/CIK0000320193.json"
        )
        mock_doc_url.return_value = "https://www.sec.gov/Archives/edgar/data/320193/000032019323000106/aapl-20230930.htm"

        mock_client = Mock(spec=EdgarClient)
        mock_client.get_json.return_value = realistic_submissions
        mock_response = Mock()
        mock_response.content = b"<!DOCTYPE html><html>10-K filing content</html>"
        mock_client.get.return_value = mock_response
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=None)
        mock_client_class.return_value = mock_client

        # Mock Path operations
        with patch("edgar.filings.Path") as mock_path:
            mock_path_instance = Mock()
            mock_path_instance.mkdir = Mock()
            mock_path_instance.__truediv__ = Mock(
                return_value=f"{temp_download_dir}/aapl-20230930.htm"
            )
            mock_path.return_value = mock_path_instance

            result = fetch_latest_10k("320193", temp_download_dir)

            assert result == f"{temp_download_dir}/aapl-20230930.htm"

            # Verify the first 10-K was selected (not 10-Q or 8-K)
            mock_doc_url.assert_called_once_with(
                "0000320193", "0000320193-23-000106", "aapl-20230930.htm"
            )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
