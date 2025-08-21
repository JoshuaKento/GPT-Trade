"""Comprehensive tests for filing processor components."""

import asyncio
import json
import logging
import os
import sys
from dataclasses import asdict
from typing import Dict, List, Set
from unittest.mock import AsyncMock, MagicMock, Mock, call, patch

import aiohttp
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from edgar.client_new import EdgarClient
from edgar.filing_processor import (
    AsyncFilingProcessor,
    FilingInfo,
    FilingProcessor,
    ProcessingResult,
    RateLimiter,
    monitor_cik_sync,
)
from edgar.s3_manager import S3Manager
from edgar.urls import CIKValidationError


class TestProcessingResult:
    """Test ProcessingResult dataclass."""

    def test_processing_result_creation(self):
        """Test creating ProcessingResult with required fields."""
        result = ProcessingResult(
            accession="0000320193-23-000006", success=True, documents_processed=3
        )

        assert result.accession == "0000320193-23-000006"
        assert result.success is True
        assert result.documents_processed == 3
        assert result.error is None
        assert result.uploaded_files == []

    def test_processing_result_with_error(self):
        """Test ProcessingResult with error information."""
        result = ProcessingResult(
            accession="0000320193-23-000006",
            success=False,
            documents_processed=0,
            error="Network timeout",
            uploaded_files=["file1.htm", "file2.htm"],
        )

        assert result.success is False
        assert result.error == "Network timeout"
        assert result.uploaded_files == ["file1.htm", "file2.htm"]

    def test_processing_result_post_init(self):
        """Test that uploaded_files is initialized to empty list if None."""
        result = ProcessingResult(
            accession="0000320193-23-000006",
            success=True,
            documents_processed=1,
            uploaded_files=None,
        )

        assert result.uploaded_files == []


class TestFilingInfo:
    """Test FilingInfo dataclass."""

    def test_filing_info_creation(self):
        """Test creating FilingInfo with required fields."""
        filing = FilingInfo(
            form="10-K",
            accession="0000320193-23-000006",
            primary_document="aapl-20230930.htm",
        )

        assert filing.form == "10-K"
        assert filing.accession == "0000320193-23-000006"
        assert filing.primary_document == "aapl-20230930.htm"
        assert filing.filing_date is None
        assert filing.report_date is None

    def test_filing_info_with_dates(self):
        """Test FilingInfo with optional date fields."""
        filing = FilingInfo(
            form="10-Q",
            accession="0000320193-23-000006",
            primary_document="aapl-20230930.htm",
            filing_date="2023-11-02",
            report_date="2023-09-30",
        )

        assert filing.filing_date == "2023-11-02"
        assert filing.report_date == "2023-09-30"


class TestRateLimiter:
    """Test async rate limiter."""

    @pytest.mark.asyncio
    async def test_rate_limiter_creation(self):
        """Test rate limiter initialization."""
        limiter = RateLimiter(initial_rate=10)
        assert limiter._current_rate == 10
        assert isinstance(limiter._semaphore, asyncio.BoundedSemaphore)

    @pytest.mark.asyncio
    async def test_rate_limiter_acquire(self):
        """Test acquiring rate limit tokens."""
        limiter = RateLimiter(initial_rate=10)

        # Should be able to acquire without blocking
        await limiter.acquire()

        # Verify event loop is set
        assert limiter._loop is not None

    @pytest.mark.asyncio
    async def test_rate_limiter_concurrency(self):
        """Test rate limiter with concurrent requests."""
        limiter = RateLimiter(initial_rate=2)  # Small rate for testing

        async def acquire_token():
            await limiter.acquire()
            return True

        # Should be able to acquire up to the rate limit
        tasks = [acquire_token() for _ in range(2)]
        results = await asyncio.gather(*tasks)

        assert all(results)


class TestFilingProcessor:
    """Test synchronous FilingProcessor."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_client = Mock(spec=EdgarClient)
        self.mock_s3 = Mock(spec=S3Manager)
        self.mock_logger = Mock(spec=logging.Logger)

        self.processor = FilingProcessor(
            edgar_client=self.mock_client,
            s3_manager=self.mock_s3,
            logger=self.mock_logger,
        )

    def test_processor_initialization(self):
        """Test processor initialization."""
        assert self.processor.edgar_client == self.mock_client
        assert self.processor.s3_manager == self.mock_s3
        assert self.processor.logger == self.mock_logger

    def test_processor_default_logger(self):
        """Test processor with default logger."""
        processor = FilingProcessor(
            edgar_client=self.mock_client, s3_manager=self.mock_s3
        )

        assert processor.logger is not None

    def test_get_recent_filings_success(self):
        """Test successful retrieval of recent filings."""
        # Mock API response
        mock_data = {
            "filings": {
                "recent": {
                    "form": ["10-K", "10-Q", "8-K"],
                    "accessionNumber": ["acc1", "acc2", "acc3"],
                    "primaryDocument": ["doc1.htm", "doc2.htm", "doc3.htm"],
                    "filingDate": ["2023-11-01", "2023-08-01", "2023-07-01"],
                    "reportDate": ["2023-09-30", "2023-06-30", "2023-06-15"],
                }
            }
        }

        self.mock_client.get_json.return_value = mock_data

        with patch("edgar.filing_processor.URLBuilder.submissions_url") as mock_url:
            mock_url.return_value = "http://test.com"

            filings = self.processor.get_recent_filings("0000320193")

            assert len(filings) == 3
            assert filings[0].form == "10-K"
            assert filings[0].accession == "acc1"
            assert filings[0].primary_document == "doc1.htm"
            assert filings[0].filing_date == "2023-11-01"
            assert filings[0].report_date == "2023-09-30"

    def test_get_recent_filings_invalid_cik(self):
        """Test get_recent_filings with invalid CIK."""
        with pytest.raises(RuntimeError) as exc_info:
            self.processor.get_recent_filings("invalid")

        assert "Failed to get filings" in str(exc_info.value)

    def test_get_recent_filings_api_error(self):
        """Test get_recent_filings when API call fails."""
        self.mock_client.get_json.side_effect = Exception("API Error")

        with patch("edgar.filing_processor.URLBuilder.submissions_url"):
            with pytest.raises(RuntimeError) as exc_info:
                self.processor.get_recent_filings("0000320193")

            assert "Failed to get filings" in str(exc_info.value)

    def test_get_recent_filings_mismatched_data(self):
        """Test handling of mismatched array lengths in API response."""
        mock_data = {
            "filings": {
                "recent": {
                    "form": ["10-K", "10-Q"],
                    "accessionNumber": ["acc1", "acc2"],
                    "primaryDocument": ["doc1.htm", "doc2.htm"],
                    "filingDate": ["2023-11-01"],  # Shorter array
                    "reportDate": ["2023-09-30"],  # Shorter array
                }
            }
        }

        self.mock_client.get_json.return_value = mock_data

        with patch("edgar.filing_processor.URLBuilder.submissions_url"):
            filings = self.processor.get_recent_filings("0000320193")

            assert len(filings) == 2
            assert filings[0].filing_date == "2023-11-01"
            assert filings[1].filing_date is None  # Missing data

    def test_get_new_filings_no_filter(self):
        """Test getting new filings without form type filter."""
        mock_filings = [
            FilingInfo("10-K", "0000320193-23-000001", "doc1.htm"),
            FilingInfo("10-Q", "0000320193-23-000002", "doc2.htm"),
            FilingInfo("8-K", "0000320193-23-000003", "doc3.htm"),
        ]

        with patch.object(
            self.processor, "get_recent_filings", return_value=mock_filings
        ):
            processed = {"0000320193-23-000002"}  # Second filing already processed

            new_filings = self.processor.get_new_filings("0000320193", processed)

            assert len(new_filings) == 2
            assert new_filings[0].accession == "0000320193-23-000001"
            assert new_filings[1].accession == "0000320193-23-000003"

    def test_get_new_filings_with_form_filter(self):
        """Test getting new filings with form type filter."""
        mock_filings = [
            FilingInfo("10-K", "0000320193-23-000001", "doc1.htm"),
            FilingInfo("10-Q", "0000320193-23-000002", "doc2.htm"),
            FilingInfo("8-K", "0000320193-23-000003", "doc3.htm"),
        ]

        with patch.object(
            self.processor, "get_recent_filings", return_value=mock_filings
        ):
            new_filings = self.processor.get_new_filings(
                "0000320193", set(), form_types=["10-K", "10-Q"]
            )

            assert len(new_filings) == 2
            assert all(f.form in ["10-K", "10-Q"] for f in new_filings)

    def test_get_new_filings_case_insensitive_filter(self):
        """Test form type filtering is case insensitive."""
        mock_filings = [
            FilingInfo("10-k", "acc1", "doc1.htm"),  # lowercase
            FilingInfo("10-Q", "0000320193-23-000002", "doc2.htm"),
        ]

        with patch.object(
            self.processor, "get_recent_filings", return_value=mock_filings
        ):
            new_filings = self.processor.get_new_filings(
                "0000320193", set(), form_types=["10-K"]  # uppercase filter
            )

            assert len(new_filings) == 1
            assert new_filings[0].form == "10-k"

    def test_get_filing_documents_success(self):
        """Test successful retrieval of filing documents."""
        mock_response = Mock()
        mock_response.text = (
            '<table class="tableFile"><tr><td>doc1.htm</td></tr></table>'
        )
        self.mock_client.get.return_value = mock_response

        mock_docs = [{"document": "doc1.htm", "type": "10-K"}]

        with patch("edgar.filing_processor.URLBuilder.filing_index_url") as mock_url:
            with patch(
                "edgar.filing_processor.parse_file_list", return_value=mock_docs
            ):
                mock_url.return_value = "http://test.com"

                documents = self.processor.get_filing_documents("0000320193", "acc1")

                assert documents == mock_docs
                self.mock_client.get.assert_called_once()

    def test_get_filing_documents_error(self):
        """Test get_filing_documents when request fails."""
        self.mock_client.get.side_effect = Exception("Network error")

        with patch("edgar.filing_processor.URLBuilder.filing_index_url"):
            with pytest.raises(RuntimeError) as exc_info:
                self.processor.get_filing_documents("0000320193", "acc1")

            assert "Failed to get documents" in str(exc_info.value)

    def test_process_filing_success(self):
        """Test successful filing processing."""
        filing = FilingInfo("10-K", "0000320193-23-000106", "doc1.htm")

        # Mock document list
        mock_docs = [
            {"document": "doc1.htm", "type": "10-K"},
            {"document": "doc2.htm", "type": "EX-99"},
        ]

        with patch.object(
            self.processor, "get_filing_documents", return_value=mock_docs
        ):
            with patch.object(
                self.processor, "download_document_stream"
            ) as mock_stream:
                # Mock streaming to return some data
                mock_stream.return_value = [b"chunk1", b"chunk2"]

                result = self.processor.process_filing(
                    "0000320193", filing, "bucket", "prefix"
                )

                assert result.success is True
                assert result.accession == "0000320193-23-000106"
                assert result.documents_processed == 2
                assert len(result.uploaded_files) == 2

                # Verify streaming was called for each document
                assert mock_stream.call_count == 2

                # Verify S3 manager was called for uploads
                assert self.mock_s3.upload_bytes.call_count == 2

    def test_process_filing_no_documents(self):
        """Test processing filing with no documents."""
        filing = FilingInfo("10-K", "0000320193-23-000106", "doc1.htm")

        with patch.object(self.processor, "get_filing_documents", return_value=[]):
            result = self.processor.process_filing(
                "0000320193", filing, "bucket", "prefix"
            )

            assert result.success is False
            assert result.error == "No documents found"
            assert result.documents_processed == 0

    def test_process_filing_partial_failure(self):
        """Test filing processing with some document failures."""
        filing = FilingInfo("10-K", "0000320193-23-000106", "doc1.htm")

        mock_docs = [
            {"document": "doc1.htm", "type": "10-K"},
            {"document": "doc2.htm", "type": "EX-99"},
            {"document": None},  # Invalid document
        ]

        def mock_stream_side_effect(cik, accession, doc_name, **kwargs):
            if doc_name == "doc1.htm":
                return [b"chunk1", b"chunk2"]  # Success
            elif doc_name == "doc2.htm":
                return [b"chunk3", b"chunk4"]  # Success
            else:
                raise Exception(
                    "Stream failed"
                )  # Should not be called for None document

        with patch.object(
            self.processor, "get_filing_documents", return_value=mock_docs
        ):
            with patch.object(
                self.processor,
                "download_document_stream",
                side_effect=mock_stream_side_effect,
            ):
                result = self.processor.process_filing(
                    "0000320193", filing, "bucket", "prefix"
                )

                assert result.success is True  # At least some documents processed
                assert result.documents_processed == 2  # Only valid documents

    def test_process_filing_document_download_failure(self):
        """Test handling of individual document download failures."""
        filing = FilingInfo("10-K", "0000320193-23-000106", "doc1.htm")

        mock_docs = [
            {"document": "doc1.htm", "type": "10-K"},
            {"document": "doc2.htm", "type": "EX-99"},
        ]

        def mock_stream_side_effect(cik, accession, doc_name, **kwargs):
            if doc_name == "doc1.htm":
                return [b"chunk1", b"chunk2"]  # Success
            else:
                raise Exception("Stream failed")  # Second document fails

        with patch.object(
            self.processor, "get_filing_documents", return_value=mock_docs
        ):
            with patch.object(
                self.processor,
                "download_document_stream",
                side_effect=mock_stream_side_effect,
            ):
                result = self.processor.process_filing(
                    "0000320193", filing, "bucket", "prefix"
                )

                assert result.success is True  # Partial success
                assert result.documents_processed == 1
                assert len(result.uploaded_files) == 1

    def test_process_filing_complete_failure(self):
        """Test filing processing complete failure."""
        filing = FilingInfo("10-K", "0000320193-23-000106", "doc1.htm")

        with patch.object(
            self.processor, "get_filing_documents", side_effect=Exception("API Error")
        ):
            result = self.processor.process_filing(
                "0000320193", filing, "bucket", "prefix"
            )

            assert result.success is False
            assert result.documents_processed == 0
            assert "API Error" in result.error


class TestAsyncFilingProcessor:
    """Test asynchronous FilingProcessor."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_rate_limiter = Mock(spec=RateLimiter)
        self.mock_rate_limiter.acquire = AsyncMock()

        self.mock_s3 = Mock(spec=S3Manager)
        self.mock_logger = Mock(spec=logging.Logger)

        self.processor = AsyncFilingProcessor(
            rate_limiter=self.mock_rate_limiter,
            s3_manager=self.mock_s3,
            logger=self.mock_logger,
        )

    def test_async_processor_initialization(self):
        """Test async processor initialization."""
        assert self.processor.rate_limiter == self.mock_rate_limiter
        assert self.processor.s3_manager == self.mock_s3
        assert self.processor.logger == self.mock_logger

    @pytest.mark.asyncio
    async def test_download_file_success(self):
        """Test successful file download."""
        # Mock the session property to return a mock session
        mock_session = Mock(spec=aiohttp.ClientSession)
        mock_response = AsyncMock()
        mock_response.raise_for_status = Mock()
        mock_response.read = AsyncMock(return_value=b"file content")

        mock_session.get.return_value.__aenter__ = AsyncMock(return_value=mock_response)
        mock_session.get.return_value.__aexit__ = AsyncMock(return_value=None)

        # Mock the session property
        with patch.object(self.processor, "session", mock_session):
            headers = {"User-Agent": "Test Agent (test@example.com)"}
            url = "http://test.com/file.htm"

            content = await self.processor.download_file(url, headers)

            assert content == b"file content"
            self.mock_rate_limiter.acquire.assert_called_once()

    @pytest.mark.asyncio
    async def test_download_file_failure(self):
        """Test file download failure."""
        mock_session = Mock(spec=aiohttp.ClientSession)
        mock_response = AsyncMock()
        mock_response.raise_for_status.side_effect = aiohttp.ClientError(
            "Download failed"
        )

        mock_session.get.return_value.__aenter__ = AsyncMock(return_value=mock_response)
        mock_session.get.return_value.__aexit__ = AsyncMock(return_value=None)

        with patch.object(self.processor, "session", mock_session):
            headers = {"User-Agent": "Test Agent (test@example.com)"}
            url = "http://test.com/file.htm"

            with pytest.raises(aiohttp.ClientError):
                await self.processor.download_file(url, headers)

    @pytest.mark.asyncio
    async def test_process_document_async_success(self):
        """Test successful async document processing."""
        with patch.object(
            self.processor, "download_file", new_callable=AsyncMock
        ) as mock_download:
            with patch("asyncio.to_thread", new_callable=AsyncMock) as mock_thread:
                mock_download.return_value = b"document content"
                mock_thread.return_value = None

                headers = {"User-Agent": "Test Agent (test@example.com)"}

                result = await self.processor._process_document_async(
                    "0000320193", "acc1", "doc.htm", "bucket", "prefix", headers
                )

                assert result == "prefix/0000320193/acc1/doc.htm"
                mock_thread.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_document_async_failure(self):
        """Test async document processing failure."""
        with patch.object(
            self.processor, "download_file", new_callable=AsyncMock
        ) as mock_download:
            mock_download.side_effect = Exception("Download failed")

            headers = {"User-Agent": "Test Agent (test@example.com)"}

            with pytest.raises(RuntimeError) as exc_info:
                await self.processor._process_document_async(
                    "0000320193", "acc1", "doc.htm", "bucket", "prefix", headers
                )

            assert "Failed to process document" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_process_filing_async_success(self):
        """Test successful async filing processing."""
        filing = FilingInfo("10-K", "acc1", "doc1.htm")
        headers = {"User-Agent": "Test"}
        semaphore = asyncio.Semaphore(1)

        # Mock index download
        index_html = '<table class="tableFile"><tr><td>doc1.htm</td></tr></table>'

        mock_docs = [{"document": "doc1.htm", "type": "10-K"}]

        with patch.object(
            self.processor, "download_file", new_callable=AsyncMock
        ) as mock_download:
            with patch(
                "edgar.filing_processor.parse_file_list", return_value=mock_docs
            ):
                with patch.object(
                    self.processor, "_process_document_async", new_callable=AsyncMock
                ) as mock_process:
                    mock_download.return_value = index_html.encode()
                    mock_process.return_value = "s3_key"

                    result = await self.processor.process_filing_async(
                        "0000320193", filing, "bucket", "prefix", semaphore, headers
                    )

                    assert result.success is True
                    assert result.accession == "0000320193-23-000106"
                    assert result.documents_processed == 1
                    assert result.uploaded_files == ["s3_key"]

    @pytest.mark.asyncio
    async def test_process_filing_async_no_documents(self):
        """Test async filing processing with no documents."""
        filing = FilingInfo("10-K", "acc1", "doc1.htm")
        headers = {"User-Agent": "Test"}
        semaphore = asyncio.Semaphore(1)

        index_html = "<html><body>No table</body></html>"

        with patch.object(
            self.processor, "download_file", new_callable=AsyncMock
        ) as mock_download:
            with patch("edgar.filing_processor.parse_file_list", return_value=[]):
                mock_download.return_value = index_html.encode()

                result = await self.processor.process_filing_async(
                    "0000320193", filing, "bucket", "prefix", semaphore, headers
                )

                assert result.success is False
                assert result.error == "No documents found"

    @pytest.mark.asyncio
    async def test_process_filing_async_partial_failure(self):
        """Test async filing processing with partial document failures."""
        filing = FilingInfo("10-K", "acc1", "doc1.htm")
        headers = {"User-Agent": "Test"}
        semaphore = asyncio.Semaphore(1)

        index_html = '<table class="tableFile"><tr><td>doc1.htm</td></tr></table>'
        mock_docs = [
            {"document": "doc1.htm", "type": "10-K"},
            {"document": "doc2.htm", "type": "EX-99"},
        ]

        with patch.object(
            self.processor, "download_file", new_callable=AsyncMock
        ) as mock_download:
            with patch(
                "edgar.filing_processor.parse_file_list", return_value=mock_docs
            ):
                with patch.object(
                    self.processor, "_process_document_async", new_callable=AsyncMock
                ) as mock_process:
                    mock_download.return_value = index_html.encode()
                    # First document succeeds, second fails
                    mock_process.side_effect = [
                        "s3_key",
                        Exception("Processing failed"),
                    ]

                    result = await self.processor.process_filing_async(
                        "0000320193", filing, "bucket", "prefix", semaphore, headers
                    )

                    assert result.success is True  # Partial success
                    assert result.documents_processed == 1
                    assert result.uploaded_files == ["s3_key"]


class TestMonitorCikSync:
    """Test synchronous CIK monitoring function."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_processor = Mock(spec=FilingProcessor)
        self.state = {}

    def test_monitor_cik_sync_new_filings(self):
        """Test monitoring with new filings."""
        mock_filings = [
            FilingInfo("10-K", "0000320193-23-000001", "doc1.htm"),
            FilingInfo("10-Q", "acc2", "doc2.htm"),
        ]

        mock_results = [
            ProcessingResult("acc1", True, 2),
            ProcessingResult("acc2", True, 1),
        ]

        self.mock_processor.get_new_filings.return_value = mock_filings
        self.mock_processor.process_filing.side_effect = mock_results

        with patch("logging.getLogger"):
            monitor_cik_sync(
                self.mock_processor, "0000320193", "bucket", "prefix", self.state
            )

            # Verify state was updated
            assert "0000320193" in self.state
            assert "acc1" in self.state["0000320193"]
            assert "acc2" in self.state["0000320193"]

            # Verify processing calls
            assert self.mock_processor.process_filing.call_count == 2

    def test_monitor_cik_sync_no_new_filings(self):
        """Test monitoring with no new filings."""
        self.mock_processor.get_new_filings.return_value = []

        with patch("logging.getLogger"):
            monitor_cik_sync(
                self.mock_processor, "0000320193", "bucket", "prefix", self.state
            )

            # Verify no processing occurred
            self.mock_processor.process_filing.assert_not_called()

    def test_monitor_cik_sync_with_existing_state(self):
        """Test monitoring with existing processed filings."""
        # Pre-populate state
        self.state["0000320193"] = {"acc1"}

        mock_filings = [FilingInfo("10-K", "acc2", "doc2.htm")]
        mock_result = ProcessingResult("acc2", True, 1)

        self.mock_processor.get_new_filings.return_value = mock_filings
        self.mock_processor.process_filing.return_value = mock_result

        with patch("logging.getLogger"):
            monitor_cik_sync(
                self.mock_processor, "0000320193", "bucket", "prefix", self.state
            )

            # Verify existing state preserved and new filing added
            assert "acc1" in self.state["0000320193"]
            assert "acc2" in self.state["0000320193"]

    def test_monitor_cik_sync_processing_failure(self):
        """Test monitoring with processing failures."""
        mock_filings = [FilingInfo("10-K", "acc1", "doc1.htm")]
        mock_result = ProcessingResult("acc1", False, 0, error="Processing failed")

        self.mock_processor.get_new_filings.return_value = mock_filings
        self.mock_processor.process_filing.return_value = mock_result

        with patch("logging.getLogger"):
            monitor_cik_sync(
                self.mock_processor, "0000320193", "bucket", "prefix", self.state
            )

            # Verify failed filing not added to state
            processed = self.state.get("0000320193", set())
            assert "acc1" not in processed

    def test_monitor_cik_sync_with_form_filter(self):
        """Test monitoring with form type filter."""
        mock_filings = [FilingInfo("10-K", "acc1", "doc1.htm")]
        mock_results = [ProcessingResult("acc1", True, 1)]

        self.mock_processor.get_new_filings.return_value = mock_filings
        self.mock_processor.process_filing.side_effect = mock_results

        with patch("logging.getLogger"):
            monitor_cik_sync(
                self.mock_processor,
                "0000320193",
                "bucket",
                "prefix",
                self.state,
                form_types=["10-K", "10-Q"],
            )

            # Verify form filter was passed
            self.mock_processor.get_new_filings.assert_called_once_with(
                "0000320193", {"acc1"}, ["10-K", "10-Q"]
            )

    def test_monitor_cik_sync_exception_handling(self):
        """Test monitoring exception handling."""
        self.mock_processor.get_new_filings.side_effect = Exception("API Error")

        with patch("logging.getLogger"):
            with pytest.raises(Exception) as exc_info:
                monitor_cik_sync(
                    self.mock_processor, "0000320193", "bucket", "prefix", self.state
                )

            assert "API Error" in str(exc_info.value)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
