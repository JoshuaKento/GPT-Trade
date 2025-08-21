"""Filing processing logic with separated concerns and proper error handling."""

import asyncio
import io
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncContextManager, Dict, Iterator, List, Optional, Set

import aiohttp

from .client_new import EdgarClient
from .parser import parse_file_list
from .s3_manager import S3Manager
from .urls import URLBuilder, validate_cik


@dataclass
class ProcessingResult:
    """Result of processing a single filing."""

    accession: str
    success: bool
    documents_processed: int
    error: Optional[str] = None
    uploaded_files: Optional[List[str]] = None

    def __post_init__(self):
        if self.uploaded_files is None:
            self.uploaded_files = []


@dataclass
class FilingInfo:
    """Information about a filing."""

    form: str
    accession: str
    primary_document: str
    filing_date: Optional[str] = None
    report_date: Optional[str] = None


class AdaptiveRateLimiter:
    """Enhanced asynchronous rate limiter with adaptive behavior."""

    def __init__(
        self, initial_rate: int, min_rate: int = 1, max_rate: int = 10
    ) -> None:
        self._initial_rate = initial_rate
        self._current_rate = initial_rate
        self._min_rate = min_rate
        self._max_rate = max_rate
        self._semaphore = asyncio.BoundedSemaphore(initial_rate)
        self._loop = None
        self._consecutive_successes = 0
        self._consecutive_failures = 0
        self._last_adjustment = 0
        self._adjustment_cooldown = 30  # seconds

    async def acquire(self) -> None:
        """Acquire a rate limit token."""
        if self._loop is None:
            self._loop = asyncio.get_event_loop()

        await self._semaphore.acquire()
        # Release the token after 1 second
        self._loop.call_later(1, self._semaphore.release)

    def report_success(self) -> None:
        """Report a successful request for rate adaptation."""
        self._consecutive_successes += 1
        self._consecutive_failures = 0

        # Consider increasing rate after sustained success
        if (
            self._consecutive_successes >= 20
            and self._current_rate < self._max_rate
            and self._should_adjust()
        ):
            self._increase_rate()

    def report_failure(self, status_code: int = None) -> None:
        """Report a failed request for rate adaptation."""
        self._consecutive_failures += 1
        self._consecutive_successes = 0

        # Decrease rate on rate limiting or server errors
        if (
            status_code in [429, 503, 504] or self._consecutive_failures >= 3
        ) and self._should_adjust():
            self._decrease_rate()

    def _should_adjust(self) -> bool:
        """Check if enough time has passed since last adjustment."""
        import time

        current_time = time.time()
        if current_time - self._last_adjustment >= self._adjustment_cooldown:
            self._last_adjustment = current_time
            return True
        return False

    def _increase_rate(self) -> None:
        """Increase the rate limit."""
        old_rate = self._current_rate
        self._current_rate = min(self._current_rate + 1, self._max_rate)

        if self._current_rate != old_rate:
            # Create new semaphore with higher limit
            self._semaphore = asyncio.BoundedSemaphore(self._current_rate)
            logging.getLogger(__name__).info(
                f"Increased rate limit from {old_rate} to {self._current_rate}"
            )

    def _decrease_rate(self) -> None:
        """Decrease the rate limit."""
        old_rate = self._current_rate
        self._current_rate = max(self._current_rate - 1, self._min_rate)

        if self._current_rate != old_rate:
            # Create new semaphore with lower limit
            self._semaphore = asyncio.BoundedSemaphore(self._current_rate)
            logging.getLogger(__name__).info(
                f"Decreased rate limit from {old_rate} to {self._current_rate}"
            )

    @property
    def current_rate(self) -> int:
        """Get the current rate limit."""
        return self._current_rate


# Backward compatibility alias
RateLimiter = AdaptiveRateLimiter


class FilingProcessor:
    """Handles processing of SEC filings for a single CIK with proper error handling."""

    def __init__(
        self,
        edgar_client: EdgarClient,
        s3_manager: S3Manager,
        logger: Optional[logging.Logger] = None,
    ):
        """Initialize filing processor.

        Args:
            edgar_client: EDGAR API client
            s3_manager: S3 operations manager
            logger: Optional logger instance
        """
        self.edgar_client = edgar_client
        self.s3_manager = s3_manager
        self.logger = logger or logging.getLogger(__name__)

    def get_recent_filings(self, cik: str) -> List[FilingInfo]:
        """Get recent filings for a CIK.

        Args:
            cik: Central Index Key

        Returns:
            List of filing information

        Raises:
            ValueError: If CIK is invalid
            RuntimeError: If API request fails
        """
        try:
            validate_cik(cik)  # Validate early
            url = URLBuilder.submissions_url(cik)
            data = self.edgar_client.get_json(url)

            # Extract recent filings
            recent = data.get("filings", {}).get("recent", {})
            forms = recent.get("form", [])
            accessions = recent.get("accessionNumber", [])
            primary_docs = recent.get("primaryDocument", [])
            filing_dates = recent.get("filingDate", [])
            report_dates = recent.get("reportDate", [])

            filings = []
            for i, (form, accession, doc) in enumerate(
                zip(forms, accessions, primary_docs)
            ):
                filing_info = FilingInfo(
                    form=form,
                    accession=accession,
                    primary_document=doc,
                    filing_date=filing_dates[i] if i < len(filing_dates) else None,
                    report_date=report_dates[i] if i < len(report_dates) else None,
                )
                filings.append(filing_info)

            self.logger.info(f"Retrieved {len(filings)} filings for CIK {cik}")
            return filings

        except Exception as e:
            self.logger.error(f"Failed to get filings for CIK {cik}: {e}")
            raise RuntimeError(f"Failed to get filings for CIK {cik}: {e}")

    def get_new_filings(
        self,
        cik: str,
        processed_accessions: Set[str],
        form_types: Optional[List[str]] = None,
    ) -> List[FilingInfo]:
        """Get filings that haven't been processed yet.

        Args:
            cik: Central Index Key
            processed_accessions: Set of already processed accession numbers
            form_types: Optional list of form types to filter by

        Returns:
            List of new filing information
        """
        all_filings = self.get_recent_filings(cik)

        # Filter by form type if specified
        if form_types:
            forms_set = {f.upper() for f in form_types}
            all_filings = [f for f in all_filings if f.form.upper() in forms_set]
            self.logger.debug(f"Filtered to {len(all_filings)} filings by form type")

        # Filter out already processed
        new_filings = [
            f for f in all_filings if f.accession not in processed_accessions
        ]

        self.logger.info(
            f"CIK {cik}: {len(all_filings)} total, {len(new_filings)} new filings"
        )
        return new_filings

    def get_filing_documents(self, cik: str, accession: str) -> List[Dict[str, str]]:
        """Get list of documents in a filing.

        Args:
            cik: Central Index Key
            accession: Filing accession number

        Returns:
            List of document information dictionaries
        """
        try:
            url = URLBuilder.filing_index_url(cik, accession)
            response = self.edgar_client.get(url)
            documents = parse_file_list(response.text)

            self.logger.debug(f"Found {len(documents)} documents in filing {accession}")
            return documents

        except Exception as e:
            self.logger.error(f"Failed to get documents for {accession}: {e}")
            raise RuntimeError(f"Failed to get documents for {accession}: {e}")

    def download_document_stream(
        self, cik: str, accession: str, document: str, chunk_size: int = 8192
    ) -> Iterator[bytes]:
        """Download document as a stream to reduce memory usage.

        Args:
            cik: Central Index Key
            accession: Filing accession number
            document: Document name
            chunk_size: Size of chunks to read

        Yields:
            Chunks of document data as bytes
        """
        try:
            doc_url = URLBuilder.document_url(cik, accession, document)
            response = self.edgar_client.get_stream(doc_url, chunk_size=chunk_size)

            for chunk in response.iter_content(
                chunk_size=chunk_size, decode_unicode=False
            ):
                if chunk:  # Filter out keep-alive chunks
                    yield chunk

        except Exception as e:
            self.logger.error(f"Failed to stream document {document}: {e}")
            raise RuntimeError(f"Failed to stream document {document}: {e}")
        finally:
            if "response" in locals():
                response.close()

    def _upload_document_streaming(
        self, cik: str, accession: str, document: str, bucket: str, s3_key: str
    ) -> None:
        """Upload document using streaming to minimize memory usage.

        Args:
            cik: Central Index Key
            accession: Filing accession number
            document: Document name
            bucket: S3 bucket name
            s3_key: S3 object key
        """
        # Create a BytesIO buffer to collect streaming data
        buffer = io.BytesIO()

        # Stream the document data
        for chunk in self.download_document_stream(cik, accession, document):
            buffer.write(chunk)

        # Upload the collected data
        buffer.seek(0)
        self.s3_manager.upload_bytes(buffer.getvalue(), bucket, s3_key)
        buffer.close()

    def process_filing(
        self, cik: str, filing: FilingInfo, bucket: str, prefix: str
    ) -> ProcessingResult:
        """Process a single filing - get files and upload to S3.

        Args:
            cik: Central Index Key
            filing: Filing information
            bucket: S3 bucket name
            prefix: S3 key prefix

        Returns:
            Processing result with success status and details
        """
        accession = filing.accession

        try:
            # Get list of documents in this filing
            documents = self.get_filing_documents(cik, accession)
            if not documents:
                return ProcessingResult(
                    accession=accession,
                    success=False,
                    documents_processed=0,
                    error="No documents found",
                )

            # Upload all documents
            uploaded_files = []
            for document in documents:
                doc_name = document.get("document")
                if not doc_name:
                    continue

                try:
                    # Check if this might be a large file (use streaming for efficiency)
                    doc_url = URLBuilder.document_url(cik, accession, doc_name)

                    # Use streaming upload for better memory efficiency
                    s3_key = f"{prefix}/{cik}/{accession}/{doc_name}"
                    self._upload_document_streaming(
                        cik, accession, doc_name, bucket, s3_key
                    )
                    uploaded_files.append(s3_key)

                    self.logger.debug(f"Uploaded {doc_name} to {s3_key}")

                except Exception as e:
                    self.logger.warning(f"Failed to process document {doc_name}: {e}")
                    continue

            return ProcessingResult(
                accession=accession,
                success=len(uploaded_files) > 0,
                documents_processed=len(uploaded_files),
                uploaded_files=uploaded_files,
            )

        except Exception as e:
            self.logger.error(f"Failed to process filing {accession}: {e}")
            return ProcessingResult(
                accession=accession, success=False, documents_processed=0, error=str(e)
            )


class AsyncFilingProcessor:
    """Async version of filing processor for concurrent operations with proper resource management."""

    def __init__(
        self,
        rate_limiter: RateLimiter,
        s3_manager: S3Manager,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[aiohttp.ClientTimeout] = None,
        connector: Optional[aiohttp.BaseConnector] = None,
        logger: Optional[logging.Logger] = None,
    ):
        """Initialize async filing processor.

        Args:
            rate_limiter: Rate limiter for API calls
            s3_manager: S3 operations manager
            headers: Default HTTP headers for requests
            timeout: Client timeout configuration
            connector: Custom aiohttp connector
            logger: Optional logger instance
        """
        self.rate_limiter = rate_limiter
        self.s3_manager = s3_manager
        self.headers = headers or {}
        self.timeout = timeout or aiohttp.ClientTimeout(total=30)
        self.connector = connector
        self.logger = logger or logging.getLogger(__name__)
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_closed = False

    async def __aenter__(self) -> "AsyncFilingProcessor":
        """Async context manager entry."""
        if self._session is None or self._session.closed:
            connector_kwargs = {}
            if self.connector:
                connector_kwargs["connector"] = self.connector
            elif hasattr(aiohttp, "TCPConnector"):
                # Create optimized TCPConnector with enhanced settings
                connector_kwargs["connector"] = aiohttp.TCPConnector(
                    limit=200,  # Increased total connection pool size
                    limit_per_host=50,  # Increased per-host connection limit
                    ttl_dns_cache=300,  # DNS cache TTL
                    use_dns_cache=True,
                    enable_cleanup_closed=True,  # Enable cleanup of closed connections
                    keepalive_timeout=30,  # Keep-alive timeout
                    # Performance optimizations
                    force_close=False,  # Reuse connections
                    family=0,  # Allow both IPv4 and IPv6
                    # SSL optimizations
                    ssl=False,  # For SEC.gov (can be overridden per request)
                )

            # Create enhanced timeout configuration
            timeout = aiohttp.ClientTimeout(
                total=(
                    self.timeout.total
                    if hasattr(self.timeout, "total")
                    else self.timeout
                ),
                connect=10,  # Connection timeout
                sock_read=30,  # Socket read timeout
                sock_connect=10,  # Socket connect timeout
            )

            self._session = aiohttp.ClientSession(
                timeout=timeout,
                headers=self.headers,
                # Optimize for performance
                skip_auto_headers=["User-Agent"],  # We set it manually
                read_bufsize=65536,  # 64KB read buffer
                **connector_kwargs,
            )
            self._session_closed = False
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit with proper cleanup."""
        await self.cleanup()

    async def cleanup(self) -> None:
        """Clean up resources properly."""
        if self._session and not self._session_closed:
            try:
                await self._session.close()
                # Give time for underlying connections to close
                await asyncio.sleep(0.1)
            except Exception as e:
                self.logger.warning(f"Error closing session: {e}")
            finally:
                self._session_closed = True

    @property
    def session(self) -> aiohttp.ClientSession:
        """Get the current session, raising an error if not initialized."""
        if self._session is None or self._session.closed:
            raise RuntimeError(
                "AsyncFilingProcessor must be used as an async context manager"
            )
        return self._session

    async def download_file(
        self, url: str, headers: Optional[Dict[str, str]] = None
    ) -> bytes:
        """Download a file with adaptive rate limiting.

        Args:
            url: URL to download
            headers: Optional HTTP headers to merge with defaults

        Returns:
            File content as bytes
        """
        await self.rate_limiter.acquire()

        # Merge headers with defaults
        request_headers = {**self.headers}
        if headers:
            request_headers.update(headers)

        try:
            async with self.session.get(url, headers=request_headers) as response:
                # Report status to adaptive rate limiter
                if response.status == 200:
                    self.rate_limiter.report_success()
                else:
                    self.rate_limiter.report_failure(response.status)

                response.raise_for_status()
                return await response.read()

        except Exception as e:
            # Report failure for exceptions
            status_code = getattr(e, "status", None) if hasattr(e, "status") else None
            self.rate_limiter.report_failure(status_code)
            self.logger.error(f"Failed to download {url}: {e}")
            raise

    async def process_filing_async(
        self,
        cik: str,
        filing: FilingInfo,
        bucket: str,
        prefix: str,
        semaphore: asyncio.Semaphore,
        headers: Optional[Dict[str, str]] = None,
    ) -> ProcessingResult:
        """Process a single filing asynchronously.

        Args:
            cik: Central Index Key
            filing: Filing information
            bucket: S3 bucket name
            prefix: S3 key prefix
            semaphore: Concurrency limiter
            headers: Optional HTTP headers to merge with defaults

        Returns:
            Processing result
        """
        async with semaphore:
            accession = filing.accession

            try:
                # Get filing index
                index_url = URLBuilder.filing_index_url(cik, accession)
                index_data = await self.download_file(index_url, headers)
                documents = parse_file_list(
                    index_data.decode("utf-8", errors="replace")
                )

                if not documents:
                    return ProcessingResult(
                        accession=accession,
                        success=False,
                        documents_processed=0,
                        error="No documents found",
                    )

                # Process documents concurrently
                tasks = []
                for document in documents:
                    doc_name = document.get("document")
                    if doc_name:
                        task = self._process_document_async(
                            cik, accession, doc_name, bucket, prefix, headers
                        )
                        tasks.append(task)

                results = await asyncio.gather(*tasks, return_exceptions=True)

                # Count successful uploads
                uploaded_files = []
                for result in results:
                    if isinstance(result, str):  # Success case returns S3 key
                        uploaded_files.append(result)
                    elif isinstance(result, Exception):
                        self.logger.warning(f"Document processing failed: {result}")

                return ProcessingResult(
                    accession=accession,
                    success=len(uploaded_files) > 0,
                    documents_processed=len(uploaded_files),
                    uploaded_files=uploaded_files,
                )

            except Exception as e:
                self.logger.error(f"Failed to process filing {accession}: {e}")
                return ProcessingResult(
                    accession=accession,
                    success=False,
                    documents_processed=0,
                    error=str(e),
                )

    async def _process_document_async(
        self,
        cik: str,
        accession: str,
        document: str,
        bucket: str,
        prefix: str,
        headers: Optional[Dict[str, str]] = None,
    ) -> str:
        """Process a single document asynchronously.

        Args:
            cik: Central Index Key
            accession: Filing accession number
            document: Document name
            bucket: S3 bucket name
            prefix: S3 key prefix
            headers: Optional HTTP headers to merge with defaults

        Returns:
            S3 key of uploaded file

        Raises:
            Exception: If processing fails
        """
        try:
            # Download document
            doc_url = URLBuilder.document_url(cik, accession, document)
            data = await self.download_file(doc_url, headers)

            # Upload to S3
            s3_key = f"{prefix}/{cik}/{accession}/{document}"
            await asyncio.to_thread(self.s3_manager.upload_bytes, data, bucket, s3_key)

            self.logger.debug(f"Uploaded {document} to {s3_key}")
            return s3_key

        except Exception as e:
            raise RuntimeError(f"Failed to process document {document}: {e}")

    @classmethod
    @asynccontextmanager
    async def create_processor(
        cls,
        rate_limiter: RateLimiter,
        s3_manager: S3Manager,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[aiohttp.ClientTimeout] = None,
        connector: Optional[aiohttp.BaseConnector] = None,
        logger: Optional[logging.Logger] = None,
    ) -> AsyncContextManager["AsyncFilingProcessor"]:
        """Factory method to create and properly manage AsyncFilingProcessor.

        Args:
            rate_limiter: Rate limiter for API calls
            s3_manager: S3 operations manager
            headers: Default HTTP headers for requests
            timeout: Client timeout configuration
            connector: Custom aiohttp connector
            logger: Optional logger instance

        Yields:
            Properly initialized AsyncFilingProcessor

        Example:
            async with AsyncFilingProcessor.create_processor(
                rate_limiter, s3_manager, headers
            ) as processor:
                result = await processor.process_filing_async(...)
        """
        processor = cls(rate_limiter, s3_manager, headers, timeout, connector, logger)
        try:
            async with processor:
                yield processor
        except Exception as e:
            # Ensure cleanup happens even if there's an exception
            await processor.cleanup()
            raise


def monitor_cik_sync(
    processor: FilingProcessor,
    cik: str,
    bucket: str,
    prefix: str,
    state: Dict[str, Set[str]],
    form_types: Optional[List[str]] = None,
) -> None:
    """Monitor a single CIK for new filings (synchronous version).

    Args:
        processor: Filing processor instance
        cik: Central Index Key
        bucket: S3 bucket name
        prefix: S3 key prefix
        state: Processing state dictionary
        form_types: Optional list of form types to filter
    """
    logger = logging.getLogger("monitor")
    logger.info(f"Checking CIK {cik}")

    try:
        processed = state.setdefault(cik, set())
        new_filings = processor.get_new_filings(cik, processed, form_types)

        if not new_filings:
            logger.info(f"No new filings for {cik}")
            return

        # Process all filings
        results = []
        for filing in new_filings:
            result = processor.process_filing(cik, filing, bucket, prefix)
            results.append(result)

            if result.success:
                processed.add(result.accession)
                logger.info(
                    f"Processed {result.accession}: {result.documents_processed} documents"
                )
            else:
                logger.error(f"Failed {result.accession}: {result.error}")

        # Summary statistics
        successful = sum(1 for r in results if r.success)
        total_docs = sum(r.documents_processed for r in results)
        logger.info(
            f"CIK {cik}: {successful}/{len(results)} filings, {total_docs} documents"
        )

    except Exception as e:
        logger.error(f"Error monitoring CIK {cik}: {e}")
        raise
