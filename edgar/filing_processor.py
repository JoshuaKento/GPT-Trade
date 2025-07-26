"""Filing processing logic with separated concerns and proper error handling."""
import asyncio
import aiohttp
from dataclasses import dataclass
from typing import List, Dict, Set, Optional, Any
import logging

from .client_new import EdgarClient
from .s3_manager import S3Manager
from .urls import URLBuilder, validate_cik
from .parser import parse_file_list


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


class RateLimiter:
    """Asynchronous token bucket rate limiter."""
    
    def __init__(self, rate: int) -> None:
        self._rate = rate
        self._semaphore = asyncio.BoundedSemaphore(rate)
        self._loop = None
    
    async def acquire(self) -> None:
        """Acquire a rate limit token."""
        if self._loop is None:
            self._loop = asyncio.get_event_loop()
        
        await self._semaphore.acquire()
        # Release the token after 1 second
        self._loop.call_later(1, self._semaphore.release)


class FilingProcessor:
    """Handles processing of SEC filings for a single CIK with proper error handling."""
    
    def __init__(
        self,
        edgar_client: EdgarClient,
        s3_manager: S3Manager,
        logger: Optional[logging.Logger] = None
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
            for i, (form, accession, doc) in enumerate(zip(forms, accessions, primary_docs)):
                filing_info = FilingInfo(
                    form=form,
                    accession=accession,
                    primary_document=doc,
                    filing_date=filing_dates[i] if i < len(filing_dates) else None,
                    report_date=report_dates[i] if i < len(report_dates) else None
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
        form_types: Optional[List[str]] = None
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
            all_filings = [
                f for f in all_filings
                if f.form.upper() in forms_set
            ]
            self.logger.debug(f"Filtered to {len(all_filings)} filings by form type")
        
        # Filter out already processed
        new_filings = [
            f for f in all_filings
            if f.accession not in processed_accessions
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
    
    def process_filing(
        self,
        cik: str,
        filing: FilingInfo,
        bucket: str,
        prefix: str
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
                    error="No documents found"
                )
            
            # Upload all documents
            uploaded_files = []
            for document in documents:
                doc_name = document.get("document")
                if not doc_name:
                    continue
                
                try:
                    # Download document
                    doc_url = URLBuilder.document_url(cik, accession, doc_name)
                    response = self.edgar_client.get(doc_url)
                    
                    # Upload to S3
                    s3_key = f"{prefix}/{cik}/{accession}/{doc_name}"
                    self.s3_manager.upload_bytes(response.content, bucket, s3_key)
                    uploaded_files.append(s3_key)
                    
                    self.logger.debug(f"Uploaded {doc_name} to {s3_key}")
                    
                except Exception as e:
                    self.logger.warning(f"Failed to process document {doc_name}: {e}")
                    continue
            
            return ProcessingResult(
                accession=accession,
                success=len(uploaded_files) > 0,
                documents_processed=len(uploaded_files),
                uploaded_files=uploaded_files
            )
            
        except Exception as e:
            self.logger.error(f"Failed to process filing {accession}: {e}")
            return ProcessingResult(
                accession=accession,
                success=False,
                documents_processed=0,
                error=str(e)
            )


class AsyncFilingProcessor:
    """Async version of filing processor for concurrent operations."""
    
    def __init__(
        self,
        rate_limiter: RateLimiter,
        s3_manager: S3Manager,
        logger: Optional[logging.Logger] = None
    ):
        """Initialize async filing processor.
        
        Args:
            rate_limiter: Rate limiter for API calls
            s3_manager: S3 operations manager
            logger: Optional logger instance
        """
        self.rate_limiter = rate_limiter
        self.s3_manager = s3_manager
        self.logger = logger or logging.getLogger(__name__)
    
    async def download_file(
        self,
        session: aiohttp.ClientSession,
        url: str,
        headers: Dict[str, str]
    ) -> bytes:
        """Download a file with rate limiting.
        
        Args:
            session: aiohttp session
            url: URL to download
            headers: HTTP headers
            
        Returns:
            File content as bytes
        """
        await self.rate_limiter.acquire()
        
        try:
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                return await response.read()
        except Exception as e:
            self.logger.error(f"Failed to download {url}: {e}")
            raise
    
    async def process_filing_async(
        self,
        session: aiohttp.ClientSession,
        headers: Dict[str, str],
        cik: str,
        filing: FilingInfo,
        bucket: str,
        prefix: str,
        semaphore: asyncio.Semaphore
    ) -> ProcessingResult:
        """Process a single filing asynchronously.
        
        Args:
            session: aiohttp session
            headers: HTTP headers for requests
            cik: Central Index Key
            filing: Filing information
            bucket: S3 bucket name
            prefix: S3 key prefix
            semaphore: Concurrency limiter
            
        Returns:
            Processing result
        """
        async with semaphore:
            accession = filing.accession
            
            try:
                # Get filing index
                index_url = URLBuilder.filing_index_url(cik, accession)
                index_data = await self.download_file(session, index_url, headers)
                documents = parse_file_list(index_data.decode("utf-8", errors="replace"))
                
                if not documents:
                    return ProcessingResult(
                        accession=accession,
                        success=False,
                        documents_processed=0,
                        error="No documents found"
                    )
                
                # Process documents concurrently
                tasks = []
                for document in documents:
                    doc_name = document.get("document")
                    if doc_name:
                        task = self._process_document_async(
                            session, headers, cik, accession, doc_name, bucket, prefix
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
                    uploaded_files=uploaded_files
                )
                
            except Exception as e:
                self.logger.error(f"Failed to process filing {accession}: {e}")
                return ProcessingResult(
                    accession=accession,
                    success=False,
                    documents_processed=0,
                    error=str(e)
                )
    
    async def _process_document_async(
        self,
        session: aiohttp.ClientSession,
        headers: Dict[str, str],
        cik: str,
        accession: str,
        document: str,
        bucket: str,
        prefix: str
    ) -> str:
        """Process a single document asynchronously.
        
        Args:
            session: aiohttp session
            headers: HTTP headers
            cik: Central Index Key
            accession: Filing accession number
            document: Document name
            bucket: S3 bucket name
            prefix: S3 key prefix
            
        Returns:
            S3 key of uploaded file
            
        Raises:
            Exception: If processing fails
        """
        try:
            # Download document
            doc_url = URLBuilder.document_url(cik, accession, document)
            data = await self.download_file(session, doc_url, headers)
            
            # Upload to S3
            s3_key = f"{prefix}/{cik}/{accession}/{document}"
            await asyncio.to_thread(self.s3_manager.upload_bytes, data, bucket, s3_key)
            
            self.logger.debug(f"Uploaded {document} to {s3_key}")
            return s3_key
            
        except Exception as e:
            raise RuntimeError(f"Failed to process document {document}: {e}")


def monitor_cik_sync(
    processor: FilingProcessor,
    cik: str,
    bucket: str,
    prefix: str,
    state: Dict[str, Set[str]],
    form_types: Optional[List[str]] = None
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
        logger.info(f"CIK {cik}: {successful}/{len(results)} filings, {total_docs} documents")
        
    except Exception as e:
        logger.error(f"Error monitoring CIK {cik}: {e}")
        raise