"""Enhanced filing processor with database integration and batch processing capabilities."""

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, AsyncContextManager, Dict, Iterator, List, Optional, Set

import aiohttp

from .client_new import EdgarClient
from .database_models import (
    Company,
    DatabaseManager,
    Document,
    Filing,
    JobStatus,
    PerformanceMetric,
    ProcessingJob,
    ProcessingStatus,
)
from .filing_processor import (
    AdaptiveRateLimiter,
    AsyncFilingProcessor,
    FilingInfo,
    FilingProcessor,
    ProcessingResult,
    RateLimiter,
)
from .parser import parse_file_list
from .s3_manager import S3Manager
from .urls import URLBuilder, validate_cik


@dataclass
class BatchProcessingResult:
    """Result of batch processing operation."""
    
    job_id: int
    total_ciks: int
    processed_ciks: int
    total_filings: int
    processed_filings: int
    failed_filings: int
    skipped_filings: int
    start_time: datetime
    end_time: Optional[datetime] = None
    error_message: Optional[str] = None
    performance_metrics: Dict[str, float] = field(default_factory=dict)
    
    @property
    def duration(self) -> Optional[float]:
        """Calculate processing duration in seconds."""
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    @property
    def success_rate(self) -> float:
        """Calculate processing success rate."""
        if self.total_filings == 0:
            return 100.0
        return (self.processed_filings / self.total_filings) * 100.0
    
    @property
    def throughput(self) -> Optional[float]:
        """Calculate processing throughput (filings per minute)."""
        duration = self.duration
        if duration and duration > 0:
            return (self.processed_filings / duration) * 60
        return None


class EnhancedFilingProcessor(FilingProcessor):
    """Enhanced filing processor with database integration and state tracking."""
    
    def __init__(
        self,
        edgar_client: EdgarClient,
        s3_manager: S3Manager,
        db_manager: Optional[DatabaseManager] = None,
        logger: Optional[logging.Logger] = None,
    ):
        """Initialize enhanced filing processor.
        
        Args:
            edgar_client: EDGAR API client
            s3_manager: S3 operations manager
            db_manager: Database manager for persistence
            logger: Optional logger instance
        """
        super().__init__(edgar_client, s3_manager, logger)
        self.db_manager = db_manager or DatabaseManager()
    
    def ensure_company_exists(self, cik: str, ticker: Optional[str] = None, 
                            name: Optional[str] = None) -> bool:
        """Ensure company record exists in database.
        
        Args:
            cik: Central Index Key
            ticker: Optional ticker symbol
            name: Optional company name
            
        Returns:
            True if successful, False otherwise
        """
        company = Company(cik=cik, ticker=ticker, name=name)
        return self.db_manager.create_company(company)
    
    def get_new_filings_with_db(
        self,
        cik: str,
        form_types: Optional[List[str]] = None,
    ) -> List[FilingInfo]:
        """Get filings that haven't been processed yet using database state.
        
        Args:
            cik: Central Index Key
            form_types: Optional list of form types to filter by
            
        Returns:
            List of new filing information
        """
        # Get processed accessions from database
        processed_accessions = self.db_manager.get_processed_accessions(cik)
        
        # Use parent method with database state
        return self.get_new_filings(cik, processed_accessions, form_types)
    
    def process_filing_with_db(
        self, cik: str, filing: FilingInfo, bucket: str, prefix: str
    ) -> ProcessingResult:
        """Process a single filing with database persistence.
        
        Args:
            cik: Central Index Key
            filing: Filing information
            bucket: S3 bucket name
            prefix: S3 key prefix
            
        Returns:
            Processing result with database persistence
        """
        start_time = time.time()
        
        # Ensure company exists
        self.ensure_company_exists(cik)
        
        # Create filing record in database
        db_filing = Filing(
            cik=cik,
            accession=filing.accession,
            form_type=filing.form,
            filing_date=filing.filing_date,
            report_date=filing.report_date,
            primary_document=filing.primary_document,
            status=ProcessingStatus.IN_PROGRESS,
            s3_prefix=prefix,
        )
        
        filing_id = self.db_manager.create_filing(db_filing)
        if not filing_id:
            self.logger.error(f"Failed to create database record for {filing.accession}")
            return ProcessingResult(
                accession=filing.accession,
                success=False,
                documents_processed=0,
                error="Database record creation failed",
            )
        
        try:
            # Process filing using parent method
            result = self.process_filing(cik, filing, bucket, prefix)
            
            # Calculate processing duration
            processing_duration = time.time() - start_time
            
            # Update database with results
            if result.success:
                self.db_manager.update_filing_status(
                    filing_id,
                    ProcessingStatus.COMPLETED,
                    documents_processed=result.documents_processed,
                    processing_duration=processing_duration,
                )
            else:
                self.db_manager.update_filing_status(
                    filing_id,
                    ProcessingStatus.FAILED,
                    error_message=result.error,
                    processing_duration=processing_duration,
                )
            
            return result
            
        except Exception as e:
            processing_duration = time.time() - start_time
            error_msg = f"Unexpected error: {str(e)}"
            
            # Update database with failure
            self.db_manager.update_filing_status(
                filing_id,
                ProcessingStatus.FAILED,
                error_message=error_msg,
                processing_duration=processing_duration,
            )
            
            self.logger.error(f"Failed to process filing {filing.accession}: {e}")
            return ProcessingResult(
                accession=filing.accession,
                success=False,
                documents_processed=0,
                error=error_msg,
            )


class AsyncEnhancedFilingProcessor(AsyncFilingProcessor):
    """Enhanced async filing processor with database integration."""
    
    def __init__(
        self,
        rate_limiter: RateLimiter,
        s3_manager: S3Manager,
        db_manager: Optional[DatabaseManager] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[aiohttp.ClientTimeout] = None,
        connector: Optional[aiohttp.BaseConnector] = None,
        logger: Optional[logging.Logger] = None,
    ):
        """Initialize enhanced async filing processor.
        
        Args:
            rate_limiter: Rate limiter for API calls
            s3_manager: S3 operations manager
            db_manager: Database manager for persistence
            headers: Default HTTP headers for requests
            timeout: Client timeout configuration
            connector: Custom aiohttp connector
            logger: Optional logger instance
        """
        super().__init__(rate_limiter, s3_manager, headers, timeout, connector, logger)
        self.db_manager = db_manager or DatabaseManager()
    
    async def process_filing_async_with_db(
        self,
        cik: str,
        filing: FilingInfo,
        bucket: str,
        prefix: str,
        semaphore: asyncio.Semaphore,
        headers: Optional[Dict[str, str]] = None,
    ) -> ProcessingResult:
        """Process a single filing asynchronously with database persistence.
        
        Args:
            cik: Central Index Key
            filing: Filing information
            bucket: S3 bucket name
            prefix: S3 key prefix
            semaphore: Concurrency limiter
            headers: Optional HTTP headers
            
        Returns:
            Processing result with database persistence
        """
        start_time = time.time()
        
        # Create filing record in database (sync operation)
        db_filing = Filing(
            cik=cik,
            accession=filing.accession,
            form_type=filing.form,
            filing_date=filing.filing_date,
            report_date=filing.report_date,
            primary_document=filing.primary_document,
            status=ProcessingStatus.IN_PROGRESS,
            s3_prefix=prefix,
        )
        
        filing_id = await asyncio.to_thread(self.db_manager.create_filing, db_filing)
        if not filing_id:
            self.logger.error(f"Failed to create database record for {filing.accession}")
            return ProcessingResult(
                accession=filing.accession,
                success=False,
                documents_processed=0,
                error="Database record creation failed",
            )
        
        try:
            # Process filing using parent method
            result = await self.process_filing_async(
                cik, filing, bucket, prefix, semaphore, headers
            )
            
            # Calculate processing duration
            processing_duration = time.time() - start_time
            
            # Update database with results (async)
            if result.success:
                await asyncio.to_thread(
                    self.db_manager.update_filing_status,
                    filing_id,
                    ProcessingStatus.COMPLETED,
                    None,
                    result.documents_processed,
                    processing_duration,
                )
            else:
                await asyncio.to_thread(
                    self.db_manager.update_filing_status,
                    filing_id,
                    ProcessingStatus.FAILED,
                    result.error,
                    result.documents_processed,
                    processing_duration,
                )
            
            return result
            
        except Exception as e:
            processing_duration = time.time() - start_time
            error_msg = f"Unexpected error: {str(e)}"
            
            # Update database with failure (async)
            await asyncio.to_thread(
                self.db_manager.update_filing_status,
                filing_id,
                ProcessingStatus.FAILED,
                error_msg,
                0,
                processing_duration,
            )
            
            self.logger.error(f"Failed to process filing {filing.accession}: {e}")
            return ProcessingResult(
                accession=filing.accession,
                success=False,
                documents_processed=0,
                error=error_msg,
            )


class BatchFilingProcessor:
    """Batch processor for handling multiple tickers with performance optimization."""
    
    def __init__(
        self,
        edgar_client: EdgarClient,
        s3_manager: S3Manager,
        db_manager: Optional[DatabaseManager] = None,
        max_concurrent_ciks: int = 10,
        max_concurrent_filings: int = 50,
        rate_limit: int = 10,
        logger: Optional[logging.Logger] = None,
    ):
        """Initialize batch filing processor.
        
        Args:
            edgar_client: EDGAR API client
            s3_manager: S3 operations manager
            db_manager: Database manager for persistence
            max_concurrent_ciks: Maximum concurrent CIKs to process
            max_concurrent_filings: Maximum concurrent filings per CIK
            rate_limit: Rate limit for API calls
            logger: Optional logger instance
        """
        self.edgar_client = edgar_client
        self.s3_manager = s3_manager
        self.db_manager = db_manager or DatabaseManager()
        self.max_concurrent_ciks = max_concurrent_ciks
        self.max_concurrent_filings = max_concurrent_filings
        self.rate_limit = rate_limit
        self.logger = logger or logging.getLogger(__name__)
        
        # Create processors
        self.sync_processor = EnhancedFilingProcessor(
            edgar_client, s3_manager, db_manager, logger
        )
        
        # Rate limiter for async operations
        self.rate_limiter = AdaptiveRateLimiter(
            initial_rate=rate_limit,
            min_rate=max(1, rate_limit // 4),
            max_rate=rate_limit * 2,
        )
    
    def create_job(
        self,
        job_name: str,
        target_ciks: List[str],
        form_types: Optional[List[str]] = None,
        config: Optional[Dict[str, Any]] = None,
    ) -> Optional[int]:
        """Create a new batch processing job.
        
        Args:
            job_name: Name for the processing job
            target_ciks: List of CIKs to process
            form_types: Optional list of form types to filter
            config: Optional job configuration
            
        Returns:
            Job ID if successful, None otherwise
        """
        job = ProcessingJob(
            job_name=job_name,
            job_type="batch_filing",
            status=JobStatus.CREATED,
            target_ciks=target_ciks,
            form_types=form_types or [],
            config=config or {},
        )
        
        return self.db_manager.create_job(job)
    
    def process_batch_sync(
        self,
        ciks: List[str],
        bucket: str,
        prefix: str,
        form_types: Optional[List[str]] = None,
        job_name: Optional[str] = None,
    ) -> BatchProcessingResult:
        """Process multiple CIKs synchronously with database tracking.
        
        Args:
            ciks: List of CIKs to process
            bucket: S3 bucket name
            prefix: S3 key prefix
            form_types: Optional list of form types to filter
            job_name: Optional job name for tracking
            
        Returns:
            Batch processing result
        """
        start_time = datetime.now(timezone.utc)
        
        # Create job record
        job_id = self.create_job(
            job_name or f"batch_sync_{start_time.strftime('%Y%m%d_%H%M%S')}",
            ciks,
            form_types,
            {
                "bucket": bucket,
                "prefix": prefix,
                "sync_mode": True,
                "max_concurrent_ciks": self.max_concurrent_ciks,
            },
        )
        
        if not job_id:
            self.logger.error("Failed to create job record")
            return BatchProcessingResult(
                job_id=0,
                total_ciks=len(ciks),
                processed_ciks=0,
                total_filings=0,
                processed_filings=0,
                failed_filings=0,
                skipped_filings=0,
                start_time=start_time,
                end_time=datetime.now(timezone.utc),
                error_message="Job creation failed",
            )
        
        # Update job status to running
        self.db_manager.update_job_status(job_id, JobStatus.RUNNING)
        
        result = BatchProcessingResult(
            job_id=job_id,
            total_ciks=len(ciks),
            processed_ciks=0,
            total_filings=0,
            processed_filings=0,
            failed_filings=0,
            skipped_filings=0,
            start_time=start_time,
        )
        
        try:
            # Process each CIK
            for cik_idx, cik in enumerate(ciks):
                try:
                    # Validate CIK
                    validate_cik(cik)
                    
                    # Ensure company exists
                    self.sync_processor.ensure_company_exists(cik)
                    
                    # Get new filings for this CIK
                    new_filings = self.sync_processor.get_new_filings_with_db(
                        cik, form_types
                    )
                    
                    result.total_filings += len(new_filings)
                    
                    # Process filings for this CIK
                    for filing in new_filings:
                        filing_result = self.sync_processor.process_filing_with_db(
                            cik, filing, bucket, prefix
                        )
                        
                        if filing_result.success:
                            result.processed_filings += 1
                        else:
                            result.failed_filings += 1
                    
                    result.processed_ciks += 1
                    
                    # Record progress metrics
                    progress_percentage = (cik_idx + 1) / len(ciks) * 100
                    self.db_manager.record_metric(
                        PerformanceMetric(
                            job_id=job_id,
                            metric_name="cik_progress",
                            metric_value=progress_percentage,
                            metric_unit="percentage",
                        )
                    )
                    
                    self.logger.info(
                        f"Processed CIK {cik}: {len(new_filings)} filings "
                        f"({result.processed_ciks}/{len(ciks)} CIKs complete)"
                    )
                    
                except Exception as e:
                    self.logger.error(f"Failed to process CIK {cik}: {e}")
                    result.failed_filings += 1
            
            # Update job totals
            self.db_manager.update_job_status(
                job_id,
                JobStatus.COMPLETED,
                processed_filings=result.processed_filings,
                failed_filings=result.failed_filings,
            )
            
            result.end_time = datetime.now(timezone.utc)
            
            # Record final metrics
            duration = result.duration
            if duration:
                self.db_manager.record_metric(
                    PerformanceMetric(
                        job_id=job_id,
                        metric_name="total_duration",
                        metric_value=duration,
                        metric_unit="seconds",
                    )
                )
                
                self.db_manager.record_metric(
                    PerformanceMetric(
                        job_id=job_id,
                        metric_name="throughput",
                        metric_value=result.throughput or 0,
                        metric_unit="filings_per_minute",
                    )
                )
            
            self.logger.info(
                f"Batch processing completed: {result.processed_filings}/"
                f"{result.total_filings} filings in {duration:.2f}s"
            )
            
            return result
            
        except Exception as e:
            error_msg = f"Batch processing failed: {str(e)}"
            result.error_message = error_msg
            result.end_time = datetime.now(timezone.utc)
            
            # Update job with failure
            self.db_manager.update_job_status(
                job_id, JobStatus.FAILED, error_message=error_msg
            )
            
            self.logger.error(error_msg)
            return result
    
    async def process_batch_async(
        self,
        ciks: List[str],
        bucket: str,
        prefix: str,
        form_types: Optional[List[str]] = None,
        job_name: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> BatchProcessingResult:
        """Process multiple CIKs asynchronously with optimal performance.
        
        Args:
            ciks: List of CIKs to process
            bucket: S3 bucket name
            prefix: S3 key prefix
            form_types: Optional list of form types to filter
            job_name: Optional job name for tracking
            headers: Optional HTTP headers
            
        Returns:
            Batch processing result
        """
        start_time = datetime.now(timezone.utc)
        
        # Create job record
        job_id = await asyncio.to_thread(
            self.create_job,
            job_name or f"batch_async_{start_time.strftime('%Y%m%d_%H%M%S')}",
            ciks,
            form_types,
            {
                "bucket": bucket,
                "prefix": prefix,
                "async_mode": True,
                "max_concurrent_ciks": self.max_concurrent_ciks,
                "max_concurrent_filings": self.max_concurrent_filings,
            },
        )
        
        if not job_id:
            self.logger.error("Failed to create job record")
            return BatchProcessingResult(
                job_id=0,
                total_ciks=len(ciks),
                processed_ciks=0,
                total_filings=0,
                processed_filings=0,
                failed_filings=0,
                skipped_filings=0,
                start_time=start_time,
                end_time=datetime.now(timezone.utc),
                error_message="Job creation failed",
            )
        
        # Update job status to running
        await asyncio.to_thread(
            self.db_manager.update_job_status, job_id, JobStatus.RUNNING
        )
        
        result = BatchProcessingResult(
            job_id=job_id,
            total_ciks=len(ciks),
            processed_ciks=0,
            total_filings=0,
            processed_filings=0,
            failed_filings=0,
            skipped_filings=0,
            start_time=start_time,
        )
        
        try:
            # Create async processor
            async with AsyncEnhancedFilingProcessor.create_processor(
                self.rate_limiter,
                self.s3_manager,
                headers,
                logger=self.logger,
                db_manager=self.db_manager,
            ) as async_processor:
                
                # Create semaphores for concurrency control
                cik_semaphore = asyncio.Semaphore(self.max_concurrent_ciks)
                filing_semaphore = asyncio.Semaphore(self.max_concurrent_filings)
                
                async def process_cik(cik: str) -> Dict[str, Any]:
                    """Process a single CIK asynchronously."""
                    async with cik_semaphore:
                        try:
                            # Validate CIK
                            validate_cik(cik)
                            
                            # Ensure company exists (sync operation)
                            await asyncio.to_thread(
                                self.sync_processor.ensure_company_exists, cik
                            )
                            
                            # Get new filings for this CIK (sync operation)
                            new_filings = await asyncio.to_thread(
                                self.sync_processor.get_new_filings_with_db,
                                cik,
                                form_types,
                            )
                            
                            # Process filings concurrently
                            filing_tasks = []
                            for filing in new_filings:
                                task = async_processor.process_filing_async_with_db(
                                    cik, filing, bucket, prefix, filing_semaphore, headers
                                )
                                filing_tasks.append(task)
                            
                            filing_results = await asyncio.gather(
                                *filing_tasks, return_exceptions=True
                            )
                            
                            # Count results
                            processed = 0
                            failed = 0
                            for filing_result in filing_results:
                                if isinstance(filing_result, ProcessingResult):
                                    if filing_result.success:
                                        processed += 1
                                    else:
                                        failed += 1
                                else:
                                    failed += 1
                                    self.logger.error(
                                        f"Filing processing exception: {filing_result}"
                                    )
                            
                            return {
                                "cik": cik,
                                "total_filings": len(new_filings),
                                "processed_filings": processed,
                                "failed_filings": failed,
                                "success": True,
                            }
                            
                        except Exception as e:
                            self.logger.error(f"Failed to process CIK {cik}: {e}")
                            return {
                                "cik": cik,
                                "total_filings": 0,
                                "processed_filings": 0,
                                "failed_filings": 0,
                                "success": False,
                                "error": str(e),
                            }
                
                # Process all CIKs concurrently
                cik_tasks = [process_cik(cik) for cik in ciks]
                cik_results = await asyncio.gather(*cik_tasks, return_exceptions=True)
                
                # Aggregate results
                for cik_result in cik_results:
                    if isinstance(cik_result, dict):
                        if cik_result["success"]:
                            result.processed_ciks += 1
                        result.total_filings += cik_result["total_filings"]
                        result.processed_filings += cik_result["processed_filings"]
                        result.failed_filings += cik_result["failed_filings"]
                    else:
                        result.failed_filings += 1
                        self.logger.error(f"CIK processing exception: {cik_result}")
                
                # Update job totals
                await asyncio.to_thread(
                    self.db_manager.update_job_status,
                    job_id,
                    JobStatus.COMPLETED,
                    None,
                    result.processed_filings,
                    result.failed_filings,
                )
                
                result.end_time = datetime.now(timezone.utc)
                
                # Record final metrics
                duration = result.duration
                if duration:
                    await asyncio.to_thread(
                        self.db_manager.record_metric,
                        PerformanceMetric(
                            job_id=job_id,
                            metric_name="total_duration",
                            metric_value=duration,
                            metric_unit="seconds",
                        ),
                    )
                    
                    await asyncio.to_thread(
                        self.db_manager.record_metric,
                        PerformanceMetric(
                            job_id=job_id,
                            metric_name="throughput",
                            metric_value=result.throughput or 0,
                            metric_unit="filings_per_minute",
                        ),
                    )
                
                self.logger.info(
                    f"Async batch processing completed: {result.processed_filings}/"
                    f"{result.total_filings} filings in {duration:.2f}s"
                )
                
                return result
                
        except Exception as e:
            error_msg = f"Async batch processing failed: {str(e)}"
            result.error_message = error_msg
            result.end_time = datetime.now(timezone.utc)
            
            # Update job with failure
            await asyncio.to_thread(
                self.db_manager.update_job_status,
                job_id,
                JobStatus.FAILED,
                error_message=error_msg,
            )
            
            self.logger.error(error_msg)
            return result
    
    def get_job_status(self, job_id: int) -> Optional[Dict[str, Any]]:
        """Get current status of a processing job.
        
        Args:
            job_id: Job ID to query
            
        Returns:
            Job status information or None if not found
        """
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.execute("""
                    SELECT job_name, job_type, status, target_ciks, form_types,
                           total_filings, processed_filings, failed_filings,
                           start_time, end_time, error_message, created_at
                    FROM processing_jobs WHERE id = ?
                """, (job_id,))
                
                row = cursor.fetchone()
                if not row:
                    return None
                
                return {
                    "job_id": job_id,
                    "job_name": row[0],
                    "job_type": row[1],
                    "status": row[2],
                    "target_ciks": json.loads(row[3]) if row[3] else [],
                    "form_types": json.loads(row[4]) if row[4] else [],
                    "total_filings": row[5],
                    "processed_filings": row[6],
                    "failed_filings": row[7],
                    "start_time": row[8],
                    "end_time": row[9],
                    "error_message": row[10],
                    "created_at": row[11],
                }
        except Exception as e:
            self.logger.error(f"Failed to get job status {job_id}: {e}")
            return None