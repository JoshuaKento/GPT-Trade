"""Job orchestration system for managing EDGAR filing processing workflows."""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Set

from .database_models import (
    DatabaseManager,
    JobStatus,
    PerformanceMetric,
    ProcessingJob,
    ProcessingStatus,
)
from .enhanced_filing_processor import BatchFilingProcessor, BatchProcessingResult


class JobOrchestrator:
    """Orchestrates and manages batch filing processing jobs with resumability."""
    
    def __init__(
        self,
        batch_processor: BatchFilingProcessor,
        db_manager: Optional[DatabaseManager] = None,
        logger: Optional[logging.Logger] = None,
    ):
        """Initialize job orchestrator.
        
        Args:
            batch_processor: Batch filing processor instance
            db_manager: Database manager for job persistence
            logger: Optional logger instance
        """
        self.batch_processor = batch_processor
        self.db_manager = db_manager or DatabaseManager()
        self.logger = logger or logging.getLogger(__name__)
        self._running_jobs: Dict[int, asyncio.Task] = {}
        self._shutdown_event = asyncio.Event()
    
    def create_scheduled_job(
        self,
        job_name: str,
        target_ciks: List[str],
        form_types: Optional[List[str]] = None,
        schedule_config: Optional[Dict[str, Any]] = None,
        processing_config: Optional[Dict[str, Any]] = None,
    ) -> Optional[int]:
        """Create a scheduled processing job.
        
        Args:
            job_name: Name for the processing job
            target_ciks: List of CIKs to process
            form_types: Optional list of form types to filter
            schedule_config: Scheduling configuration
            processing_config: Processing configuration
            
        Returns:
            Job ID if successful, None otherwise
        """
        config = {
            "schedule": schedule_config or {},
            "processing": processing_config or {},
            "created_by": "orchestrator",
            "resumable": True,
        }
        
        return self.batch_processor.create_job(
            job_name, target_ciks, form_types, config
        )
    
    def pause_job(self, job_id: int) -> bool:
        """Pause a running job.
        
        Args:
            job_id: Job ID to pause
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Cancel running task if exists
            if job_id in self._running_jobs:
                task = self._running_jobs[job_id]
                task.cancel()
                del self._running_jobs[job_id]
            
            # Update job status
            return self.db_manager.update_job_status(job_id, JobStatus.PAUSED)
        except Exception as e:
            self.logger.error(f"Failed to pause job {job_id}: {e}")
            return False
    
    def resume_job(self, job_id: int) -> bool:
        """Resume a paused job.
        
        Args:
            job_id: Job ID to resume
            
        Returns:
            True if successful, False otherwise
        """
        try:
            job_info = self.batch_processor.get_job_status(job_id)
            if not job_info:
                self.logger.error(f"Job {job_id} not found")
                return False
            
            if job_info["status"] != JobStatus.PAUSED.value:
                self.logger.warning(f"Job {job_id} is not paused")
                return False
            
            # Update status and restart
            self.db_manager.update_job_status(job_id, JobStatus.RUNNING)
            
            # Create task to resume processing
            task = asyncio.create_task(self._resume_job_processing(job_id))
            self._running_jobs[job_id] = task
            
            return True
        except Exception as e:
            self.logger.error(f"Failed to resume job {job_id}: {e}")
            return False
    
    def cancel_job(self, job_id: int) -> bool:
        """Cancel a job.
        
        Args:
            job_id: Job ID to cancel
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Cancel running task if exists
            if job_id in self._running_jobs:
                task = self._running_jobs[job_id]
                task.cancel()
                del self._running_jobs[job_id]
            
            # Update job status
            return self.db_manager.update_job_status(job_id, JobStatus.CANCELLED)
        except Exception as e:
            self.logger.error(f"Failed to cancel job {job_id}: {e}")
            return False
    
    async def _resume_job_processing(self, job_id: int) -> None:
        """Resume processing for a paused job.
        
        Args:
            job_id: Job ID to resume processing for
        """
        try:
            job_info = self.batch_processor.get_job_status(job_id)
            if not job_info:
                self.logger.error(f"Job {job_id} not found for resume")
                return
            
            # Get remaining CIKs to process
            target_ciks = job_info["target_ciks"]
            form_types = job_info["form_types"]
            
            # Get already processed CIKs from database
            processed_ciks = self._get_processed_ciks_for_job(job_id)
            remaining_ciks = [cik for cik in target_ciks if cik not in processed_ciks]
            
            if not remaining_ciks:
                self.logger.info(f"Job {job_id} already completed")
                self.db_manager.update_job_status(job_id, JobStatus.COMPLETED)
                return
            
            self.logger.info(
                f"Resuming job {job_id}: {len(remaining_ciks)} CIKs remaining"
            )
            
            # Resume processing with remaining CIKs
            config = job_info.get("config", {})
            processing_config = config.get("processing", {})
            
            bucket = processing_config.get("bucket", "edgar-filings")
            prefix = processing_config.get("prefix", "filings")
            
            # Use async processing for resumption
            result = await self.batch_processor.process_batch_async(
                remaining_ciks,
                bucket,
                prefix,
                form_types,
                f"{job_info['job_name']}_resume",
            )
            
            self.logger.info(f"Job {job_id} resumed and completed: {result}")
            
        except asyncio.CancelledError:
            self.logger.info(f"Job {job_id} processing was cancelled")
            raise
        except Exception as e:
            self.logger.error(f"Failed to resume job {job_id}: {e}")
            self.db_manager.update_job_status(
                job_id, JobStatus.FAILED, error_message=str(e)
            )
        finally:
            # Clean up running task
            if job_id in self._running_jobs:
                del self._running_jobs[job_id]
    
    def _get_processed_ciks_for_job(self, job_id: int) -> Set[str]:
        """Get set of CIKs that have been processed for a job.
        
        Args:
            job_id: Job ID to check
            
        Returns:
            Set of processed CIKs
        """
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.execute("""
                    SELECT DISTINCT cik FROM filings 
                    WHERE status IN ('completed', 'skipped')
                    AND created_at >= (
                        SELECT start_time FROM processing_jobs WHERE id = ?
                    )
                """, (job_id,))
                return {row[0] for row in cursor.fetchall()}
        except Exception as e:
            self.logger.error(f"Failed to get processed CIKs for job {job_id}: {e}")
            return set()
    
    async def run_job(
        self,
        job_id: int,
        bucket: str,
        prefix: str,
        use_async: bool = True,
    ) -> BatchProcessingResult:
        """Run a job by ID.
        
        Args:
            job_id: Job ID to run
            bucket: S3 bucket name
            prefix: S3 key prefix
            use_async: Whether to use async processing
            
        Returns:
            Batch processing result
        """
        try:
            job_info = self.batch_processor.get_job_status(job_id)
            if not job_info:
                raise ValueError(f"Job {job_id} not found")
            
            target_ciks = job_info["target_ciks"]
            form_types = job_info["form_types"]
            job_name = job_info["job_name"]
            
            self.logger.info(f"Starting job {job_id}: {job_name}")
            
            if use_async:
                result = await self.batch_processor.process_batch_async(
                    target_ciks, bucket, prefix, form_types, job_name
                )
            else:
                result = self.batch_processor.process_batch_sync(
                    target_ciks, bucket, prefix, form_types, job_name
                )
            
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to run job {job_id}: {e}")
            self.db_manager.update_job_status(
                job_id, JobStatus.FAILED, error_message=str(e)
            )
            raise
    
    def get_job_progress(self, job_id: int) -> Optional[Dict[str, Any]]:
        """Get detailed progress information for a job.
        
        Args:
            job_id: Job ID to get progress for
            
        Returns:
            Progress information or None if not found
        """
        try:
            job_info = self.batch_processor.get_job_status(job_id)
            if not job_info:
                return None
            
            # Get performance metrics
            metrics = self.db_manager.get_job_metrics(job_id)
            
            # Calculate additional progress metrics
            progress_info = {
                **job_info,
                "metrics": [
                    {
                        "name": m.metric_name,
                        "value": m.metric_value,
                        "unit": m.metric_unit,
                        "timestamp": m.timestamp.isoformat() if m.timestamp else None,
                    }
                    for m in metrics
                ],
            }
            
            # Add calculated fields
            if job_info["total_filings"] > 0:
                progress_info["progress_percentage"] = (
                    job_info["processed_filings"] / job_info["total_filings"]
                ) * 100
            else:
                progress_info["progress_percentage"] = 0.0
            
            # Calculate ETA if processing
            if (
                job_info["status"] == JobStatus.RUNNING.value
                and job_info["start_time"]
                and job_info["processed_filings"] > 0
            ):
                start_time = datetime.fromisoformat(job_info["start_time"])
                elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
                rate = job_info["processed_filings"] / elapsed  # filings per second
                
                remaining_filings = (
                    job_info["total_filings"] - job_info["processed_filings"]
                )
                eta_seconds = remaining_filings / rate if rate > 0 else None
                
                if eta_seconds:
                    eta_time = datetime.now(timezone.utc) + timedelta(
                        seconds=eta_seconds
                    )
                    progress_info["estimated_completion"] = eta_time.isoformat()
                    progress_info["eta_seconds"] = eta_seconds
            
            return progress_info
            
        except Exception as e:
            self.logger.error(f"Failed to get job progress {job_id}: {e}")
            return None
    
    def list_jobs(
        self,
        status_filter: Optional[List[JobStatus]] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """List jobs with optional status filtering.
        
        Args:
            status_filter: Optional list of job statuses to filter by
            limit: Maximum number of jobs to return
            
        Returns:
            List of job information
        """
        try:
            with self.db_manager.get_connection() as conn:
                query = """
                    SELECT id, job_name, job_type, status, target_ciks, form_types,
                           total_filings, processed_filings, failed_filings,
                           start_time, end_time, created_at
                    FROM processing_jobs
                """
                
                params = []
                if status_filter:
                    status_values = [status.value for status in status_filter]
                    placeholders = ",".join("?" * len(status_values))
                    query += f" WHERE status IN ({placeholders})"
                    params.extend(status_values)
                
                query += " ORDER BY created_at DESC LIMIT ?"
                params.append(limit)
                
                cursor = conn.execute(query, params)
                
                jobs = []
                for row in cursor.fetchall():
                    job_info = {
                        "job_id": row[0],
                        "job_name": row[1],
                        "job_type": row[2],
                        "status": row[3],
                        "target_ciks": json.loads(row[4]) if row[4] else [],
                        "form_types": json.loads(row[5]) if row[5] else [],
                        "total_filings": row[6],
                        "processed_filings": row[7],
                        "failed_filings": row[8],
                        "start_time": row[9],
                        "end_time": row[10],
                        "created_at": row[11],
                    }
                    
                    # Add progress percentage
                    if job_info["total_filings"] > 0:
                        job_info["progress_percentage"] = (
                            job_info["processed_filings"] / job_info["total_filings"]
                        ) * 100
                    else:
                        job_info["progress_percentage"] = 0.0
                    
                    jobs.append(job_info)
                
                return jobs
                
        except Exception as e:
            self.logger.error(f"Failed to list jobs: {e}")
            return []
    
    def cleanup_completed_jobs(self, older_than_days: int = 7) -> int:
        """Clean up old completed jobs.
        
        Args:
            older_than_days: Remove jobs completed more than this many days ago
            
        Returns:
            Number of jobs cleaned up
        """
        try:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=older_than_days)
            
            with self.db_manager.get_connection() as conn:
                cursor = conn.execute("""
                    DELETE FROM processing_jobs 
                    WHERE status = 'completed' AND end_time < ?
                """, (cutoff_date.isoformat(),))
                
                conn.commit()
                return cursor.rowcount
                
        except Exception as e:
            self.logger.error(f"Failed to cleanup completed jobs: {e}")
            return 0
    
    async def monitor_sla_compliance(
        self,
        sla_minutes: int = 30,
        max_filings: int = 50,
        check_interval_seconds: int = 60,
    ) -> None:
        """Monitor SLA compliance for processing jobs.
        
        Args:
            sla_minutes: SLA time limit in minutes
            max_filings: Maximum number of filings for SLA
            check_interval_seconds: How often to check SLA compliance
        """
        self.logger.info(
            f"Starting SLA monitoring: {sla_minutes} min for {max_filings} filings"
        )
        
        while not self._shutdown_event.is_set():
            try:
                # Check running jobs for SLA compliance
                running_jobs = self.list_jobs([JobStatus.RUNNING])
                
                for job in running_jobs:
                    if not job["start_time"]:
                        continue
                    
                    start_time = datetime.fromisoformat(job["start_time"])
                    elapsed_minutes = (
                        datetime.now(timezone.utc) - start_time
                    ).total_seconds() / 60
                    
                    # Check if job exceeds SLA
                    if (
                        elapsed_minutes > sla_minutes
                        and job["total_filings"] <= max_filings
                    ):
                        self.logger.warning(
                            f"Job {job['job_id']} ({job['job_name']}) "
                            f"exceeds SLA: {elapsed_minutes:.1f} min "
                            f"for {job['total_filings']} filings"
                        )
                        
                        # Record SLA violation metric
                        await asyncio.to_thread(
                            self.db_manager.record_metric,
                            PerformanceMetric(
                                job_id=job["job_id"],
                                metric_name="sla_violation",
                                metric_value=elapsed_minutes,
                                metric_unit="minutes",
                                metadata={
                                    "sla_limit": sla_minutes,
                                    "filing_count": job["total_filings"],
                                },
                            ),
                        )
                
                # Wait for next check
                await asyncio.sleep(check_interval_seconds)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"SLA monitoring error: {e}")
                await asyncio.sleep(check_interval_seconds)
        
        self.logger.info("SLA monitoring stopped")
    
    async def shutdown(self) -> None:
        """Gracefully shutdown the orchestrator."""
        self.logger.info("Shutting down job orchestrator...")
        
        # Signal shutdown
        self._shutdown_event.set()
        
        # Cancel all running jobs
        for job_id, task in list(self._running_jobs.items()):
            self.logger.info(f"Cancelling job {job_id}")
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        # Update job statuses to paused (for resumability)
        for job_id in self._running_jobs:
            self.db_manager.update_job_status(job_id, JobStatus.PAUSED)
        
        self._running_jobs.clear()
        self.logger.info("Job orchestrator shutdown complete")


class RetryMechanism:
    """Handles retry logic for failed processing operations."""
    
    def __init__(
        self,
        db_manager: DatabaseManager,
        max_retries: int = 3,
        retry_delay_seconds: int = 300,  # 5 minutes
        exponential_backoff: bool = True,
        logger: Optional[logging.Logger] = None,
    ):
        """Initialize retry mechanism.
        
        Args:
            db_manager: Database manager for tracking retries
            max_retries: Maximum number of retry attempts
            retry_delay_seconds: Base delay between retries
            exponential_backoff: Whether to use exponential backoff
            logger: Optional logger instance
        """
        self.db_manager = db_manager
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds
        self.exponential_backoff = exponential_backoff
        self.logger = logger or logging.getLogger(__name__)
    
    def get_failed_filings(self, hours_ago: int = 24) -> List[Dict[str, Any]]:
        """Get failed filings eligible for retry.
        
        Args:
            hours_ago: Look for failures within this time window
            
        Returns:
            List of failed filing information
        """
        try:
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours_ago)
            
            with self.db_manager.get_connection() as conn:
                cursor = conn.execute("""
                    SELECT id, cik, accession, form_type, retry_count, error_message
                    FROM filings 
                    WHERE status = 'failed' 
                    AND retry_count < ? 
                    AND updated_at >= ?
                    ORDER BY updated_at DESC
                """, (self.max_retries, cutoff_time.isoformat()))
                
                return [
                    {
                        "filing_id": row[0],
                        "cik": row[1],
                        "accession": row[2],
                        "form_type": row[3],
                        "retry_count": row[4],
                        "error_message": row[5],
                    }
                    for row in cursor.fetchall()
                ]
                
        except Exception as e:
            self.logger.error(f"Failed to get failed filings: {e}")
            return []
    
    async def retry_failed_filings(
        self,
        batch_processor: BatchFilingProcessor,
        bucket: str,
        prefix: str,
    ) -> Dict[str, int]:
        """Retry failed filings with exponential backoff.
        
        Args:
            batch_processor: Batch processor for retrying
            bucket: S3 bucket name
            prefix: S3 key prefix
            
        Returns:
            Dictionary with retry statistics
        """
        failed_filings = self.get_failed_filings()
        
        if not failed_filings:
            self.logger.info("No failed filings to retry")
            return {"total": 0, "retried": 0, "skipped": 0, "succeeded": 0}
        
        self.logger.info(f"Found {len(failed_filings)} failed filings to retry")
        
        stats = {"total": len(failed_filings), "retried": 0, "skipped": 0, "succeeded": 0}
        
        for filing_info in failed_filings:
            try:
                filing_id = filing_info["filing_id"]
                retry_count = filing_info["retry_count"]
                
                # Calculate delay with exponential backoff
                if self.exponential_backoff:
                    delay = self.retry_delay_seconds * (2 ** retry_count)
                else:
                    delay = self.retry_delay_seconds
                
                self.logger.info(
                    f"Retrying filing {filing_info['accession']} "
                    f"(attempt {retry_count + 1}, delay {delay}s)"
                )
                
                # Wait before retry
                await asyncio.sleep(delay)
                
                # Update retry count and status
                with self.db_manager.get_connection() as conn:
                    conn.execute("""
                        UPDATE filings 
                        SET retry_count = retry_count + 1, 
                            status = 'retrying',
                            updated_at = ?
                        WHERE id = ?
                    """, (datetime.now(timezone.utc).isoformat(), filing_id))
                    conn.commit()
                
                # Retry the filing (simplified - would need full filing info)
                # This is a placeholder for the actual retry logic
                stats["retried"] += 1
                
                # For now, just mark as retried
                # In a real implementation, you would re-process the filing
                
            except Exception as e:
                self.logger.error(
                    f"Failed to retry filing {filing_info['accession']}: {e}"
                )
                stats["skipped"] += 1
        
        return stats