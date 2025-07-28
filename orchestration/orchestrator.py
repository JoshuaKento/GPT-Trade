"""Main orchestration manager for ETL pipeline coordination."""

import json
import logging
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Callable, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from .scheduler import JobScheduler, JobConfig, JobStatus, Priority, ScheduleType
from .progress import ProgressTracker, JobProgress, TaskProgress, ProgressStatus
from .performance import PerformanceMonitor, SLAMetrics
from .resource_manager import ResourceManager, ResourceConfig


@dataclass
class TickerConfig:
    """Configuration for ticker processing."""
    
    ticker: str
    cik: Optional[str] = None
    form_types: List[str] = field(default_factory=lambda: ["10-K", "10-Q"])
    priority: Priority = Priority.NORMAL
    
    # Processing options
    fetch_latest_only: bool = True
    max_filings: int = 5
    date_range: Optional[tuple] = None  # (start_date, end_date)
    
    # Retry configuration
    max_retries: int = 3
    retry_delay: float = 5.0
    
    # Custom metadata
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class OrchestrationConfig:
    """Configuration for the orchestration system."""
    
    # Processing limits
    max_concurrent_tickers: int = 6
    max_concurrent_jobs: int = 3
    processing_timeout: float = 1800.0  # 30 minutes
    
    # Performance requirements
    sla_max_duration: float = 1800.0  # 30 minutes for 50 tickers
    sla_max_tickers: int = 50
    target_throughput: float = 2.78  # tickers per minute (50/18 minutes with buffer)
    
    # Resource management
    rate_limit_per_sec: float = 6.0  # SEC limit
    memory_limit_mb: int = 2048
    disk_space_limit_gb: int = 10
    
    # Data storage
    s3_bucket: Optional[str] = None
    s3_prefix: str = "edgar-data"
    local_storage_path: str = "./data"
    
    # Monitoring
    enable_performance_monitoring: bool = True
    enable_progress_tracking: bool = True
    enable_alerting: bool = True
    
    # Persistence
    progress_db_path: str = "orchestration_progress.db"
    results_db_path: str = "orchestration_results.db"
    
    # Logging
    log_level: str = "INFO"
    log_to_file: bool = True
    log_file_path: str = "orchestration.log"
    
    def validate(self) -> None:
        """Validate configuration parameters."""
        if self.max_concurrent_tickers <= 0:
            raise ValueError("max_concurrent_tickers must be positive")
            
        if self.sla_max_duration <= 0:
            raise ValueError("sla_max_duration must be positive")
            
        if self.rate_limit_per_sec <= 0 or self.rate_limit_per_sec > 10:
            raise ValueError("rate_limit_per_sec must be between 0 and 10")
            
        if self.target_throughput <= 0:
            raise ValueError("target_throughput must be positive")


class TickerProcessingError(Exception):
    """Exception raised during ticker processing."""
    pass


class OrchestrationManager:
    """Main orchestration manager for ETL pipeline coordination.
    
    This class coordinates the entire ETL process for processing multiple
    tickers while respecting SEC rate limits and meeting SLA requirements.
    """
    
    def __init__(self, config: OrchestrationConfig):
        """Initialize orchestration manager.
        
        Args:
            config: Orchestration configuration
        """
        self.config = config
        self.config.validate()
        
        # Core components
        self.job_scheduler = JobScheduler(
            max_workers=config.max_concurrent_jobs,
            enable_scheduling=True
        )
        
        self.progress_tracker = ProgressTracker(
            persistence_enabled=config.enable_progress_tracking,
            db_path=config.progress_db_path
        ) if config.enable_progress_tracking else None
        
        self.performance_monitor = PerformanceMonitor(
            sla_max_duration=config.sla_max_duration,
            target_throughput=config.target_throughput
        ) if config.enable_performance_monitoring else None
        
        self.resource_manager = ResourceManager(
            ResourceConfig(
                rate_limit_per_sec=config.rate_limit_per_sec,
                max_concurrent_requests=config.max_concurrent_tickers,
                memory_limit_mb=config.memory_limit_mb
            )
        )
        
        # State tracking
        self._active_jobs: Dict[str, str] = {}  # job_id -> ticker
        self._ticker_results: Dict[str, Dict[str, Any]] = {}
        self._orchestration_lock = threading.Lock()
        
        # Setup logging
        self._setup_logging()
        self.logger = logging.getLogger(__name__)
        
        # Callbacks
        self._completion_callbacks: List[Callable] = []
        self._error_callbacks: List[Callable] = []
        
        self.logger.info("Orchestration manager initialized")
    
    def process_tickers(self, 
                       tickers: List[Union[str, TickerConfig]], 
                       job_name: Optional[str] = None,
                       schedule_type: ScheduleType = ScheduleType.IMMEDIATE,
                       schedule_expression: Optional[str] = None) -> str:
        """Process a list of tickers through the ETL pipeline.
        
        Args:
            tickers: List of ticker symbols or TickerConfig objects
            job_name: Optional name for the orchestration job
            schedule_type: How to schedule the job
            schedule_expression: Schedule expression for periodic jobs
            
        Returns:
            Orchestration job ID
        """
        # Generate job ID and name
        orchestration_id = str(uuid.uuid4())
        if not job_name:
            job_name = f"ETL_Orchestration_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Normalize tickers to TickerConfig objects
        ticker_configs = []
        for ticker in tickers:
            if isinstance(ticker, str):
                ticker_configs.append(TickerConfig(ticker=ticker))
            elif isinstance(ticker, TickerConfig):
                ticker_configs.append(ticker)
            else:
                raise ValueError(f"Invalid ticker type: {type(ticker)}")
        
        # Validate SLA requirements
        if len(ticker_configs) > self.config.sla_max_tickers:
            self.logger.warning(
                f"Ticker count ({len(ticker_configs)}) exceeds SLA limit ({self.config.sla_max_tickers})"
            )
        
        # Create orchestration job
        job_config = JobConfig(
            job_id=orchestration_id,
            name=job_name,
            function=self._execute_orchestration,
            args=(ticker_configs, orchestration_id),
            schedule_type=schedule_type,
            schedule_expression=schedule_expression,
            priority=Priority.HIGH,
            timeout=self.config.processing_timeout
        )
        
        # Submit to scheduler
        self.job_scheduler.submit_job(job_config)
        
        self.logger.info(
            f"Submitted orchestration job {orchestration_id} with {len(ticker_configs)} tickers"
        )
        
        return orchestration_id
    
    def process_single_ticker(self, 
                             ticker: Union[str, TickerConfig],
                             priority: Priority = Priority.NORMAL) -> str:
        """Process a single ticker immediately.
        
        Args:
            ticker: Ticker symbol or configuration
            priority: Job priority
            
        Returns:
            Job ID
        """
        return self.process_tickers([ticker], schedule_type=ScheduleType.IMMEDIATE)
    
    def get_orchestration_status(self, orchestration_id: str) -> Dict[str, Any]:
        """Get status of an orchestration job.
        
        Args:
            orchestration_id: Orchestration job ID
            
        Returns:
            Status dictionary
        """
        # Get job status from scheduler
        job_status = self.job_scheduler.get_job_status(orchestration_id)
        job_result = self.job_scheduler.get_job_result(orchestration_id)
        
        # Get progress from tracker
        progress_summary = {}
        if self.progress_tracker:
            progress_summary = self.progress_tracker.get_progress_summary(orchestration_id)
        
        # Get performance metrics
        performance_metrics = {}
        if self.performance_monitor:
            performance_metrics = self.performance_monitor.get_job_metrics(orchestration_id)
        
        return {
            'orchestration_id': orchestration_id,
            'job_status': job_status.value if job_status else 'unknown',
            'progress': progress_summary,
            'performance': performance_metrics,
            'result': {
                'success': job_result.success if job_result else False,
                'error': str(job_result.error) if job_result and job_result.error else None,
                'duration': job_result.duration if job_result else None,
                'start_time': job_result.start_time.isoformat() if job_result and job_result.start_time else None,
                'end_time': job_result.end_time.isoformat() if job_result and job_result.end_time else None
            }
        }
    
    def cancel_orchestration(self, orchestration_id: str) -> bool:
        """Cancel an orchestration job.
        
        Args:
            orchestration_id: Orchestration job ID
            
        Returns:
            True if cancelled successfully
        """
        success = self.job_scheduler.cancel_job(orchestration_id)
        
        if success:
            # Cancel all related ticker processing jobs
            with self._orchestration_lock:
                jobs_to_cancel = [
                    job_id for job_id, ticker in self._active_jobs.items()
                    if job_id.startswith(orchestration_id)
                ]
            
            for job_id in jobs_to_cancel:
                self.job_scheduler.cancel_job(job_id)
            
            self.logger.info(f"Cancelled orchestration {orchestration_id} and {len(jobs_to_cancel)} related jobs")
        
        return success
    
    def get_system_health(self) -> Dict[str, Any]:
        """Get overall system health status.
        
        Returns:
            System health dictionary
        """
        scheduler_stats = self.job_scheduler.get_job_statistics()
        
        health = {
            'timestamp': datetime.now().isoformat(),
            'scheduler': {
                'queue_size': scheduler_stats['queue_size'],
                'running_jobs': scheduler_stats['running_jobs'],
                'worker_utilization': scheduler_stats['worker_utilization'],
                'total_jobs': scheduler_stats['total_jobs']
            },
            'resource_manager': {
                'current_rate_limit': self.resource_manager.current_rate_limit,
                'active_requests': self.resource_manager.get_active_request_count(),
                'memory_usage_mb': self.resource_manager.get_memory_usage(),
                'rate_limit_exceeded': self.resource_manager.is_rate_limited()
            }
        }
        
        if self.performance_monitor:
            health['performance'] = self.performance_monitor.get_system_metrics()
        
        # Calculate overall health score
        health_score = 100.0
        
        # Reduce score based on utilization
        if scheduler_stats['worker_utilization'] > 0.9:
            health_score -= 20
        elif scheduler_stats['worker_utilization'] > 0.7:
            health_score -= 10
        
        # Reduce score based on queue size
        if scheduler_stats['queue_size'] > 10:
            health_score -= 15
        elif scheduler_stats['queue_size'] > 5:
            health_score -= 5
        
        # Reduce score if rate limited
        if self.resource_manager.is_rate_limited():
            health_score -= 25
        
        health['overall_score'] = max(0, health_score)
        health['status'] = 'healthy' if health_score >= 80 else 'degraded' if health_score >= 50 else 'unhealthy'
        
        return health
    
    def add_completion_callback(self, callback: Callable[[str, Dict[str, Any]], None]) -> None:
        """Add callback for orchestration completion.
        
        Args:
            callback: Function to call with (orchestration_id, results)
        """
        self._completion_callbacks.append(callback)
    
    def add_error_callback(self, callback: Callable[[str, Exception], None]) -> None:
        """Add callback for orchestration errors.
        
        Args:
            callback: Function to call with (orchestration_id, error)
        """
        self._error_callbacks.append(callback)
    
    def shutdown(self, timeout: float = 30.0) -> None:
        """Shutdown the orchestration manager.
        
        Args:
            timeout: Maximum time to wait for shutdown
        """
        self.logger.info("Shutting down orchestration manager...")
        
        # Shutdown components
        self.job_scheduler.shutdown(wait=True, timeout=timeout)
        
        if self.progress_tracker:
            self.progress_tracker.shutdown()
        
        if self.performance_monitor:
            self.performance_monitor.shutdown()
        
        self.resource_manager.shutdown()
        
        self.logger.info("Orchestration manager shutdown complete")
    
    def _execute_orchestration(self, 
                              ticker_configs: List[TickerConfig], 
                              orchestration_id: str) -> Dict[str, Any]:
        """Execute the main orchestration logic.
        
        Args:
            ticker_configs: List of ticker configurations
            orchestration_id: Orchestration job ID
            
        Returns:
            Orchestration results
        """
        start_time = datetime.now()
        
        try:
            # Initialize progress tracking
            if self.progress_tracker:
                job_progress = self.progress_tracker.create_job_progress(
                    job_id=orchestration_id,
                    name=f"ETL Processing for {len(ticker_configs)} tickers",
                    total_tickers=len(ticker_configs),
                    metadata={
                        'tickers': [tc.ticker for tc in ticker_configs],
                        'start_time': start_time.isoformat()
                    }
                )
            
            # Initialize performance monitoring
            if self.performance_monitor:
                sla_metrics = SLAMetrics(
                    job_id=orchestration_id,
                    start_time=start_time,
                    target_duration=self.config.sla_max_duration,
                    target_throughput=self.config.target_throughput,
                    total_items=len(ticker_configs)
                )
                self.performance_monitor.start_monitoring(sla_metrics)
            
            # Process tickers in batches
            results = self._process_ticker_batch(ticker_configs, orchestration_id)
            
            # Calculate final metrics
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Check SLA compliance
            sla_compliant = duration <= self.config.sla_max_duration
            throughput = len(ticker_configs) / (duration / 60)  # tickers per minute
            
            orchestration_result = {
                'orchestration_id': orchestration_id,
                'success': True,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'duration_seconds': duration,
                'total_tickers': len(ticker_configs),
                'successful_tickers': sum(1 for r in results.values() if r.get('success', False)),
                'failed_tickers': sum(1 for r in results.values() if not r.get('success', False)),
                'throughput_per_minute': throughput,
                'sla_compliant': sla_compliant,
                'ticker_results': results
            }
            
            # Update progress
            if self.progress_tracker:
                self.progress_tracker.mark_task_completed(
                    job_id=orchestration_id,
                    task_id=f"{orchestration_id}_main",
                    success=True
                )
            
            # Notify completion callbacks
            for callback in self._completion_callbacks:
                try:
                    callback(orchestration_id, orchestration_result)
                except Exception as e:
                    self.logger.error(f"Error in completion callback: {e}")
            
            self.logger.info(
                f"Orchestration {orchestration_id} completed: "
                f"{orchestration_result['successful_tickers']}/{len(ticker_configs)} successful, "
                f"{duration:.2f}s, SLA: {sla_compliant}"
            )
            
            return orchestration_result
            
        except Exception as e:
            self.logger.error(f"Orchestration {orchestration_id} failed: {e}")
            
            # Update progress
            if self.progress_tracker:
                self.progress_tracker.mark_task_completed(
                    job_id=orchestration_id,
                    task_id=f"{orchestration_id}_main",
                    success=False,
                    error_message=str(e)
                )
            
            # Notify error callbacks
            for callback in self._error_callbacks:
                try:
                    callback(orchestration_id, e)
                except Exception as cb_error:
                    self.logger.error(f"Error in error callback: {cb_error}")
            
            raise TickerProcessingError(f"Orchestration failed: {e}") from e
    
    def _process_ticker_batch(self, 
                             ticker_configs: List[TickerConfig], 
                             orchestration_id: str) -> Dict[str, Dict[str, Any]]:
        """Process a batch of tickers concurrently.
        
        Args:
            ticker_configs: List of ticker configurations
            orchestration_id: Parent orchestration ID
            
        Returns:
            Dictionary of ticker results
        """
        results = {}
        
        # Create individual ticker processing jobs
        ticker_jobs = []
        for ticker_config in ticker_configs:
            job_id = f"{orchestration_id}_{ticker_config.ticker}_{uuid.uuid4().hex[:8]}"
            
            # Create task progress
            if self.progress_tracker:
                self.progress_tracker.create_task_progress(
                    job_id=orchestration_id,
                    task_id=job_id,
                    name=f"Process {ticker_config.ticker}",
                    total_steps=5,  # Fetch, Parse, Validate, Store, Complete
                    items_total=len(ticker_config.form_types) * ticker_config.max_filings
                )
            
            # Create job configuration
            job_config = JobConfig(
                job_id=job_id,
                name=f"Process_{ticker_config.ticker}",
                function=self._process_single_ticker,
                args=(ticker_config, orchestration_id, job_id),
                priority=ticker_config.priority,
                max_retries=ticker_config.max_retries,
                retry_delay=ticker_config.retry_delay,
                timeout=300.0  # 5 minutes per ticker
            )
            
            ticker_jobs.append((job_id, ticker_config.ticker, job_config))
        
        # Submit all jobs
        for job_id, ticker, job_config in ticker_jobs:
            self.job_scheduler.submit_job(job_config)
            
            with self._orchestration_lock:
                self._active_jobs[job_id] = ticker
        
        # Wait for all jobs to complete
        for job_id, ticker, _ in ticker_jobs:
            try:
                result = self.job_scheduler.wait_for_job(job_id, timeout=300.0)
                
                if result.success:
                    results[ticker] = {
                        'success': True,
                        'result': result.result,
                        'duration': result.duration
                    }
                else:
                    results[ticker] = {
                        'success': False,
                        'error': str(result.error) if result.error else 'Unknown error',
                        'duration': result.duration
                    }
                
            except Exception as e:
                results[ticker] = {
                    'success': False,
                    'error': str(e),
                    'duration': None
                }
            
            finally:
                # Clean up job tracking
                with self._orchestration_lock:
                    self._active_jobs.pop(job_id, None)
        
        return results
    
    def _process_single_ticker(self, 
                              ticker_config: TickerConfig, 
                              orchestration_id: str,
                              task_id: str) -> Dict[str, Any]:
        """Process a single ticker through the ETL pipeline.
        
        Args:
            ticker_config: Ticker configuration
            orchestration_id: Parent orchestration ID
            task_id: Task identifier
            
        Returns:
            Processing results
        """
        from edgar import EdgarClient, FilingProcessor, S3Manager
        from edgar.config_manager import get_config_manager
        
        ticker = ticker_config.ticker
        
        try:
            # Update progress
            if self.progress_tracker:
                self.progress_tracker.update_task_progress(
                    job_id=orchestration_id,
                    task_id=task_id,
                    status=ProgressStatus.IN_PROGRESS,
                    current_step=1,
                    current_operation=f"Initializing {ticker} processing"
                )
            
            # Get EDGAR configuration
            config_manager = get_config_manager()
            edgar_config = config_manager.get_config()
            
            # Initialize components with resource management
            with self.resource_manager.acquire_request_slot():
                # Initialize EDGAR client
                client = EdgarClient(edgar_config)
                
                # Initialize S3 manager if configured
                s3_manager = None
                if self.config.s3_bucket:
                    s3_manager = S3Manager(bucket_name=self.config.s3_bucket)
                
                # Initialize filing processor
                processor = FilingProcessor(
                    client=client,
                    s3_manager=s3_manager
                )
                
                # Update progress
                if self.progress_tracker:
                    self.progress_tracker.update_task_progress(
                        job_id=orchestration_id,
                        task_id=task_id,
                        current_step=2,
                        current_operation=f"Fetching filings for {ticker}"
                    )
                
                # Process each form type
                all_results = []
                
                for i, form_type in enumerate(ticker_config.form_types):
                    try:
                        # Apply rate limiting
                        self.resource_manager.wait_for_rate_limit()
                        
                        # Fetch filings
                        filings = client.get_company_filings(
                            ticker=ticker,
                            form_type=form_type,
                            count=ticker_config.max_filings
                        )
                        
                        # Process each filing
                        for j, filing in enumerate(filings):
                            # Update progress
                            if self.progress_tracker:
                                items_processed = i * ticker_config.max_filings + j + 1
                                self.progress_tracker.update_task_progress(
                                    job_id=orchestration_id,
                                    task_id=task_id,
                                    current_step=3,
                                    current_operation=f"Processing {filing.accession_number}",
                                    items_processed=items_processed
                                )
                            
                            # Apply rate limiting for each filing request
                            self.resource_manager.wait_for_rate_limit()
                            
                            # Process filing
                            result = processor.process_filing(filing)
                            all_results.append(result)
                        
                    except Exception as e:
                        self.logger.warning(f"Error processing {form_type} for {ticker}: {e}")
                        # Continue with other form types
                
                # Update progress - storing results
                if self.progress_tracker:
                    self.progress_tracker.update_task_progress(
                        job_id=orchestration_id,
                        task_id=task_id,
                        current_step=4,
                        current_operation=f"Storing results for {ticker}"
                    )
                
                # Store results in local storage if configured
                if self.config.local_storage_path:
                    storage_path = Path(self.config.local_storage_path) / "processed" / ticker
                    storage_path.mkdir(parents=True, exist_ok=True)
                    
                    results_file = storage_path / f"{ticker}_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                    with open(results_file, 'w') as f:
                        json.dump(all_results, f, indent=2, default=str)
                
                # Final progress update
                if self.progress_tracker:
                    self.progress_tracker.update_task_progress(
                        job_id=orchestration_id,
                        task_id=task_id,
                        current_step=5,
                        current_operation=f"Completed {ticker} processing",
                        progress_percentage=100.0
                    )
                    
                    self.progress_tracker.mark_task_completed(
                        job_id=orchestration_id,
                        task_id=task_id,
                        success=True
                    )
                
                return {
                    'ticker': ticker,
                    'filings_processed': len(all_results),
                    'form_types': ticker_config.form_types,
                    'results': all_results,
                    'metadata': ticker_config.metadata
                }
                
        except Exception as e:
            # Mark task as failed
            if self.progress_tracker:
                self.progress_tracker.mark_task_completed(
                    job_id=orchestration_id,
                    task_id=task_id,
                    success=False,
                    error_message=str(e)
                )
            
            self.logger.error(f"Failed to process ticker {ticker}: {e}")
            raise TickerProcessingError(f"Ticker processing failed: {e}") from e
    
    def _setup_logging(self) -> None:
        """Setup logging configuration."""
        log_level = getattr(logging, self.config.log_level.upper())
        
        # Configure root logger
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Add file handler if configured
        if self.config.log_to_file:
            file_handler = logging.FileHandler(self.config.log_file_path)
            file_handler.setLevel(log_level)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            file_handler.setFormatter(formatter)
            
            # Add to all existing loggers
            for logger_name in logging.Logger.manager.loggerDict:
                logger = logging.getLogger(logger_name)
                logger.addHandler(file_handler)