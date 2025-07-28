"""
ETL Pipeline orchestration system for GPT Trader platform.

This module provides job scheduling, task queue management, and workflow
orchestration for processing multiple tickers efficiently.
"""

import logging
import time
import threading
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
import psutil

from .models import ProcessingJob, ProcessingStatus, Company
from .database import get_session, session_scope
from .config import GPTTraderConfig, TickerConfig
from .filing_processor_db import BatchFilingProcessor
from .monitoring import PerformanceMonitor, SLAMonitor

logger = logging.getLogger(__name__)


@dataclass
class ETLJobResult:
    """Result of an ETL job execution."""
    job_id: int
    job_uuid: str
    success: bool
    duration_seconds: float
    items_processed: int
    items_failed: int
    error_message: Optional[str] = None
    performance_metrics: Dict[str, Any] = None


@dataclass
class ETLTask:
    """Individual ETL task definition."""
    task_id: str
    task_type: str
    ticker_config: TickerConfig
    parameters: Dict[str, Any]
    priority: int = 1
    max_retries: int = 3
    retry_count: int = 0
    created_at: datetime = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow()


class TaskQueue:
    """Thread-safe task queue with priority support."""
    
    def __init__(self, max_size: int = 1000):
        self._queue = Queue(maxsize=max_size)
        self._priority_queue = Queue(maxsize=max_size)
        self._lock = threading.Lock()
        self._total_tasks = 0
        self._completed_tasks = 0
        
    def put_task(self, task: ETLTask, high_priority: bool = False) -> None:
        """Add task to queue."""
        with self._lock:
            if high_priority or task.priority > 5:
                self._priority_queue.put(task)
            else:
                self._queue.put(task)
            self._total_tasks += 1
    
    def get_task(self, timeout: float = 1.0) -> Optional[ETLTask]:
        """Get next task from queue."""
        # Try priority queue first
        try:
            return self._priority_queue.get(timeout=timeout)
        except Empty:
            pass
        
        # Then regular queue
        try:
            return self._queue.get(timeout=timeout)
        except Empty:
            return None
    
    def task_done(self) -> None:
        """Mark task as completed."""
        with self._lock:
            self._completed_tasks += 1
            self._queue.task_done()
    
    def get_queue_stats(self) -> Dict[str, int]:
        """Get queue statistics."""
        return {
            'total_tasks': self._total_tasks,
            'completed_tasks': self._completed_tasks,
            'pending_tasks': self._total_tasks - self._completed_tasks,
            'priority_queue_size': self._priority_queue.qsize(),
            'regular_queue_size': self._queue.qsize()
        }
    
    def is_empty(self) -> bool:
        """Check if queue is empty."""
        return self._queue.empty() and self._priority_queue.empty()


class ETLWorker:
    """ETL worker thread for processing tasks."""
    
    def __init__(self, worker_id: str, config: GPTTraderConfig, 
                 task_queue: TaskQueue, result_callback: Callable[[ETLJobResult], None]):
        self.worker_id = worker_id
        self.config = config
        self.task_queue = task_queue
        self.result_callback = result_callback
        self.is_running = False
        self.current_task: Optional[ETLTask] = None
        self.processor = BatchFilingProcessor(config)
        
    def start(self) -> None:
        """Start worker thread."""
        self.is_running = True
        self.thread = threading.Thread(target=self._run, name=f"ETLWorker-{self.worker_id}")
        self.thread.start()
        logger.info(f"ETL worker {self.worker_id} started")
    
    def stop(self) -> None:
        """Stop worker thread."""
        self.is_running = False
        if hasattr(self, 'thread'):
            self.thread.join(timeout=30)
        logger.info(f"ETL worker {self.worker_id} stopped")
    
    def _run(self) -> None:
        """Main worker loop."""
        while self.is_running:
            try:
                # Get next task
                task = self.task_queue.get_task(timeout=1.0)
                if task is None:
                    continue
                
                self.current_task = task
                logger.info(f"Worker {self.worker_id} processing task {task.task_id}")
                
                # Process task
                result = self._process_task(task)
                
                # Report result
                if self.result_callback:
                    self.result_callback(result)
                
                # Mark task done
                self.task_queue.task_done()
                self.current_task = None
                
            except Exception as e:
                logger.error(f"Worker {self.worker_id} error: {e}")
                if self.current_task:
                    self.task_queue.task_done()
                    self.current_task = None
    
    def _process_task(self, task: ETLTask) -> ETLJobResult:
        """Process a single ETL task."""
        start_time = datetime.utcnow()
        
        try:
            # Create job record
            with session_scope() as session:
                job = ProcessingJob.create_job(
                    session=session,
                    job_type=task.task_type,
                    job_name=f"{task.task_type}_{task.ticker_config.ticker}_{start_time.strftime('%Y%m%d_%H%M%S')}",
                    ticker_list=[task.ticker_config.ticker],
                    parameters=task.parameters
                )
                job.start_job(session, total_items=1)
            
            # Process ticker
            new_filings, updated_filings = self.processor.processor.fetch_and_store_filings(
                task.ticker_config,
                max_filings=task.parameters.get('max_filings')
            )
            
            # Complete job
            with session_scope() as session:
                job = session.merge(job)
                job.complete_job(session)
            
            duration = (datetime.utcnow() - start_time).total_seconds()
            
            return ETLJobResult(
                job_id=job.id,
                job_uuid=job.job_uuid,
                success=True,
                duration_seconds=duration,
                items_processed=new_filings + updated_filings,
                items_failed=0,
                performance_metrics={
                    'new_filings': new_filings,
                    'updated_filings': updated_filings,
                    'ticker': task.ticker_config.ticker
                }
            )
            
        except Exception as e:
            duration = (datetime.utcnow() - start_time).total_seconds()
            logger.error(f"Task {task.task_id} failed: {e}")
            
            # Mark job as failed
            try:
                with session_scope() as session:
                    if 'job' in locals():
                        job = session.merge(job)
                        job.fail_job(session, str(e))
            except Exception as job_error:
                logger.error(f"Failed to update job status: {job_error}")
            
            return ETLJobResult(
                job_id=getattr(job, 'id', 0) if 'job' in locals() else 0,
                job_uuid=getattr(job, 'job_uuid', '') if 'job' in locals() else '',
                success=False,
                duration_seconds=duration,
                items_processed=0,
                items_failed=1,
                error_message=str(e)
            )


class ETLPipeline:
    """Main ETL pipeline orchestrator."""
    
    def __init__(self, config: GPTTraderConfig):
        self.config = config
        self.task_queue = TaskQueue(max_size=1000)
        self.workers: List[ETLWorker] = []
        self.is_running = False
        self.performance_monitor = PerformanceMonitor()
        self.sla_monitor = SLAMonitor(config)
        self.results: List[ETLJobResult] = []
        self._results_lock = threading.Lock()
        
    def start(self) -> None:
        """Start the ETL pipeline."""
        if self.is_running:
            logger.warning("ETL pipeline is already running")
            return
        
        self.is_running = True
        
        # Start workers
        for i in range(self.config.etl.max_concurrent_jobs):
            worker = ETLWorker(
                worker_id=f"worker-{i+1}",
                config=self.config,
                task_queue=self.task_queue,
                result_callback=self._handle_result
            )
            self.workers.append(worker)
            worker.start()
        
        # Start monitoring
        self.performance_monitor.start()
        self.sla_monitor.start()
        
        logger.info(f"ETL pipeline started with {len(self.workers)} workers")
    
    def stop(self) -> None:
        """Stop the ETL pipeline."""
        if not self.is_running:
            return
        
        self.is_running = False
        
        # Stop workers
        for worker in self.workers:
            worker.stop()
        
        # Stop monitoring
        self.performance_monitor.stop()
        self.sla_monitor.stop()
        
        logger.info("ETL pipeline stopped")
    
    def submit_ticker_task(self, ticker_config: TickerConfig, 
                          task_type: str = "fetch_filings",
                          parameters: Dict[str, Any] = None,
                          high_priority: bool = False) -> str:
        """Submit a ticker processing task."""
        task_id = f"{task_type}_{ticker_config.ticker}_{int(time.time())}"
        
        task = ETLTask(
            task_id=task_id,
            task_type=task_type,
            ticker_config=ticker_config,
            parameters=parameters or {},
            priority=ticker_config.priority
        )
        
        self.task_queue.put_task(task, high_priority)
        logger.info(f"Submitted task {task_id} for ticker {ticker_config.ticker}")
        
        return task_id
    
    def submit_batch_job(self, ticker_configs: List[TickerConfig],
                        job_name: str = None) -> str:
        """Submit a batch job for multiple tickers."""
        job_name = job_name or f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        task_ids = []
        for ticker_config in ticker_configs:
            task_id = self.submit_ticker_task(
                ticker_config=ticker_config,
                task_type="batch_fetch",
                parameters={'job_name': job_name}
            )
            task_ids.append(task_id)
        
        logger.info(f"Submitted batch job {job_name} with {len(task_ids)} tasks")
        return job_name
    
    def run_daily_etl(self) -> ETLJobResult:
        """Run daily ETL for all active tickers."""
        start_time = datetime.utcnow()
        logger.info("Starting daily ETL process")
        
        if not self.is_running:
            self.start()
        
        # Submit all active tickers
        active_tickers = self.config.get_active_tickers()
        job_name = self.submit_batch_job(active_tickers, "daily_etl")
        
        # Wait for completion or timeout
        timeout = timedelta(minutes=self.config.etl.sla_processing_time_minutes)
        
        while datetime.utcnow() - start_time < timeout:
            if self.task_queue.is_empty() and all(w.current_task is None for w in self.workers):
                break
            time.sleep(5)
        
        # Collect results
        duration = (datetime.utcnow() - start_time).total_seconds()
        
        with self._results_lock:
            recent_results = [r for r in self.results if r.job_uuid.startswith('daily_etl')]
            total_processed = sum(r.items_processed for r in recent_results)
            total_failed = sum(r.items_failed for r in recent_results)
            success_rate = (total_processed / (total_processed + total_failed)) if (total_processed + total_failed) > 0 else 0
        
        # Check SLA compliance
        sla_met = (duration <= self.config.etl.sla_processing_time_minutes * 60 and 
                  success_rate >= self.config.etl.sla_success_rate_threshold)
        
        result = ETLJobResult(
            job_id=0,
            job_uuid=job_name,
            success=sla_met,
            duration_seconds=duration,
            items_processed=total_processed,
            items_failed=total_failed,
            performance_metrics={
                'sla_met': sla_met,
                'success_rate': success_rate,
                'tickers_processed': len(active_tickers),
                'timeout_reached': duration >= timeout.total_seconds()
            }
        )
        
        logger.info(f"Daily ETL completed: {result}")
        return result
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get current pipeline status and metrics."""
        queue_stats = self.task_queue.get_queue_stats()
        
        worker_status = []
        for worker in self.workers:
            status = {
                'worker_id': worker.worker_id,
                'is_running': worker.is_running,
                'current_task': worker.current_task.task_id if worker.current_task else None
            }
            worker_status.append(status)
        
        with self._results_lock:
            recent_results = [r for r in self.results if 
                            datetime.utcnow() - datetime.fromtimestamp(0) < timedelta(hours=24)]
            success_rate = (sum(1 for r in recent_results if r.success) / len(recent_results)) if recent_results else 0
        
        return {
            'is_running': self.is_running,
            'queue_stats': queue_stats,
            'worker_status': worker_status,
            'recent_success_rate': success_rate,
            'system_metrics': {
                'cpu_usage': psutil.cpu_percent(),
                'memory_usage': psutil.virtual_memory().percent,
                'disk_usage': psutil.disk_usage('/').percent
            },
            'performance_metrics': self.performance_monitor.get_metrics(),
            'sla_status': self.sla_monitor.get_status()
        }
    
    def _handle_result(self, result: ETLJobResult) -> None:
        """Handle job result."""
        with self._results_lock:
            self.results.append(result)
            # Keep only recent results to prevent memory growth
            if len(self.results) > 1000:
                self.results = self.results[-500:]
        
        # Update monitoring
        self.performance_monitor.record_job(result)
        self.sla_monitor.record_job(result)
        
        if result.success:
            logger.info(f"Job {result.job_uuid} completed successfully in {result.duration_seconds:.2f}s")
        else:
            logger.warning(f"Job {result.job_uuid} failed: {result.error_message}")


class ETLScheduler:
    """Scheduler for automated ETL jobs."""
    
    def __init__(self, config: GPTTraderConfig):
        self.config = config
        self.pipeline = ETLPipeline(config)
        self.is_running = False
        self.scheduler_thread: Optional[threading.Thread] = None
        
    def start(self) -> None:
        """Start the scheduler."""
        if self.is_running:
            return
        
        self.is_running = True
        self.pipeline.start()
        
        self.scheduler_thread = threading.Thread(target=self._scheduler_loop, name="ETLScheduler")
        self.scheduler_thread.start()
        
        logger.info("ETL scheduler started")
    
    def stop(self) -> None:
        """Stop the scheduler."""
        self.is_running = False
        
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=30)
        
        self.pipeline.stop()
        logger.info("ETL scheduler stopped")
    
    def _scheduler_loop(self) -> None:
        """Main scheduler loop."""
        last_daily_run = None
        
        while self.is_running:
            try:
                now = datetime.utcnow()
                
                # Check if it's time for daily ETL (run once per day at configured time)
                if self._should_run_daily_etl(now, last_daily_run):
                    logger.info("Starting scheduled daily ETL")
                    self.pipeline.run_daily_etl()
                    last_daily_run = now
                
                # Sleep before next check
                time.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"Scheduler error: {e}")
                time.sleep(60)
    
    def _should_run_daily_etl(self, now: datetime, last_run: Optional[datetime]) -> bool:
        """Check if daily ETL should run."""
        # Default to 6 AM UTC if not configured
        target_hour = getattr(self.config, 'daily_etl_hour', 6)
        
        # Check if we're in the target hour and haven't run today
        if now.hour == target_hour:
            if last_run is None or last_run.date() < now.date():
                return True
        
        return False