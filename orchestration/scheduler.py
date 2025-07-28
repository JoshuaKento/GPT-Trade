"""Job scheduling and task queue management for ETL orchestration."""

import asyncio
import logging
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from queue import Queue, PriorityQueue
from threading import Event, Lock, Thread
from typing import Any, Callable, Dict, List, Optional, Set, Union

import schedule


class JobStatus(Enum):
    """Job execution status."""
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"


class ScheduleType(Enum):
    """Types of job schedules."""
    IMMEDIATE = "immediate"
    CRON = "cron"
    INTERVAL = "interval"
    ONCE = "once"


class Priority(Enum):
    """Job priority levels."""
    LOW = 3
    NORMAL = 2
    HIGH = 1
    CRITICAL = 0


@dataclass
class JobConfig:
    """Configuration for a single job."""
    
    job_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    description: str = ""
    
    # Execution parameters
    function: Optional[Callable] = None
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    
    # Scheduling
    schedule_type: ScheduleType = ScheduleType.IMMEDIATE
    schedule_expression: Optional[str] = None  # Cron expression or interval
    
    # Priority and constraints
    priority: Priority = Priority.NORMAL
    max_retries: int = 3
    retry_delay: float = 5.0  # seconds
    timeout: Optional[float] = None  # seconds
    
    # Dependencies
    depends_on: List[str] = field(default_factory=list)
    
    # Metadata
    tags: Set[str] = field(default_factory=set)
    created_at: datetime = field(default_factory=datetime.now)
    
    def __post_init__(self):
        if not self.name:
            self.name = f"job_{self.job_id[:8]}"


@dataclass 
class JobResult:
    """Result of job execution."""
    
    job_id: str
    status: JobStatus
    result: Any = None
    error: Optional[Exception] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration: Optional[float] = None
    retry_count: int = 0
    
    @property
    def success(self) -> bool:
        return self.status == JobStatus.COMPLETED
    
    @property
    def failed(self) -> bool:
        return self.status == JobStatus.FAILED


class TaskQueue:
    """Thread-safe priority queue for job tasks."""
    
    def __init__(self):
        self._queue = PriorityQueue()
        self._items = {}  # job_id -> JobConfig
        self._lock = Lock()
        
    def put(self, job_config: JobConfig) -> None:
        """Add job to queue."""
        priority_value = job_config.priority.value
        # Use creation time as tiebreaker for same priority
        timestamp = job_config.created_at.timestamp()
        
        with self._lock:
            self._queue.put((priority_value, timestamp, job_config.job_id))
            self._items[job_config.job_id] = job_config
            
    def get(self, timeout: Optional[float] = None) -> Optional[JobConfig]:
        """Get next job from queue."""
        try:
            _, _, job_id = self._queue.get(timeout=timeout)
            with self._lock:
                return self._items.pop(job_id, None)
        except:
            return None
            
    def remove(self, job_id: str) -> bool:
        """Remove job from queue if present."""
        with self._lock:
            if job_id in self._items:
                del self._items[job_id]
                return True
            return False
            
    def size(self) -> int:
        """Get queue size."""
        return self._queue.qsize()
        
    def empty(self) -> bool:
        """Check if queue is empty."""
        return self._queue.empty()


class JobScheduler:
    """Advanced job scheduler with task queue management."""
    
    def __init__(self, 
                 max_workers: int = 6,
                 queue_timeout: float = 1.0,
                 enable_scheduling: bool = True):
        """Initialize job scheduler.
        
        Args:
            max_workers: Maximum number of concurrent workers
            queue_timeout: Timeout for queue operations
            enable_scheduling: Whether to enable periodic scheduling
        """
        self.max_workers = max_workers
        self.queue_timeout = queue_timeout
        self.enable_scheduling = enable_scheduling
        
        # Core components
        self.task_queue = TaskQueue()
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        
        # State tracking
        self._jobs: Dict[str, JobConfig] = {}
        self._results: Dict[str, JobResult] = {}
        self._running_jobs: Dict[str, Any] = {}  # job_id -> Future
        self._dependencies: Dict[str, Set[str]] = {}  # job_id -> dependent job_ids
        
        # Control
        self._shutdown_event = Event()
        self._worker_thread: Optional[Thread] = None
        self._scheduler_thread: Optional[Thread] = None
        self._lock = Lock()
        
        # Logging
        self.logger = logging.getLogger(__name__)
        
        # Start background threads
        self._start_worker()
        if enable_scheduling:
            self._start_scheduler()
    
    def submit_job(self, job_config: JobConfig) -> str:
        """Submit a job for execution.
        
        Args:
            job_config: Job configuration
            
        Returns:
            Job ID
        """
        with self._lock:
            self._jobs[job_config.job_id] = job_config
            
            # Initialize result
            self._results[job_config.job_id] = JobResult(
                job_id=job_config.job_id,
                status=JobStatus.PENDING
            )
            
            # Track dependencies
            for dep_id in job_config.depends_on:
                if dep_id not in self._dependencies:
                    self._dependencies[dep_id] = set()
                self._dependencies[dep_id].add(job_config.job_id)
        
        # Queue immediately or schedule
        if job_config.schedule_type == ScheduleType.IMMEDIATE:
            self._queue_job_if_ready(job_config.job_id)
        else:
            self._schedule_job(job_config)
            
        self.logger.info(f"Submitted job {job_config.job_id}: {job_config.name}")
        return job_config.job_id
    
    def cancel_job(self, job_id: str) -> bool:
        """Cancel a job.
        
        Args:
            job_id: Job identifier
            
        Returns:
            True if cancelled successfully
        """
        with self._lock:
            # Remove from queue if pending
            if self.task_queue.remove(job_id):
                self._results[job_id].status = JobStatus.CANCELLED
                return True
                
            # Cancel running job
            if job_id in self._running_jobs:
                future = self._running_jobs[job_id]
                if future.cancel():
                    self._results[job_id].status = JobStatus.CANCELLED
                    del self._running_jobs[job_id]
                    return True
                    
        return False
    
    def get_job_status(self, job_id: str) -> Optional[JobStatus]:
        """Get current status of a job.
        
        Args:
            job_id: Job identifier
            
        Returns:
            Job status or None if job not found
        """
        result = self._results.get(job_id)
        return result.status if result else None
    
    def get_job_result(self, job_id: str) -> Optional[JobResult]:
        """Get result of a job.
        
        Args:
            job_id: Job identifier
            
        Returns:
            Job result or None if job not found
        """
        return self._results.get(job_id)
    
    def wait_for_job(self, job_id: str, timeout: Optional[float] = None) -> JobResult:
        """Wait for a job to complete.
        
        Args:
            job_id: Job identifier
            timeout: Maximum time to wait in seconds
            
        Returns:
            Job result
            
        Raises:
            TimeoutError: If timeout exceeded
            ValueError: If job not found
        """
        if job_id not in self._results:
            raise ValueError(f"Job {job_id} not found")
            
        start_time = time.time()
        while True:
            result = self._results[job_id]
            if result.status in {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED}:
                return result
                
            if timeout and (time.time() - start_time) > timeout:
                raise TimeoutError(f"Timeout waiting for job {job_id}")
                
            time.sleep(0.1)
    
    def get_queue_size(self) -> int:
        """Get current queue size."""
        return self.task_queue.size()
    
    def get_running_jobs_count(self) -> int:
        """Get number of currently running jobs."""
        with self._lock:
            return len(self._running_jobs)
    
    def get_job_statistics(self) -> Dict[str, Any]:
        """Get job execution statistics."""
        with self._lock:
            total_jobs = len(self._results)
            status_counts = {}
            
            for result in self._results.values():
                status = result.status.value
                status_counts[status] = status_counts.get(status, 0) + 1
            
            return {
                "total_jobs": total_jobs,
                "queue_size": self.get_queue_size(),
                "running_jobs": len(self._running_jobs),
                "status_counts": status_counts,
                "worker_utilization": len(self._running_jobs) / self.max_workers
            }
    
    def shutdown(self, wait: bool = True, timeout: float = 30.0) -> None:
        """Shutdown the scheduler.
        
        Args:
            wait: Whether to wait for running jobs to complete
            timeout: Maximum time to wait for shutdown
        """
        self.logger.info("Shutting down job scheduler...")
        
        # Signal shutdown
        self._shutdown_event.set()
        
        # Wait for threads to finish
        if self._worker_thread and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=timeout)
            
        if self._scheduler_thread and self._scheduler_thread.is_alive():
            self._scheduler_thread.join(timeout=timeout)
        
        # Shutdown executor
        self.executor.shutdown(wait=wait, timeout=timeout)
        
        self.logger.info("Job scheduler shutdown complete")
    
    def _start_worker(self) -> None:
        """Start the worker thread."""
        self._worker_thread = Thread(target=self._worker_loop, daemon=True)
        self._worker_thread.start()
        
    def _start_scheduler(self) -> None:
        """Start the scheduler thread."""
        self._scheduler_thread = Thread(target=self._scheduler_loop, daemon=True)
        self._scheduler_thread.start()
    
    def _worker_loop(self) -> None:
        """Main worker loop that processes queued jobs."""
        while not self._shutdown_event.is_set():
            try:
                # Get next job from queue
                job_config = self.task_queue.get(timeout=self.queue_timeout)
                if job_config is None:
                    continue
                
                # Submit to executor
                future = self.executor.submit(self._execute_job, job_config)
                
                with self._lock:
                    self._running_jobs[job_config.job_id] = future
                    self._results[job_config.job_id].status = JobStatus.RUNNING
                    self._results[job_config.job_id].start_time = datetime.now()
                
                # Handle completion asynchronously
                future.add_done_callback(
                    lambda f, job_id=job_config.job_id: self._handle_job_completion(job_id, f)
                )
                
            except Exception as e:
                self.logger.error(f"Error in worker loop: {e}")
    
    def _scheduler_loop(self) -> None:
        """Main scheduler loop for periodic jobs."""
        while not self._shutdown_event.is_set():
            try:
                schedule.run_pending()
                time.sleep(1)
            except Exception as e:
                self.logger.error(f"Error in scheduler loop: {e}")
    
    def _execute_job(self, job_config: JobConfig) -> Any:
        """Execute a single job.
        
        Args:
            job_config: Job configuration
            
        Returns:
            Job result
        """
        if job_config.function is None:
            raise ValueError(f"No function specified for job {job_config.job_id}")
        
        # Apply timeout if specified
        if job_config.timeout:
            import signal
            
            def timeout_handler(signum, frame):
                raise TimeoutError(f"Job {job_config.job_id} timed out after {job_config.timeout}s")
            
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(int(job_config.timeout))
        
        try:
            result = job_config.function(*job_config.args, **job_config.kwargs)
            return result
        finally:
            if job_config.timeout:
                signal.alarm(0)  # Cancel timeout
    
    def _handle_job_completion(self, job_id: str, future) -> None:
        """Handle job completion."""
        with self._lock:
            if job_id in self._running_jobs:
                del self._running_jobs[job_id]
            
            result = self._results[job_id]
            result.end_time = datetime.now()
            
            if result.start_time:
                result.duration = (result.end_time - result.start_time).total_seconds()
        
        try:
            job_result = future.result()
            result.result = job_result
            result.status = JobStatus.COMPLETED
            
            self.logger.info(f"Job {job_id} completed successfully")
            
            # Queue dependent jobs
            self._queue_dependent_jobs(job_id)
            
        except Exception as e:
            result.error = e
            
            # Check for retry
            job_config = self._jobs[job_id]
            if result.retry_count < job_config.max_retries:
                result.retry_count += 1
                result.status = JobStatus.RETRYING
                
                self.logger.warning(f"Job {job_id} failed, retrying ({result.retry_count}/{job_config.max_retries})")
                
                # Schedule retry with delay
                def retry_job():
                    time.sleep(job_config.retry_delay)
                    self._queue_job_if_ready(job_id)
                
                Thread(target=retry_job, daemon=True).start()
            else:
                result.status = JobStatus.FAILED
                self.logger.error(f"Job {job_id} failed after {job_config.max_retries} retries: {e}")
    
    def _queue_job_if_ready(self, job_id: str) -> None:
        """Queue job if all dependencies are satisfied."""
        job_config = self._jobs[job_id]
        
        # Check dependencies
        for dep_id in job_config.depends_on:
            dep_result = self._results.get(dep_id)
            if not dep_result or dep_result.status != JobStatus.COMPLETED:
                return  # Dependencies not satisfied
        
        # Queue the job
        with self._lock:
            self._results[job_id].status = JobStatus.QUEUED
        
        self.task_queue.put(job_config)
    
    def _queue_dependent_jobs(self, completed_job_id: str) -> None:
        """Queue jobs that depend on the completed job."""
        dependent_jobs = self._dependencies.get(completed_job_id, set())
        
        for dep_job_id in dependent_jobs:
            if self._results[dep_job_id].status == JobStatus.PENDING:
                self._queue_job_if_ready(dep_job_id)
    
    def _schedule_job(self, job_config: JobConfig) -> None:
        """Schedule a job based on its schedule configuration."""
        if not self.enable_scheduling:
            return
            
        if job_config.schedule_type == ScheduleType.INTERVAL:
            # Parse interval (e.g., "5m", "1h", "30s")
            interval_str = job_config.schedule_expression
            if interval_str:
                interval_seconds = self._parse_interval(interval_str)
                schedule.every(interval_seconds).seconds.do(
                    lambda: self._queue_job_if_ready(job_config.job_id)
                )
        
        elif job_config.schedule_type == ScheduleType.CRON:
            # For cron expressions, would need additional library like python-crontab
            # For now, log that it's not implemented
            self.logger.warning(f"CRON scheduling not implemented for job {job_config.job_id}")
        
        elif job_config.schedule_type == ScheduleType.ONCE:
            # Schedule for one-time execution at specified time
            # This would require parsing the schedule_expression as a datetime
            self.logger.warning(f"ONCE scheduling not implemented for job {job_config.job_id}")
    
    def _parse_interval(self, interval_str: str) -> int:
        """Parse interval string to seconds."""
        if interval_str.endswith('s'):
            return int(interval_str[:-1])
        elif interval_str.endswith('m'):
            return int(interval_str[:-1]) * 60
        elif interval_str.endswith('h'):
            return int(interval_str[:-1]) * 3600
        elif interval_str.endswith('d'):
            return int(interval_str[:-1]) * 86400
        else:
            return int(interval_str)  # Assume seconds