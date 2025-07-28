"""Progress tracking and status reporting for ETL orchestration."""

import json
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from threading import Lock, Thread, Event
from typing import Any, Dict, List, Optional, Set, Callable
import sqlite3
from pathlib import Path


class ProgressStatus(Enum):
    """Progress status for jobs and tasks."""
    NOT_STARTED = "not_started"
    INITIALIZING = "initializing"
    IN_PROGRESS = "in_progress"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class TaskProgress:
    """Progress information for individual tasks."""
    
    task_id: str
    name: str
    status: ProgressStatus = ProgressStatus.NOT_STARTED
    current_step: int = 0
    total_steps: int = 0
    progress_percentage: float = 0.0
    
    # Timing
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration: Optional[float] = None
    estimated_remaining: Optional[float] = None
    
    # Details
    current_operation: str = ""
    items_processed: int = 0
    items_total: int = 0
    bytes_processed: int = 0
    bytes_total: int = 0
    
    # Error handling
    error_message: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    
    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def update_progress(self, 
                       current: Optional[int] = None,
                       total: Optional[int] = None,
                       operation: Optional[str] = None,
                       **kwargs) -> None:
        """Update task progress."""
        if current is not None:
            self.current_step = current
        if total is not None:
            self.total_steps = total
        if operation is not None:
            self.current_operation = operation
            
        # Update from kwargs
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
        
        # Calculate percentage
        if self.total_steps > 0:
            self.progress_percentage = min(100.0, (self.current_step / self.total_steps) * 100)
        
        # Calculate items percentage if available
        if self.items_total > 0 and self.items_processed <= self.items_total:
            items_percentage = (self.items_processed / self.items_total) * 100
            if self.progress_percentage == 0.0:
                self.progress_percentage = items_percentage
        
        # Estimate remaining time
        if self.start_time and self.progress_percentage > 0:
            elapsed = (datetime.now() - self.start_time).total_seconds()
            if self.progress_percentage < 100:
                self.estimated_remaining = (elapsed / self.progress_percentage) * (100 - self.progress_percentage)


@dataclass
class JobProgress:
    """Progress information for entire jobs."""
    
    job_id: str
    name: str
    status: ProgressStatus = ProgressStatus.NOT_STARTED
    
    # Task tracking
    tasks: Dict[str, TaskProgress] = field(default_factory=dict)
    current_task_id: Optional[str] = None
    
    # Overall progress
    overall_percentage: float = 0.0
    
    # Timing
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration: Optional[float] = None
    estimated_remaining: Optional[float] = None
    
    # Statistics
    total_tickers: int = 0
    completed_tickers: int = 0
    failed_tickers: int = 0
    
    # Performance metrics
    throughput: float = 0.0  # items per second
    avg_processing_time: float = 0.0  # seconds per item
    
    # Resource usage
    peak_memory_mb: float = 0.0
    cpu_usage_percent: float = 0.0
    
    # Error tracking
    errors: List[Dict[str, Any]] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def add_task(self, task: TaskProgress) -> None:
        """Add a task to the job."""
        self.tasks[task.task_id] = task
        
    def update_overall_progress(self) -> None:
        """Update overall job progress based on task progress."""
        if not self.tasks:
            return
            
        total_weight = len(self.tasks)
        weighted_progress = sum(task.progress_percentage for task in self.tasks.values())
        self.overall_percentage = weighted_progress / total_weight
        
        # Update status based on task statuses
        task_statuses = [task.status for task in self.tasks.values()]
        
        if all(status == ProgressStatus.COMPLETED for status in task_statuses):
            self.status = ProgressStatus.COMPLETED
            if not self.end_time:
                self.end_time = datetime.now()
        elif any(status == ProgressStatus.FAILED for status in task_statuses):
            self.status = ProgressStatus.FAILED
        elif any(status == ProgressStatus.IN_PROGRESS for status in task_statuses):
            self.status = ProgressStatus.IN_PROGRESS
        
        # Calculate timing
        if self.start_time:
            if self.end_time:
                self.duration = (self.end_time - self.start_time).total_seconds()
            elif self.overall_percentage > 0:
                elapsed = (datetime.now() - self.start_time).total_seconds()
                self.estimated_remaining = (elapsed / self.overall_percentage) * (100 - self.overall_percentage)
        
        # Calculate throughput
        if self.duration and self.completed_tickers:
            self.throughput = self.completed_tickers / self.duration
            self.avg_processing_time = self.duration / self.completed_tickers


class ProgressPersistence:
    """Persistence layer for progress data."""
    
    def __init__(self, db_path: str = "progress.db"):
        self.db_path = Path(db_path)
        self.logger = logging.getLogger(__name__)
        self._init_database()
        
    def _init_database(self) -> None:
        """Initialize SQLite database for progress storage."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS job_progress (
                    job_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    overall_percentage REAL,
                    total_tickers INTEGER,
                    completed_tickers INTEGER,
                    failed_tickers INTEGER,
                    metadata TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS task_progress (
                    task_id TEXT PRIMARY KEY,
                    job_id TEXT,
                    name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    current_step INTEGER,
                    total_steps INTEGER,
                    progress_percentage REAL,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    current_operation TEXT,
                    items_processed INTEGER,
                    items_total INTEGER,
                    error_message TEXT,
                    metadata TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (job_id) REFERENCES job_progress (job_id)
                )
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_job_status ON job_progress (status)
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_task_job ON task_progress (job_id)
            """)
    
    def save_job_progress(self, progress: JobProgress) -> None:
        """Save job progress to database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO job_progress 
                    (job_id, name, status, start_time, end_time, overall_percentage,
                     total_tickers, completed_tickers, failed_tickers, metadata, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, (
                    progress.job_id,
                    progress.name,
                    progress.status.value,
                    progress.start_time,
                    progress.end_time,
                    progress.overall_percentage,
                    progress.total_tickers,
                    progress.completed_tickers,
                    progress.failed_tickers,
                    json.dumps(progress.metadata)
                ))
        except Exception as e:
            self.logger.error(f"Failed to save job progress: {e}")
    
    def save_task_progress(self, progress: TaskProgress, job_id: str) -> None:
        """Save task progress to database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO task_progress 
                    (task_id, job_id, name, status, current_step, total_steps,
                     progress_percentage, start_time, end_time, current_operation,
                     items_processed, items_total, error_message, metadata, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, (
                    progress.task_id,
                    job_id,
                    progress.name,
                    progress.status.value,
                    progress.current_step,
                    progress.total_steps,
                    progress.progress_percentage,
                    progress.start_time,
                    progress.end_time,
                    progress.current_operation,
                    progress.items_processed,
                    progress.items_total,
                    progress.error_message,
                    json.dumps(progress.metadata)
                ))
        except Exception as e:
            self.logger.error(f"Failed to save task progress: {e}")
    
    def load_job_progress(self, job_id: str) -> Optional[JobProgress]:
        """Load job progress from database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("""
                    SELECT * FROM job_progress WHERE job_id = ?
                """, (job_id,))
                
                row = cursor.fetchone()
                if not row:
                    return None
                
                progress = JobProgress(
                    job_id=row['job_id'],
                    name=row['name'],
                    status=ProgressStatus(row['status']),
                    overall_percentage=row['overall_percentage'] or 0.0,
                    start_time=datetime.fromisoformat(row['start_time']) if row['start_time'] else None,
                    end_time=datetime.fromisoformat(row['end_time']) if row['end_time'] else None,
                    total_tickers=row['total_tickers'] or 0,
                    completed_tickers=row['completed_tickers'] or 0,
                    failed_tickers=row['failed_tickers'] or 0,
                    metadata=json.loads(row['metadata']) if row['metadata'] else {}
                )
                
                # Load associated tasks
                task_cursor = conn.execute("""
                    SELECT * FROM task_progress WHERE job_id = ?
                """, (job_id,))
                
                for task_row in task_cursor.fetchall():
                    task = TaskProgress(
                        task_id=task_row['task_id'],
                        name=task_row['name'],
                        status=ProgressStatus(task_row['status']),
                        current_step=task_row['current_step'] or 0,
                        total_steps=task_row['total_steps'] or 0,
                        progress_percentage=task_row['progress_percentage'] or 0.0,
                        start_time=datetime.fromisoformat(task_row['start_time']) if task_row['start_time'] else None,
                        end_time=datetime.fromisoformat(task_row['end_time']) if task_row['end_time'] else None,
                        current_operation=task_row['current_operation'] or "",
                        items_processed=task_row['items_processed'] or 0,
                        items_total=task_row['items_total'] or 0,
                        error_message=task_row['error_message'],
                        metadata=json.loads(task_row['metadata']) if task_row['metadata'] else {}
                    )
                    progress.add_task(task)
                
                return progress
                
        except Exception as e:
            self.logger.error(f"Failed to load job progress: {e}")
            return None
    
    def get_recent_jobs(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent job progress summaries."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("""
                    SELECT job_id, name, status, overall_percentage, 
                           start_time, end_time, total_tickers, completed_tickers,
                           failed_tickers, updated_at
                    FROM job_progress 
                    ORDER BY updated_at DESC 
                    LIMIT ?
                """, (limit,))
                
                return [dict(row) for row in cursor.fetchall()]
                
        except Exception as e:
            self.logger.error(f"Failed to get recent jobs: {e}")
            return []


class ProgressTracker:
    """Real-time progress tracking and reporting system."""
    
    def __init__(self, 
                 persistence_enabled: bool = True,
                 db_path: str = "progress.db",
                 update_interval: float = 1.0):
        """Initialize progress tracker.
        
        Args:
            persistence_enabled: Whether to persist progress to database
            db_path: Path to SQLite database file
            update_interval: How often to update/persist progress (seconds)
        """
        self.persistence_enabled = persistence_enabled
        self.update_interval = update_interval
        
        # Progress storage
        self._job_progress: Dict[str, JobProgress] = {}
        self._subscribers: Dict[str, List[Callable]] = defaultdict(list)
        
        # Performance tracking
        self._progress_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        
        # Thread safety
        self._lock = Lock()
        
        # Background update thread
        self._update_event = Event()
        self._update_thread: Optional[Thread] = None
        
        # Persistence
        self.persistence = ProgressPersistence(db_path) if persistence_enabled else None
        
        # Logging
        self.logger = logging.getLogger(__name__)
        
        # Start background thread
        self._start_update_thread()
    
    def create_job_progress(self, 
                           job_id: str, 
                           name: str, 
                           total_tickers: int = 0,
                           metadata: Optional[Dict[str, Any]] = None) -> JobProgress:
        """Create a new job progress tracker.
        
        Args:
            job_id: Unique job identifier
            name: Human-readable job name
            total_tickers: Total number of tickers to process
            metadata: Additional metadata
            
        Returns:
            JobProgress instance
        """
        with self._lock:
            progress = JobProgress(
                job_id=job_id,
                name=name,
                total_tickers=total_tickers,
                metadata=metadata or {},
                start_time=datetime.now()
            )
            
            self._job_progress[job_id] = progress
            
        self.logger.info(f"Created job progress tracker: {job_id} ({name})")
        self._notify_subscribers(job_id, "job_created", progress)
        
        return progress
    
    def create_task_progress(self, 
                            job_id: str,
                            task_id: str,
                            name: str,
                            total_steps: int = 0,
                            items_total: int = 0) -> TaskProgress:
        """Create a new task progress tracker.
        
        Args:
            job_id: Parent job identifier
            task_id: Unique task identifier
            name: Human-readable task name
            total_steps: Total number of steps
            items_total: Total number of items to process
            
        Returns:
            TaskProgress instance
        """
        with self._lock:
            if job_id not in self._job_progress:
                raise ValueError(f"Job {job_id} not found")
            
            task = TaskProgress(
                task_id=task_id,
                name=name,
                total_steps=total_steps,
                items_total=items_total,
                start_time=datetime.now()
            )
            
            self._job_progress[job_id].add_task(task)
            
        self.logger.debug(f"Created task progress tracker: {task_id} for job {job_id}")
        self._notify_subscribers(job_id, "task_created", task)
        
        return task
    
    def update_task_progress(self, 
                            job_id: str, 
                            task_id: str, 
                            **kwargs) -> None:
        """Update task progress.
        
        Args:
            job_id: Parent job identifier
            task_id: Task identifier
            **kwargs: Progress updates
        """
        with self._lock:
            if job_id not in self._job_progress:
                return
                
            job_progress = self._job_progress[job_id]
            if task_id not in job_progress.tasks:
                return
            
            task = job_progress.tasks[task_id]
            
            # Update task
            task.update_progress(**kwargs)
            
            # Update job overall progress
            job_progress.update_overall_progress()
            
            # Store progress history
            self._progress_history[task_id].append({
                'timestamp': datetime.now(),
                'percentage': task.progress_percentage,
                'items_processed': task.items_processed
            })
        
        self._notify_subscribers(job_id, "task_updated", task)
    
    def mark_task_completed(self, 
                           job_id: str, 
                           task_id: str, 
                           success: bool = True,
                           error_message: Optional[str] = None) -> None:
        """Mark a task as completed.
        
        Args:
            job_id: Parent job identifier
            task_id: Task identifier
            success: Whether task completed successfully
            error_message: Error message if failed
        """
        with self._lock:
            if job_id not in self._job_progress:
                return
                
            job_progress = self._job_progress[job_id]
            if task_id not in job_progress.tasks:
                return
            
            task = job_progress.tasks[task_id]
            task.end_time = datetime.now()
            
            if task.start_time:
                task.duration = (task.end_time - task.start_time).total_seconds()
            
            if success:
                task.status = ProgressStatus.COMPLETED
                task.progress_percentage = 100.0
                job_progress.completed_tickers += 1
            else:
                task.status = ProgressStatus.FAILED
                task.error_message = error_message
                job_progress.failed_tickers += 1
                
                if error_message:
                    job_progress.errors.append({
                        'task_id': task_id,
                        'timestamp': datetime.now().isoformat(),
                        'error': error_message
                    })
            
            # Update job overall progress
            job_progress.update_overall_progress()
        
        self._notify_subscribers(job_id, "task_completed", task)
    
    def get_job_progress(self, job_id: str) -> Optional[JobProgress]:
        """Get current job progress.
        
        Args:
            job_id: Job identifier
            
        Returns:
            JobProgress instance or None
        """
        with self._lock:
            return self._job_progress.get(job_id)
    
    def get_task_progress(self, job_id: str, task_id: str) -> Optional[TaskProgress]:
        """Get current task progress.
        
        Args:
            job_id: Parent job identifier
            task_id: Task identifier
            
        Returns:
            TaskProgress instance or None
        """
        with self._lock:
            job_progress = self._job_progress.get(job_id)
            if job_progress:
                return job_progress.tasks.get(task_id)
            return None
    
    def get_progress_summary(self, job_id: str) -> Dict[str, Any]:
        """Get progress summary for a job.
        
        Args:
            job_id: Job identifier
            
        Returns:
            Progress summary dictionary
        """
        with self._lock:
            job_progress = self._job_progress.get(job_id)
            if not job_progress:
                return {}
            
            return {
                'job_id': job_id,
                'name': job_progress.name,
                'status': job_progress.status.value,
                'overall_percentage': job_progress.overall_percentage,
                'start_time': job_progress.start_time.isoformat() if job_progress.start_time else None,
                'end_time': job_progress.end_time.isoformat() if job_progress.end_time else None,
                'duration': job_progress.duration,
                'estimated_remaining': job_progress.estimated_remaining,
                'total_tickers': job_progress.total_tickers,
                'completed_tickers': job_progress.completed_tickers,
                'failed_tickers': job_progress.failed_tickers,
                'throughput': job_progress.throughput,
                'tasks': {
                    task_id: {
                        'name': task.name,
                        'status': task.status.value,
                        'percentage': task.progress_percentage,
                        'current_operation': task.current_operation,
                        'items_processed': task.items_processed,
                        'items_total': task.items_total
                    }
                    for task_id, task in job_progress.tasks.items()
                },
                'errors': job_progress.errors[-10:],  # Last 10 errors
                'warnings': job_progress.warnings[-10:]  # Last 10 warnings
            }
    
    def subscribe_to_updates(self, job_id: str, callback: Callable) -> None:
        """Subscribe to progress updates for a job.
        
        Args:
            job_id: Job identifier
            callback: Function to call on updates
        """
        with self._lock:
            self._subscribers[job_id].append(callback)
    
    def unsubscribe_from_updates(self, job_id: str, callback: Callable) -> None:
        """Unsubscribe from progress updates.
        
        Args:
            job_id: Job identifier
            callback: Function to remove from updates
        """
        with self._lock:
            if job_id in self._subscribers:
                self._subscribers[job_id].remove(callback)
    
    def cleanup_job(self, job_id: str, keep_history: bool = True) -> None:
        """Clean up job progress data.
        
        Args:
            job_id: Job identifier
            keep_history: Whether to keep progress in database
        """
        with self._lock:
            if job_id in self._job_progress:
                if self.persistence_enabled and keep_history:
                    # Save final state to database
                    self.persistence.save_job_progress(self._job_progress[job_id])
                    for task in self._job_progress[job_id].tasks.values():
                        self.persistence.save_task_progress(task, job_id)
                
                # Remove from memory
                del self._job_progress[job_id]
                
            # Clean up subscribers
            if job_id in self._subscribers:
                del self._subscribers[job_id]
                
            # Clean up history
            tasks_to_remove = [tid for tid in self._progress_history.keys() if tid.startswith(job_id)]
            for tid in tasks_to_remove:
                del self._progress_history[tid]
        
        self.logger.info(f"Cleaned up progress data for job {job_id}")
    
    def shutdown(self) -> None:
        """Shutdown the progress tracker."""
        self.logger.info("Shutting down progress tracker...")
        
        # Stop update thread
        self._update_event.set()
        if self._update_thread and self._update_thread.is_alive():
            self._update_thread.join(timeout=5.0)
        
        # Save all active progress to database
        if self.persistence_enabled:
            with self._lock:
                for job_progress in self._job_progress.values():
                    self.persistence.save_job_progress(job_progress)
                    for task in job_progress.tasks.values():
                        self.persistence.save_task_progress(task, job_progress.job_id)
        
        self.logger.info("Progress tracker shutdown complete")
    
    def _start_update_thread(self) -> None:
        """Start the background update thread."""
        self._update_thread = Thread(target=self._update_loop, daemon=True)
        self._update_thread.start()
    
    def _update_loop(self) -> None:
        """Background loop for periodic updates."""
        while not self._update_event.is_set():
            try:
                # Persist progress to database
                if self.persistence_enabled:
                    with self._lock:
                        for job_progress in self._job_progress.values():
                            self.persistence.save_job_progress(job_progress)
                            for task in job_progress.tasks.values():
                                self.persistence.save_task_progress(task, job_progress.job_id)
                
                # Wait for next update
                self._update_event.wait(timeout=self.update_interval)
                
            except Exception as e:
                self.logger.error(f"Error in progress update loop: {e}")
    
    def _notify_subscribers(self, job_id: str, event_type: str, data: Any) -> None:
        """Notify subscribers of progress updates."""
        subscribers = self._subscribers.get(job_id, [])
        for callback in subscribers:
            try:
                callback(job_id, event_type, data)
            except Exception as e:
                self.logger.error(f"Error notifying subscriber: {e}")