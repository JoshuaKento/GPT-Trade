"""Database models for EDGAR filing processing with comprehensive metadata persistence."""

import enum
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Set

import sqlite3
from contextlib import contextmanager
import json
import logging
import threading
from pathlib import Path


class ProcessingStatus(enum.Enum):
    """Status values for processing operations."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRYING = "retrying"


class JobStatus(enum.Enum):
    """Status values for batch jobs."""
    CREATED = "created"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


@dataclass
class Company:
    """Company entity for tracking processed companies."""
    cik: str
    ticker: Optional[str] = None
    name: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc)
        if self.updated_at is None:
            self.updated_at = datetime.now(timezone.utc)


@dataclass
class Filing:
    """Filing entity for tracking processed filings."""
    id: Optional[int] = None
    cik: str = ""
    accession: str = ""
    form_type: str = ""
    filing_date: Optional[str] = None
    report_date: Optional[str] = None
    primary_document: Optional[str] = None
    status: ProcessingStatus = ProcessingStatus.PENDING
    documents_processed: int = 0
    error_message: Optional[str] = None
    s3_prefix: Optional[str] = None
    processing_duration: Optional[float] = None
    retry_count: int = 0
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc)
        if self.updated_at is None:
            self.updated_at = datetime.now(timezone.utc)


@dataclass
class Document:
    """Document entity for tracking individual filing documents."""
    id: Optional[int] = None
    filing_id: int = 0
    document_name: str = ""
    document_type: Optional[str] = None
    s3_key: Optional[str] = None
    file_size: Optional[int] = None
    status: ProcessingStatus = ProcessingStatus.PENDING
    error_message: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc)
        if self.updated_at is None:
            self.updated_at = datetime.now(timezone.utc)


@dataclass
class ProcessingJob:
    """Job entity for tracking batch processing operations."""
    id: Optional[int] = None
    job_name: str = ""
    job_type: str = "batch_filing"
    status: JobStatus = JobStatus.CREATED
    target_ciks: List[str] = field(default_factory=list)
    form_types: List[str] = field(default_factory=list)
    config: Dict[str, Any] = field(default_factory=dict)
    total_filings: int = 0
    processed_filings: int = 0
    failed_filings: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    error_message: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc)
        if self.updated_at is None:
            self.updated_at = datetime.now(timezone.utc)
    
    @property
    def duration(self) -> Optional[float]:
        """Calculate job duration in seconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    @property
    def progress_percentage(self) -> float:
        """Calculate job progress percentage."""
        if self.total_filings == 0:
            return 0.0
        return (self.processed_filings / self.total_filings) * 100.0


@dataclass
class PerformanceMetric:
    """Performance metrics for monitoring and SLA tracking."""
    id: Optional[int] = None
    job_id: Optional[int] = None
    metric_name: str = ""
    metric_value: float = 0.0
    metric_unit: str = ""
    timestamp: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now(timezone.utc)


class DatabaseManager:
    """Thread-safe database manager for EDGAR filing metadata."""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, *args, **kwargs):
        """Singleton pattern for database manager."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, db_path: str = "edgar_filings.db", logger: Optional[logging.Logger] = None):
        """Initialize database manager.
        
        Args:
            db_path: Path to SQLite database file
            logger: Optional logger instance
        """
        if hasattr(self, '_initialized'):
            return
            
        self.db_path = Path(db_path)
        self.logger = logger or logging.getLogger(__name__)
        self._connection_pool = threading.local()
        self._initialized = True
        
        # Create database tables
        self._create_tables()
    
    @contextmanager
    def get_connection(self):
        """Get thread-local database connection."""
        if not hasattr(self._connection_pool, 'connection'):
            self._connection_pool.connection = sqlite3.connect(
                str(self.db_path),
                check_same_thread=False,
                timeout=30.0
            )
            self._connection_pool.connection.row_factory = sqlite3.Row
            # Enable WAL mode for better concurrency
            self._connection_pool.connection.execute("PRAGMA journal_mode=WAL")
            self._connection_pool.connection.execute("PRAGMA synchronous=NORMAL")
            self._connection_pool.connection.execute("PRAGMA cache_size=-64000")  # 64MB cache
            
        try:
            yield self._connection_pool.connection
        except Exception as e:
            self._connection_pool.connection.rollback()
            self.logger.error(f"Database operation failed: {e}")
            raise
    
    def _create_tables(self):
        """Create database tables if they don't exist."""
        with self.get_connection() as conn:
            # Companies table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS companies (
                    cik TEXT PRIMARY KEY,
                    ticker TEXT,
                    name TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Filings table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS filings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cik TEXT NOT NULL,
                    accession TEXT NOT NULL UNIQUE,
                    form_type TEXT NOT NULL,
                    filing_date TEXT,
                    report_date TEXT,
                    primary_document TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    documents_processed INTEGER DEFAULT 0,
                    error_message TEXT,
                    s3_prefix TEXT,
                    processing_duration REAL,
                    retry_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (cik) REFERENCES companies (cik)
                )
            """)
            
            # Documents table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS documents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filing_id INTEGER NOT NULL,
                    document_name TEXT NOT NULL,
                    document_type TEXT,
                    s3_key TEXT,
                    file_size INTEGER,
                    status TEXT NOT NULL DEFAULT 'pending',
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (filing_id) REFERENCES filings (id),
                    UNIQUE(filing_id, document_name)
                )
            """)
            
            # Processing jobs table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS processing_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_name TEXT NOT NULL,
                    job_type TEXT NOT NULL DEFAULT 'batch_filing',
                    status TEXT NOT NULL DEFAULT 'created',
                    target_ciks TEXT,  -- JSON array
                    form_types TEXT,   -- JSON array
                    config TEXT,       -- JSON object
                    total_filings INTEGER DEFAULT 0,
                    processed_filings INTEGER DEFAULT 0,
                    failed_filings INTEGER DEFAULT 0,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Performance metrics table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS performance_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id INTEGER,
                    metric_name TEXT NOT NULL,
                    metric_value REAL NOT NULL,
                    metric_unit TEXT NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    metadata TEXT,  -- JSON object
                    FOREIGN KEY (job_id) REFERENCES processing_jobs (id)
                )
            """)
            
            # Create indexes for performance
            conn.execute("CREATE INDEX IF NOT EXISTS idx_filings_cik ON filings (cik)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_filings_status ON filings (status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_filings_accession ON filings (accession)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_documents_filing_id ON documents (filing_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_documents_status ON documents (status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON processing_jobs (status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_metrics_job_id ON performance_metrics (job_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_metrics_name ON performance_metrics (metric_name)")
            
            conn.commit()
    
    def create_company(self, company: Company) -> bool:
        """Create or update a company record.
        
        Args:
            company: Company object to create/update
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.get_connection() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO companies 
                    (cik, ticker, name, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    company.cik,
                    company.ticker,
                    company.name,
                    company.created_at.isoformat() if company.created_at else None,
                    datetime.now(timezone.utc).isoformat()
                ))
                conn.commit()
                return True
        except Exception as e:
            self.logger.error(f"Failed to create company {company.cik}: {e}")
            return False
    
    def create_filing(self, filing: Filing) -> Optional[int]:
        """Create a filing record.
        
        Args:
            filing: Filing object to create
            
        Returns:
            Filing ID if successful, None otherwise
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.execute("""
                    INSERT OR REPLACE INTO filings 
                    (cik, accession, form_type, filing_date, report_date, primary_document,
                     status, documents_processed, error_message, s3_prefix, processing_duration,
                     retry_count, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    filing.cik,
                    filing.accession,
                    filing.form_type,
                    filing.filing_date,
                    filing.report_date,
                    filing.primary_document,
                    filing.status.value,
                    filing.documents_processed,
                    filing.error_message,
                    filing.s3_prefix,
                    filing.processing_duration,
                    filing.retry_count,
                    filing.created_at.isoformat() if filing.created_at else None,
                    datetime.now(timezone.utc).isoformat()
                ))
                conn.commit()
                return cursor.lastrowid
        except Exception as e:
            self.logger.error(f"Failed to create filing {filing.accession}: {e}")
            return None
    
    def update_filing_status(self, filing_id: int, status: ProcessingStatus, 
                           error_message: Optional[str] = None,
                           documents_processed: Optional[int] = None,
                           processing_duration: Optional[float] = None) -> bool:
        """Update filing processing status.
        
        Args:
            filing_id: Filing ID to update
            status: New processing status
            error_message: Optional error message
            documents_processed: Number of documents processed
            processing_duration: Processing duration in seconds
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.get_connection() as conn:
                updates = ["status = ?", "updated_at = ?"]
                values = [status.value, datetime.now(timezone.utc).isoformat()]
                
                if error_message is not None:
                    updates.append("error_message = ?")
                    values.append(error_message)
                
                if documents_processed is not None:
                    updates.append("documents_processed = ?")
                    values.append(documents_processed)
                
                if processing_duration is not None:
                    updates.append("processing_duration = ?")
                    values.append(processing_duration)
                
                values.append(filing_id)
                
                conn.execute(f"""
                    UPDATE filings SET {', '.join(updates)}
                    WHERE id = ?
                """, values)
                conn.commit()
                return True
        except Exception as e:
            self.logger.error(f"Failed to update filing {filing_id}: {e}")
            return False
    
    def get_processed_accessions(self, cik: str) -> Set[str]:
        """Get set of processed accession numbers for a CIK.
        
        Args:
            cik: Company CIK
            
        Returns:
            Set of processed accession numbers
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.execute("""
                    SELECT accession FROM filings 
                    WHERE cik = ? AND status IN ('completed', 'skipped')
                """, (cik,))
                return {row[0] for row in cursor.fetchall()}
        except Exception as e:
            self.logger.error(f"Failed to get processed accessions for {cik}: {e}")
            return set()
    
    def create_job(self, job: ProcessingJob) -> Optional[int]:
        """Create a processing job record.
        
        Args:
            job: ProcessingJob object to create
            
        Returns:
            Job ID if successful, None otherwise
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.execute("""
                    INSERT INTO processing_jobs 
                    (job_name, job_type, status, target_ciks, form_types, config,
                     total_filings, processed_filings, failed_filings, start_time,
                     end_time, error_message, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    job.job_name,
                    job.job_type,
                    job.status.value,
                    json.dumps(job.target_ciks),
                    json.dumps(job.form_types),
                    json.dumps(job.config),
                    job.total_filings,
                    job.processed_filings,
                    job.failed_filings,
                    job.start_time.isoformat() if job.start_time else None,
                    job.end_time.isoformat() if job.end_time else None,
                    job.error_message,
                    job.created_at.isoformat() if job.created_at else None,
                    datetime.now(timezone.utc).isoformat()
                ))
                conn.commit()
                return cursor.lastrowid
        except Exception as e:
            self.logger.error(f"Failed to create job {job.job_name}: {e}")
            return None
    
    def update_job_status(self, job_id: int, status: JobStatus, 
                         error_message: Optional[str] = None,
                         processed_filings: Optional[int] = None,
                         failed_filings: Optional[int] = None) -> bool:
        """Update job processing status.
        
        Args:
            job_id: Job ID to update
            status: New job status
            error_message: Optional error message
            processed_filings: Number of processed filings
            failed_filings: Number of failed filings
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.get_connection() as conn:
                updates = ["status = ?", "updated_at = ?"]
                values = [status.value, datetime.now(timezone.utc).isoformat()]
                
                if status == JobStatus.RUNNING and processed_filings is None:
                    updates.append("start_time = ?")
                    values.append(datetime.now(timezone.utc).isoformat())
                elif status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
                    updates.append("end_time = ?")
                    values.append(datetime.now(timezone.utc).isoformat())
                
                if error_message is not None:
                    updates.append("error_message = ?")
                    values.append(error_message)
                
                if processed_filings is not None:
                    updates.append("processed_filings = ?")
                    values.append(processed_filings)
                
                if failed_filings is not None:
                    updates.append("failed_filings = ?")
                    values.append(failed_filings)
                
                values.append(job_id)
                
                conn.execute(f"""
                    UPDATE processing_jobs SET {', '.join(updates)}
                    WHERE id = ?
                """, values)
                conn.commit()
                return True
        except Exception as e:
            self.logger.error(f"Failed to update job {job_id}: {e}")
            return False
    
    def record_metric(self, metric: PerformanceMetric) -> bool:
        """Record a performance metric.
        
        Args:
            metric: PerformanceMetric object to record
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.get_connection() as conn:
                conn.execute("""
                    INSERT INTO performance_metrics 
                    (job_id, metric_name, metric_value, metric_unit, timestamp, metadata)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    metric.job_id,
                    metric.metric_name,
                    metric.metric_value,
                    metric.metric_unit,
                    metric.timestamp.isoformat() if metric.timestamp else None,
                    json.dumps(metric.metadata)
                ))
                conn.commit()
                return True
        except Exception as e:
            self.logger.error(f"Failed to record metric {metric.metric_name}: {e}")
            return False
    
    def get_job_metrics(self, job_id: int) -> List[PerformanceMetric]:
        """Get all metrics for a specific job.
        
        Args:
            job_id: Job ID to get metrics for
            
        Returns:
            List of performance metrics
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.execute("""
                    SELECT job_id, metric_name, metric_value, metric_unit, timestamp, metadata
                    FROM performance_metrics 
                    WHERE job_id = ?
                    ORDER BY timestamp
                """, (job_id,))
                
                metrics = []
                for row in cursor.fetchall():
                    metric = PerformanceMetric(
                        job_id=row[0],
                        metric_name=row[1],
                        metric_value=row[2],
                        metric_unit=row[3],
                        timestamp=datetime.fromisoformat(row[4]) if row[4] else None,
                        metadata=json.loads(row[5]) if row[5] else {}
                    )
                    metrics.append(metric)
                
                return metrics
        except Exception as e:
            self.logger.error(f"Failed to get metrics for job {job_id}: {e}")
            return []
    
    def cleanup_old_records(self, days: int = 30) -> bool:
        """Clean up old records to manage database size.
        
        Args:
            days: Number of days to keep records
            
        Returns:
            True if successful, False otherwise
        """
        try:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
            
            with self.get_connection() as conn:
                # Clean up old performance metrics
                conn.execute("""
                    DELETE FROM performance_metrics 
                    WHERE timestamp < ?
                """, (cutoff_date.isoformat(),))
                
                # Clean up old completed jobs (keep failed jobs longer)
                conn.execute("""
                    DELETE FROM processing_jobs 
                    WHERE status = 'completed' AND end_time < ?
                """, (cutoff_date.isoformat(),))
                
                conn.commit()
                return True
        except Exception as e:
            self.logger.error(f"Failed to cleanup old records: {e}")
            return False