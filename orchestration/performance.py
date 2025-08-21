"""Performance monitoring and SLA compliance tracking for ETL orchestration."""

import json
import logging
import psutil
import statistics
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from threading import Lock, Thread, Event
from typing import Any, Dict, List, Optional, Tuple, Callable
import sqlite3
from pathlib import Path


class AlertLevel(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class SLAStatus(Enum):
    """SLA compliance status."""
    COMPLIANT = "compliant"
    AT_RISK = "at_risk" 
    VIOLATED = "violated"


@dataclass
class SLAMetrics:
    """SLA metrics for job monitoring."""
    
    job_id: str
    start_time: datetime
    target_duration: float  # seconds
    target_throughput: float  # items per minute
    total_items: int
    
    # Current metrics
    current_duration: float = 0.0
    items_completed: int = 0
    current_throughput: float = 0.0
    
    # Status tracking
    status: SLAStatus = SLAStatus.COMPLIANT
    violations: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    # Performance predictions
    estimated_completion_time: Optional[datetime] = None
    estimated_total_duration: Optional[float] = None
    
    def update(self, items_completed: int) -> None:
        """Update SLA metrics with current progress."""
        self.items_completed = items_completed
        
        # Calculate current duration
        self.current_duration = (datetime.now() - self.start_time).total_seconds()
        
        # Calculate current throughput (items per minute)
        if self.current_duration > 0:
            self.current_throughput = (self.items_completed / self.current_duration) * 60
        
        # Estimate completion time
        if self.current_throughput > 0 and self.items_completed > 0:
            remaining_items = self.total_items - self.items_completed
            remaining_minutes = remaining_items / self.current_throughput
            self.estimated_completion_time = datetime.now() + timedelta(minutes=remaining_minutes)
            self.estimated_total_duration = self.current_duration + (remaining_minutes * 60)
        
        # Check SLA compliance
        self._check_sla_compliance()
    
    def _check_sla_compliance(self) -> None:
        """Check current SLA compliance status."""
        self.violations.clear()
        self.warnings.clear()
        
        # Check duration violations
        if self.estimated_total_duration:
            if self.estimated_total_duration > self.target_duration:
                self.violations.append(
                    f"Estimated duration ({self.estimated_total_duration:.1f}s) "
                    f"exceeds target ({self.target_duration:.1f}s)"
                )
                self.status = SLAStatus.VIOLATED
            elif self.estimated_total_duration > self.target_duration * 0.9:
                self.warnings.append(
                    f"Estimated duration approaching target limit "
                    f"({self.estimated_total_duration:.1f}s / {self.target_duration:.1f}s)"
                )
                if self.status == SLAStatus.COMPLIANT:
                    self.status = SLAStatus.AT_RISK
        
        # Check throughput violations
        if self.current_throughput > 0:
            if self.current_throughput < self.target_throughput * 0.7:
                self.violations.append(
                    f"Throughput ({self.current_throughput:.2f}/min) "
                    f"significantly below target ({self.target_throughput:.2f}/min)"
                )
                self.status = SLAStatus.VIOLATED
            elif self.current_throughput < self.target_throughput * 0.9:
                self.warnings.append(
                    f"Throughput below target "
                    f"({self.current_throughput:.2f}/min vs {self.target_throughput:.2f}/min)"
                )
                if self.status == SLAStatus.COMPLIANT:
                    self.status = SLAStatus.AT_RISK
        
        # If no violations or warnings, ensure compliant status
        if not self.violations and not self.warnings:
            self.status = SLAStatus.COMPLIANT


@dataclass
class PerformanceMetrics:
    """System performance metrics."""
    
    timestamp: datetime
    
    # System metrics
    cpu_percent: float
    memory_percent: float
    memory_mb: float
    disk_usage_percent: float
    disk_io_read_mb: float
    disk_io_write_mb: float
    network_bytes_sent: int
    network_bytes_recv: int
    
    # Application metrics
    active_jobs: int
    queue_size: int
    requests_per_second: float
    response_time_ms: float
    error_rate_percent: float
    
    # Resource utilization
    thread_count: int
    file_descriptors: int
    
    @classmethod
    def capture(cls, active_jobs: int = 0, queue_size: int = 0, 
                requests_per_second: float = 0.0, response_time_ms: float = 0.0,
                error_rate_percent: float = 0.0) -> 'PerformanceMetrics':
        """Capture current system performance metrics."""
        # System metrics
        cpu_percent = psutil.cpu_percent(interval=0.1)
        
        memory = psutil.virtual_memory()
        memory_percent = memory.percent
        memory_mb = memory.used / 1024 / 1024
        
        disk = psutil.disk_usage('/')
        disk_usage_percent = (disk.used / disk.total) * 100
        
        disk_io = psutil.disk_io_counters()
        disk_io_read_mb = disk_io.read_bytes / 1024 / 1024 if disk_io else 0
        disk_io_write_mb = disk_io.write_bytes / 1024 / 1024 if disk_io else 0
        
        network = psutil.net_io_counters()
        network_bytes_sent = network.bytes_sent if network else 0
        network_bytes_recv = network.bytes_recv if network else 0
        
        # Process metrics
        process = psutil.Process()
        thread_count = process.num_threads()
        file_descriptors = process.num_fds() if hasattr(process, 'num_fds') else 0
        
        return cls(
            timestamp=datetime.now(),
            cpu_percent=cpu_percent,
            memory_percent=memory_percent,
            memory_mb=memory_mb,
            disk_usage_percent=disk_usage_percent,
            disk_io_read_mb=disk_io_read_mb,
            disk_io_write_mb=disk_io_write_mb,
            network_bytes_sent=network_bytes_sent,
            network_bytes_recv=network_bytes_recv,
            active_jobs=active_jobs,
            queue_size=queue_size,
            requests_per_second=requests_per_second,
            response_time_ms=response_time_ms,
            error_rate_percent=error_rate_percent,
            thread_count=thread_count,
            file_descriptors=file_descriptors
        )


@dataclass
class Alert:
    """Performance alert."""
    
    alert_id: str
    level: AlertLevel
    title: str
    message: str
    job_id: Optional[str] = None
    metric_name: Optional[str] = None
    metric_value: Optional[float] = None
    threshold: Optional[float] = None
    timestamp: datetime = field(default_factory=datetime.now)
    acknowledged: bool = False
    resolved: bool = False


@dataclass
class PerformanceReport:
    """Comprehensive performance report."""
    
    report_id: str
    start_time: datetime
    end_time: datetime
    
    # Job statistics
    total_jobs: int
    successful_jobs: int
    failed_jobs: int
    avg_job_duration: float
    
    # SLA statistics
    sla_compliant_jobs: int
    sla_violations: int
    avg_throughput: float
    
    # System statistics
    avg_cpu_percent: float
    avg_memory_percent: float
    max_memory_mb: float
    avg_response_time_ms: float
    total_errors: int
    
    # Alerts summary
    total_alerts: int
    critical_alerts: int
    warning_alerts: int
    
    # Recommendations
    recommendations: List[str] = field(default_factory=list)
    
    def generate_recommendations(self) -> None:
        """Generate performance improvement recommendations."""
        self.recommendations.clear()
        
        # CPU recommendations
        if self.avg_cpu_percent > 80:
            self.recommendations.append(
                "High CPU usage detected. Consider increasing worker processes or optimizing processing logic."
            )
        
        # Memory recommendations
        if self.avg_memory_percent > 85:
            self.recommendations.append(
                "High memory usage detected. Consider implementing data streaming or batch size optimization."
            )
        
        # Throughput recommendations
        if self.sla_violations > 0:
            self.recommendations.append(
                "SLA violations detected. Consider increasing concurrency or optimizing rate limiting."
            )
        
        # Error rate recommendations
        error_rate = (self.total_errors / max(self.total_jobs, 1)) * 100
        if error_rate > 5:
            self.recommendations.append(
                f"High error rate ({error_rate:.1f}%). Review error logs and implement retry strategies."
            )
        
        # Response time recommendations
        if self.avg_response_time_ms > 5000:
            self.recommendations.append(
                "High response times detected. Consider optimizing network calls or implementing caching."
            )


class PerformancePersistence:
    """Persistence layer for performance data."""
    
    def __init__(self, db_path: str = "performance.db"):
        self.db_path = Path(db_path)
        self.logger = logging.getLogger(__name__)
        self._init_database()
    
    def _init_database(self) -> None:
        """Initialize SQLite database for performance storage."""
        with sqlite3.connect(self.db_path) as conn:
            # Metrics table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS performance_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TIMESTAMP NOT NULL,
                    cpu_percent REAL,
                    memory_percent REAL,
                    memory_mb REAL,
                    disk_usage_percent REAL,
                    active_jobs INTEGER,
                    queue_size INTEGER,
                    requests_per_second REAL,
                    response_time_ms REAL,
                    error_rate_percent REAL,
                    thread_count INTEGER
                )
            """)
            
            # SLA metrics table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sla_metrics (
                    job_id TEXT PRIMARY KEY,
                    start_time TIMESTAMP NOT NULL,
                    target_duration REAL NOT NULL,
                    target_throughput REAL NOT NULL,
                    total_items INTEGER NOT NULL,
                    final_duration REAL,
                    items_completed INTEGER,
                    final_throughput REAL,
                    status TEXT,
                    violations TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Alerts table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS alerts (
                    alert_id TEXT PRIMARY KEY,
                    level TEXT NOT NULL,
                    title TEXT NOT NULL,
                    message TEXT NOT NULL,
                    job_id TEXT,
                    metric_name TEXT,
                    metric_value REAL,
                    threshold REAL,
                    timestamp TIMESTAMP NOT NULL,
                    acknowledged BOOLEAN DEFAULT FALSE,
                    resolved BOOLEAN DEFAULT FALSE
                )
            """)
            
            # Create indexes
            conn.execute("CREATE INDEX IF NOT EXISTS idx_metrics_timestamp ON performance_metrics (timestamp)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_sla_start_time ON sla_metrics (start_time)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_timestamp ON alerts (timestamp)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_level ON alerts (level)")
    
    def save_metrics(self, metrics: PerformanceMetrics) -> None:
        """Save performance metrics to database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO performance_metrics 
                    (timestamp, cpu_percent, memory_percent, memory_mb, disk_usage_percent,
                     active_jobs, queue_size, requests_per_second, response_time_ms,
                     error_rate_percent, thread_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    metrics.timestamp,
                    metrics.cpu_percent,
                    metrics.memory_percent,
                    metrics.memory_mb,
                    metrics.disk_usage_percent,
                    metrics.active_jobs,
                    metrics.queue_size,
                    metrics.requests_per_second,
                    metrics.response_time_ms,
                    metrics.error_rate_percent,
                    metrics.thread_count
                ))
        except Exception as e:
            self.logger.error(f"Failed to save performance metrics: {e}")
    
    def save_sla_metrics(self, sla_metrics: SLAMetrics) -> None:
        """Save SLA metrics to database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO sla_metrics 
                    (job_id, start_time, target_duration, target_throughput, total_items,
                     final_duration, items_completed, final_throughput, status, violations)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    sla_metrics.job_id,
                    sla_metrics.start_time,
                    sla_metrics.target_duration,
                    sla_metrics.target_throughput,
                    sla_metrics.total_items,
                    sla_metrics.current_duration,
                    sla_metrics.items_completed,
                    sla_metrics.current_throughput,
                    sla_metrics.status.value,
                    json.dumps(sla_metrics.violations)
                ))
        except Exception as e:
            self.logger.error(f"Failed to save SLA metrics: {e}")
    
    def save_alert(self, alert: Alert) -> None:
        """Save alert to database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO alerts 
                    (alert_id, level, title, message, job_id, metric_name,
                     metric_value, threshold, timestamp, acknowledged, resolved)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    alert.alert_id,
                    alert.level.value,
                    alert.title,
                    alert.message,
                    alert.job_id,
                    alert.metric_name,
                    alert.metric_value,
                    alert.threshold,
                    alert.timestamp,
                    alert.acknowledged,
                    alert.resolved
                ))
        except Exception as e:
            self.logger.error(f"Failed to save alert: {e}")
    
    def get_metrics_history(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Get performance metrics history."""
        try:
            cutoff_time = datetime.now() - timedelta(hours=hours)
            
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("""
                    SELECT * FROM performance_metrics 
                    WHERE timestamp > ?
                    ORDER BY timestamp
                """, (cutoff_time,))
                
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            self.logger.error(f"Failed to get metrics history: {e}")
            return []
    
    def get_recent_alerts(self, hours: int = 24, unresolved_only: bool = True) -> List[Dict[str, Any]]:
        """Get recent alerts."""
        try:
            cutoff_time = datetime.now() - timedelta(hours=hours)
            
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                
                where_clause = "WHERE timestamp > ?"
                params = [cutoff_time]
                
                if unresolved_only:
                    where_clause += " AND resolved = FALSE"
                
                cursor = conn.execute(f"""
                    SELECT * FROM alerts 
                    {where_clause}
                    ORDER BY timestamp DESC
                """, params)
                
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            self.logger.error(f"Failed to get recent alerts: {e}")
            return []


class PerformanceMonitor:
    """Performance monitoring and SLA compliance tracking system."""
    
    def __init__(self, 
                 sla_max_duration: float = 1800.0,  # 30 minutes
                 target_throughput: float = 2.78,   # tickers per minute
                 monitoring_interval: float = 10.0,  # seconds
                 persistence_enabled: bool = True,
                 db_path: str = "performance.db"):
        """Initialize performance monitor.
        
        Args:
            sla_max_duration: Maximum allowed job duration in seconds
            target_throughput: Target throughput in items per minute
            monitoring_interval: How often to collect metrics (seconds)
            persistence_enabled: Whether to persist metrics to database
            db_path: Path to SQLite database file
        """
        self.sla_max_duration = sla_max_duration
        self.target_throughput = target_throughput
        self.monitoring_interval = monitoring_interval
        self.persistence_enabled = persistence_enabled
        
        # Monitoring state
        self._sla_metrics: Dict[str, SLAMetrics] = {}
        self._performance_history: deque = deque(maxlen=1000)
        self._alerts: Dict[str, Alert] = {}
        self._alert_callbacks: List[Callable[[Alert], None]] = []
        
        # Thread safety
        self._lock = Lock()
        
        # Background monitoring
        self._monitoring_event = Event()
        self._monitoring_thread: Optional[Thread] = None
        
        # Persistence
        self.persistence = PerformancePersistence(db_path) if persistence_enabled else None
        
        # Logging
        self.logger = logging.getLogger(__name__)
        
        # Start monitoring
        self._start_monitoring()
    
    def start_monitoring(self, sla_metrics: SLAMetrics) -> None:
        """Start monitoring a job's SLA compliance.
        
        Args:
            sla_metrics: SLA metrics configuration
        """
        with self._lock:
            self._sla_metrics[sla_metrics.job_id] = sla_metrics
        
        self.logger.info(f"Started SLA monitoring for job {sla_metrics.job_id}")
    
    def update_job_progress(self, job_id: str, items_completed: int) -> None:
        """Update job progress for SLA monitoring.
        
        Args:
            job_id: Job identifier
            items_completed: Number of items completed
        """
        with self._lock:
            if job_id in self._sla_metrics:
                sla_metrics = self._sla_metrics[job_id]
                sla_metrics.update(items_completed)
                
                # Check for SLA violations and create alerts
                self._check_sla_alerts(sla_metrics)
                
                # Persist metrics
                if self.persistence_enabled:
                    self.persistence.save_sla_metrics(sla_metrics)
    
    def complete_job_monitoring(self, job_id: str) -> SLAMetrics:
        """Complete monitoring for a job and return final metrics.
        
        Args:
            job_id: Job identifier
            
        Returns:
            Final SLA metrics
        """
        with self._lock:
            if job_id in self._sla_metrics:
                sla_metrics = self._sla_metrics[job_id]
                
                # Final update
                sla_metrics.current_duration = (datetime.now() - sla_metrics.start_time).total_seconds()
                
                # Persist final metrics
                if self.persistence_enabled:
                    self.persistence.save_sla_metrics(sla_metrics)
                
                # Remove from active monitoring
                final_metrics = self._sla_metrics.pop(job_id)
                
                self.logger.info(f"Completed SLA monitoring for job {job_id}: {final_metrics.status.value}")
                return final_metrics
        
        raise ValueError(f"Job {job_id} not found in SLA monitoring")
    
    def create_alert(self, 
                    level: AlertLevel,
                    title: str,
                    message: str,
                    job_id: Optional[str] = None,
                    metric_name: Optional[str] = None,
                    metric_value: Optional[float] = None,
                    threshold: Optional[float] = None) -> str:
        """Create a performance alert.
        
        Args:
            level: Alert severity level
            title: Alert title
            message: Alert message
            job_id: Associated job ID
            metric_name: Metric that triggered alert
            metric_value: Current metric value
            threshold: Threshold that was exceeded
            
        Returns:
            Alert ID
        """
        alert = Alert(
            alert_id=str(uuid.uuid4()),
            level=level,
            title=title,
            message=message,
            job_id=job_id,
            metric_name=metric_name,
            metric_value=metric_value,
            threshold=threshold
        )
        
        with self._lock:
            self._alerts[alert.alert_id] = alert
        
        # Persist alert
        if self.persistence_enabled:
            self.persistence.save_alert(alert)
        
        # Notify callbacks
        for callback in self._alert_callbacks:
            try:
                callback(alert)
            except Exception as e:
                self.logger.error(f"Error in alert callback: {e}")
        
        self.logger.warning(f"Alert created: [{level.value}] {title} - {message}")
        return alert.alert_id
    
    def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge an alert.
        
        Args:
            alert_id: Alert identifier
            
        Returns:
            True if acknowledged successfully
        """
        with self._lock:
            if alert_id in self._alerts:
                self._alerts[alert_id].acknowledged = True
                
                if self.persistence_enabled:
                    self.persistence.save_alert(self._alerts[alert_id])
                
                return True
        return False
    
    def resolve_alert(self, alert_id: str) -> bool:
        """Resolve an alert.
        
        Args:
            alert_id: Alert identifier
            
        Returns:
            True if resolved successfully
        """
        with self._lock:
            if alert_id in self._alerts:
                alert = self._alerts[alert_id]
                alert.resolved = True
                alert.acknowledged = True
                
                if self.persistence_enabled:
                    self.persistence.save_alert(alert)
                
                return True
        return False
    
    def get_job_metrics(self, job_id: str) -> Dict[str, Any]:
        """Get SLA metrics for a specific job.
        
        Args:
            job_id: Job identifier
            
        Returns:
            Job metrics dictionary
        """
        with self._lock:
            if job_id in self._sla_metrics:
                sla = self._sla_metrics[job_id]
                return {
                    'job_id': sla.job_id,
                    'status': sla.status.value,
                    'duration': sla.current_duration,
                    'target_duration': sla.target_duration,
                    'throughput': sla.current_throughput,
                    'target_throughput': sla.target_throughput,
                    'items_completed': sla.items_completed,
                    'total_items': sla.total_items,
                    'progress_percent': (sla.items_completed / sla.total_items) * 100,
                    'estimated_completion': sla.estimated_completion_time.isoformat() if sla.estimated_completion_time else None,
                    'violations': sla.violations,
                    'warnings': sla.warnings
                }
        return {}
    
    def get_system_metrics(self) -> Dict[str, Any]:
        """Get current system performance metrics.
        
        Returns:
            System metrics dictionary
        """
        current_metrics = PerformanceMetrics.capture()
        
        with self._lock:
            # Calculate aggregated metrics from history
            if self._performance_history:
                recent_metrics = list(self._performance_history)[-60:]  # Last 60 readings
                
                avg_cpu = statistics.mean(m.cpu_percent for m in recent_metrics)
                avg_memory = statistics.mean(m.memory_percent for m in recent_metrics)
                avg_response_time = statistics.mean(m.response_time_ms for m in recent_metrics)
                
            else:
                avg_cpu = current_metrics.cpu_percent
                avg_memory = current_metrics.memory_percent
                avg_response_time = current_metrics.response_time_ms
            
            # Count active SLA monitoring
            active_sla_jobs = len(self._sla_metrics)
            sla_violations = sum(1 for sla in self._sla_metrics.values() 
                               if sla.status == SLAStatus.VIOLATED)
            
            # Count alerts
            unresolved_alerts = sum(1 for alert in self._alerts.values() if not alert.resolved)
            critical_alerts = sum(1 for alert in self._alerts.values() 
                                if alert.level == AlertLevel.CRITICAL and not alert.resolved)
        
        return {
            'timestamp': datetime.now().isoformat(),
            'current': {
                'cpu_percent': current_metrics.cpu_percent,
                'memory_percent': current_metrics.memory_percent,
                'memory_mb': current_metrics.memory_mb,
                'active_jobs': current_metrics.active_jobs,
                'queue_size': current_metrics.queue_size,
                'thread_count': current_metrics.thread_count
            },
            'averages': {
                'cpu_percent': avg_cpu,
                'memory_percent': avg_memory,
                'response_time_ms': avg_response_time
            },
            'sla': {
                'active_jobs': active_sla_jobs,
                'violations': sla_violations,
                'compliance_rate': ((active_sla_jobs - sla_violations) / max(active_sla_jobs, 1)) * 100
            },
            'alerts': {
                'unresolved': unresolved_alerts,
                'critical': critical_alerts
            }
        }
    
    def generate_performance_report(self, hours: int = 24) -> PerformanceReport:
        """Generate a comprehensive performance report.
        
        Args:
            hours: Number of hours to include in report
            
        Returns:
            Performance report
        """
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)
        
        # Get historical data
        metrics_history = []
        alerts_history = []
        
        if self.persistence_enabled:
            metrics_history = self.persistence.get_metrics_history(hours)
            alerts_history = self.persistence.get_recent_alerts(hours, unresolved_only=False)
        
        # Calculate statistics
        if metrics_history:
            avg_cpu = statistics.mean(m['cpu_percent'] for m in metrics_history if m['cpu_percent'])
            avg_memory = statistics.mean(m['memory_percent'] for m in metrics_history if m['memory_percent'])
            max_memory = max(m['memory_mb'] for m in metrics_history if m['memory_mb'])
            avg_response_time = statistics.mean(m['response_time_ms'] for m in metrics_history if m['response_time_ms'])
        else:
            avg_cpu = avg_memory = max_memory = avg_response_time = 0.0
        
        # Alert statistics
        total_alerts = len(alerts_history)
        critical_alerts = sum(1 for a in alerts_history if a['level'] == AlertLevel.CRITICAL.value)
        warning_alerts = sum(1 for a in alerts_history if a['level'] == AlertLevel.WARNING.value)
        
        # Job statistics (simplified - would need job history in real implementation)
        total_jobs = len(self._sla_metrics)
        successful_jobs = sum(1 for sla in self._sla_metrics.values() if sla.status != SLAStatus.VIOLATED)
        failed_jobs = total_jobs - successful_jobs
        
        avg_job_duration = statistics.mean(sla.current_duration for sla in self._sla_metrics.values()) if self._sla_metrics else 0.0
        avg_throughput = statistics.mean(sla.current_throughput for sla in self._sla_metrics.values()) if self._sla_metrics else 0.0
        
        # Create report
        report = PerformanceReport(
            report_id=str(uuid.uuid4()),
            start_time=start_time,
            end_time=end_time,
            total_jobs=total_jobs,
            successful_jobs=successful_jobs,
            failed_jobs=failed_jobs,
            avg_job_duration=avg_job_duration,
            sla_compliant_jobs=successful_jobs,
            sla_violations=failed_jobs,
            avg_throughput=avg_throughput,
            avg_cpu_percent=avg_cpu,
            avg_memory_percent=avg_memory,
            max_memory_mb=max_memory,
            avg_response_time_ms=avg_response_time,
            total_errors=failed_jobs,  # Simplified
            total_alerts=total_alerts,
            critical_alerts=critical_alerts,
            warning_alerts=warning_alerts
        )
        
        # Generate recommendations
        report.generate_recommendations()
        
        return report
    
    def add_alert_callback(self, callback: Callable[[Alert], None]) -> None:
        """Add callback for alert notifications.
        
        Args:
            callback: Function to call when alerts are created
        """
        self._alert_callbacks.append(callback)
    
    def shutdown(self) -> None:
        """Shutdown the performance monitor."""
        self.logger.info("Shutting down performance monitor...")
        
        # Stop monitoring thread
        self._monitoring_event.set()
        if self._monitoring_thread and self._monitoring_thread.is_alive():
            self._monitoring_thread.join(timeout=5.0)
        
        # Save final metrics
        if self.persistence_enabled:
            with self._lock:
                for sla_metrics in self._sla_metrics.values():
                    self.persistence.save_sla_metrics(sla_metrics)
                for alert in self._alerts.values():
                    self.persistence.save_alert(alert)
        
        self.logger.info("Performance monitor shutdown complete")
    
    def _start_monitoring(self) -> None:
        """Start background monitoring thread."""
        self._monitoring_thread = Thread(target=self._monitoring_loop, daemon=True)
        self._monitoring_thread.start()
    
    def _monitoring_loop(self) -> None:
        """Background monitoring loop."""
        while not self._monitoring_event.is_set():
            try:
                # Capture current metrics
                current_metrics = PerformanceMetrics.capture(
                    active_jobs=len(self._sla_metrics),
                    queue_size=0,  # Would need to get from scheduler
                    requests_per_second=0.0,  # Would calculate from recent history
                    response_time_ms=0.0,  # Would calculate from recent history
                    error_rate_percent=0.0  # Would calculate from recent history
                )
                
                # Store in history
                with self._lock:
                    self._performance_history.append(current_metrics)
                
                # Persist metrics
                if self.persistence_enabled:
                    self.persistence.save_metrics(current_metrics)
                
                # Check system alerts
                self._check_system_alerts(current_metrics)
                
                # Wait for next interval
                self._monitoring_event.wait(timeout=self.monitoring_interval)
                
            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
    
    def _check_sla_alerts(self, sla_metrics: SLAMetrics) -> None:
        """Check for SLA-related alerts."""
        if sla_metrics.status == SLAStatus.VIOLATED:
            for violation in sla_metrics.violations:
                self.create_alert(
                    level=AlertLevel.CRITICAL,
                    title="SLA Violation",
                    message=violation,
                    job_id=sla_metrics.job_id,
                    metric_name="sla_compliance"
                )
        elif sla_metrics.status == SLAStatus.AT_RISK:
            for warning in sla_metrics.warnings:
                self.create_alert(
                    level=AlertLevel.WARNING,
                    title="SLA At Risk",
                    message=warning,
                    job_id=sla_metrics.job_id,
                    metric_name="sla_compliance"
                )
    
    def _check_system_alerts(self, metrics: PerformanceMetrics) -> None:
        """Check for system performance alerts."""
        # CPU alerts
        if metrics.cpu_percent > 90:
            self.create_alert(
                level=AlertLevel.CRITICAL,
                title="High CPU Usage",
                message=f"CPU usage at {metrics.cpu_percent:.1f}%",
                metric_name="cpu_percent",
                metric_value=metrics.cpu_percent,
                threshold=90.0
            )
        elif metrics.cpu_percent > 80:
            self.create_alert(
                level=AlertLevel.WARNING,
                title="Elevated CPU Usage",
                message=f"CPU usage at {metrics.cpu_percent:.1f}%",
                metric_name="cpu_percent",
                metric_value=metrics.cpu_percent,
                threshold=80.0
            )
        
        # Memory alerts
        if metrics.memory_percent > 95:
            self.create_alert(
                level=AlertLevel.CRITICAL,
                title="High Memory Usage",
                message=f"Memory usage at {metrics.memory_percent:.1f}%",
                metric_name="memory_percent",
                metric_value=metrics.memory_percent,
                threshold=95.0
            )
        elif metrics.memory_percent > 85:
            self.create_alert(
                level=AlertLevel.WARNING,
                title="Elevated Memory Usage",
                message=f"Memory usage at {metrics.memory_percent:.1f}%",
                metric_name="memory_percent",
                metric_value=metrics.memory_percent,
                threshold=85.0
            )