"""Performance monitoring and SLA compliance system for EDGAR filing processing."""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

from .database_models import DatabaseManager, JobStatus, PerformanceMetric


@dataclass
class SLATarget:
    """SLA target configuration."""
    
    max_duration_minutes: int = 30
    max_filings: int = 50
    success_rate_threshold: float = 95.0
    throughput_threshold: float = 1.0  # filings per minute
    
    def is_compliant(
        self,
        duration_minutes: float,
        filing_count: int,
        success_rate: float,
        throughput: float,
    ) -> bool:
        """Check if metrics meet SLA targets.
        
        Args:
            duration_minutes: Processing duration in minutes
            filing_count: Number of filings processed
            success_rate: Success rate percentage
            throughput: Processing throughput (filings per minute)
            
        Returns:
            True if all SLA targets are met
        """
        if filing_count > self.max_filings:
            # Scale duration target for larger batches
            scaled_duration = self.max_duration_minutes * (filing_count / self.max_filings)
        else:
            scaled_duration = self.max_duration_minutes
        
        return (
            duration_minutes <= scaled_duration
            and success_rate >= self.success_rate_threshold
            and throughput >= self.throughput_threshold
        )


@dataclass
class PerformanceReport:
    """Performance analysis report."""
    
    job_id: int
    job_name: str
    start_time: datetime
    end_time: Optional[datetime]
    duration_minutes: Optional[float]
    total_filings: int
    processed_filings: int
    failed_filings: int
    success_rate: float
    throughput: Optional[float]
    sla_compliant: bool
    metrics: Dict[str, float] = field(default_factory=dict)
    recommendations: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert report to dictionary."""
        return {
            "job_id": self.job_id,
            "job_name": self.job_name,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_minutes": self.duration_minutes,
            "total_filings": self.total_filings,
            "processed_filings": self.processed_filings,
            "failed_filings": self.failed_filings,
            "success_rate": self.success_rate,
            "throughput": self.throughput,
            "sla_compliant": self.sla_compliant,
            "metrics": self.metrics,
            "recommendations": self.recommendations,
        }


class PerformanceMonitor:
    """Monitors and analyzes performance of filing processing operations."""
    
    def __init__(
        self,
        db_manager: DatabaseManager,
        sla_target: Optional[SLATarget] = None,
        logger: Optional[logging.Logger] = None,
    ):
        """Initialize performance monitor.
        
        Args:
            db_manager: Database manager for metrics storage
            sla_target: SLA target configuration
            logger: Optional logger instance
        """
        self.db_manager = db_manager
        self.sla_target = sla_target or SLATarget()
        self.logger = logger or logging.getLogger(__name__)
        self._monitoring_tasks: List[asyncio.Task] = []
        self._shutdown_event = asyncio.Event()
    
    def record_job_start(self, job_id: int) -> None:
        """Record job start metrics.
        
        Args:
            job_id: Job ID to record metrics for
        """
        metrics = [
            PerformanceMetric(
                job_id=job_id,
                metric_name="job_start",
                metric_value=time.time(),
                metric_unit="timestamp",
            ),
            PerformanceMetric(
                job_id=job_id,
                metric_name="memory_usage_start",
                metric_value=self._get_memory_usage(),
                metric_unit="mb",
            ),
        ]
        
        for metric in metrics:
            self.db_manager.record_metric(metric)
    
    def record_job_completion(
        self,
        job_id: int,
        total_filings: int,
        processed_filings: int,
        failed_filings: int,
        duration_seconds: float,
    ) -> None:
        """Record job completion metrics.
        
        Args:
            job_id: Job ID to record metrics for
            total_filings: Total number of filings
            processed_filings: Number of successfully processed filings
            failed_filings: Number of failed filings
            duration_seconds: Processing duration in seconds
        """
        success_rate = (processed_filings / total_filings * 100) if total_filings > 0 else 0
        throughput = (processed_filings / duration_seconds * 60) if duration_seconds > 0 else 0
        
        metrics = [
            PerformanceMetric(
                job_id=job_id,
                metric_name="job_completion",
                metric_value=time.time(),
                metric_unit="timestamp",
            ),
            PerformanceMetric(
                job_id=job_id,
                metric_name="duration",
                metric_value=duration_seconds,
                metric_unit="seconds",
            ),
            PerformanceMetric(
                job_id=job_id,
                metric_name="success_rate",
                metric_value=success_rate,
                metric_unit="percentage",
            ),
            PerformanceMetric(
                job_id=job_id,
                metric_name="throughput",
                metric_value=throughput,
                metric_unit="filings_per_minute",
            ),
            PerformanceMetric(
                job_id=job_id,
                metric_name="memory_usage_end",
                metric_value=self._get_memory_usage(),
                metric_unit="mb",
            ),
        ]
        
        # Check SLA compliance
        duration_minutes = duration_seconds / 60
        sla_compliant = self.sla_target.is_compliant(
            duration_minutes, total_filings, success_rate, throughput
        )
        
        metrics.append(
            PerformanceMetric(
                job_id=job_id,
                metric_name="sla_compliant",
                metric_value=1.0 if sla_compliant else 0.0,
                metric_unit="boolean",
                metadata={
                    "sla_target_duration": self.sla_target.max_duration_minutes,
                    "sla_target_success_rate": self.sla_target.success_rate_threshold,
                    "sla_target_throughput": self.sla_target.throughput_threshold,
                },
            )
        )
        
        for metric in metrics:
            self.db_manager.record_metric(metric)
        
        if not sla_compliant:
            self.logger.warning(
                f"Job {job_id} failed SLA: {duration_minutes:.1f}min, "
                f"{success_rate:.1f}% success, {throughput:.2f} filings/min"
            )
    
    def record_cik_processing_metrics(
        self,
        job_id: int,
        cik: str,
        filings_count: int,
        processing_time: float,
        success: bool,
    ) -> None:
        """Record CIK-level processing metrics.
        
        Args:
            job_id: Job ID
            cik: Company CIK
            filings_count: Number of filings processed
            processing_time: Processing time in seconds
            success: Whether processing was successful
        """
        metrics = [
            PerformanceMetric(
                job_id=job_id,
                metric_name="cik_processing_time",
                metric_value=processing_time,
                metric_unit="seconds",
                metadata={"cik": cik, "filings_count": filings_count},
            ),
            PerformanceMetric(
                job_id=job_id,
                metric_name="cik_success",
                metric_value=1.0 if success else 0.0,
                metric_unit="boolean",
                metadata={"cik": cik},
            ),
        ]
        
        if filings_count > 0 and processing_time > 0:
            cik_throughput = (filings_count / processing_time) * 60
            metrics.append(
                PerformanceMetric(
                    job_id=job_id,
                    metric_name="cik_throughput",
                    metric_value=cik_throughput,
                    metric_unit="filings_per_minute",
                    metadata={"cik": cik},
                )
            )
        
        for metric in metrics:
            self.db_manager.record_metric(metric)
    
    def generate_performance_report(self, job_id: int) -> Optional[PerformanceReport]:
        """Generate comprehensive performance report for a job.
        
        Args:
            job_id: Job ID to generate report for
            
        Returns:
            Performance report or None if job not found
        """
        try:
            # Get job information
            with self.db_manager.get_connection() as conn:
                cursor = conn.execute("""
                    SELECT job_name, status, total_filings, processed_filings, 
                           failed_filings, start_time, end_time
                    FROM processing_jobs WHERE id = ?
                """, (job_id,))
                
                job_row = cursor.fetchone()
                if not job_row:
                    return None
                
                job_name = job_row[0]
                total_filings = job_row[2]
                processed_filings = job_row[3]
                failed_filings = job_row[4]
                start_time_str = job_row[5]
                end_time_str = job_row[6]
            
            # Parse timestamps
            start_time = datetime.fromisoformat(start_time_str) if start_time_str else None
            end_time = datetime.fromisoformat(end_time_str) if end_time_str else None
            
            if not start_time:
                return None
            
            # Calculate basic metrics
            duration_minutes = None
            if end_time:
                duration_minutes = (end_time - start_time).total_seconds() / 60
            
            success_rate = (processed_filings / total_filings * 100) if total_filings > 0 else 0
            throughput = None
            if duration_minutes and duration_minutes > 0:
                throughput = processed_filings / duration_minutes
            
            # Get detailed metrics
            metrics = self.db_manager.get_job_metrics(job_id)
            metric_dict = {m.metric_name: m.metric_value for m in metrics}
            
            # Check SLA compliance
            sla_compliant = False
            if duration_minutes and throughput:
                sla_compliant = self.sla_target.is_compliant(
                    duration_minutes, total_filings, success_rate, throughput
                )
            
            # Generate recommendations
            recommendations = self._generate_recommendations(
                duration_minutes, total_filings, success_rate, throughput, metric_dict
            )
            
            return PerformanceReport(
                job_id=job_id,
                job_name=job_name,
                start_time=start_time,
                end_time=end_time,
                duration_minutes=duration_minutes,
                total_filings=total_filings,
                processed_filings=processed_filings,
                failed_filings=failed_filings,
                success_rate=success_rate,
                throughput=throughput,
                sla_compliant=sla_compliant,
                metrics=metric_dict,
                recommendations=recommendations,
            )
            
        except Exception as e:
            self.logger.error(f"Failed to generate performance report for job {job_id}: {e}")
            return None
    
    def _generate_recommendations(
        self,
        duration_minutes: Optional[float],
        total_filings: int,
        success_rate: float,
        throughput: Optional[float],
        metrics: Dict[str, float],
    ) -> List[str]:
        """Generate performance improvement recommendations.
        
        Args:
            duration_minutes: Processing duration in minutes
            total_filings: Total number of filings
            success_rate: Success rate percentage
            throughput: Processing throughput
            metrics: Additional metrics
            
        Returns:
            List of recommendation strings
        """
        recommendations = []
        
        # Duration recommendations
        if duration_minutes and duration_minutes > self.sla_target.max_duration_minutes:
            if total_filings <= self.sla_target.max_filings:
                recommendations.append(
                    "Consider increasing concurrency limits or optimizing rate limiting"
                )
            else:
                recommendations.append(
                    "Consider breaking large batches into smaller chunks for better performance"
                )
        
        # Success rate recommendations
        if success_rate < self.sla_target.success_rate_threshold:
            recommendations.append(
                "Investigate and address frequent failure patterns. "
                "Consider implementing more robust retry mechanisms."
            )
        
        # Throughput recommendations
        if throughput and throughput < self.sla_target.throughput_threshold:
            recommendations.append(
                "Low throughput detected. Consider increasing rate limits, "
                "optimizing network settings, or using async processing."
            )
        
        # Memory usage recommendations
        memory_start = metrics.get("memory_usage_start", 0)
        memory_end = metrics.get("memory_usage_end", 0)
        if memory_end > memory_start * 2:
            recommendations.append(
                "High memory growth detected. Consider implementing "
                "streaming processing for large documents."
            )
        
        # CIK processing time variance
        cik_times = [
            m.metric_value for m in self.db_manager.get_job_metrics(0)
            if m.metric_name == "cik_processing_time"
        ]
        if len(cik_times) > 1:
            avg_time = sum(cik_times) / len(cik_times)
            max_time = max(cik_times)
            if max_time > avg_time * 3:
                recommendations.append(
                    "High variance in CIK processing times detected. "
                    "Consider implementing adaptive load balancing."
                )
        
        if not recommendations:
            recommendations.append("Performance is within acceptable parameters.")
        
        return recommendations
    
    def get_sla_compliance_summary(self, days: int = 7) -> Dict[str, Any]:
        """Get SLA compliance summary for the specified period.
        
        Args:
            days: Number of days to analyze
            
        Returns:
            SLA compliance summary
        """
        try:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
            
            with self.db_manager.get_connection() as conn:
                # Get completed jobs in the period
                cursor = conn.execute("""
                    SELECT id, job_name, total_filings, processed_filings, 
                           failed_filings, start_time, end_time
                    FROM processing_jobs 
                    WHERE status = 'completed' AND end_time >= ?
                    ORDER BY end_time DESC
                """, (cutoff_date.isoformat(),))
                
                jobs = cursor.fetchall()
                
                if not jobs:
                    return {
                        "period_days": days,
                        "total_jobs": 0,
                        "compliant_jobs": 0,
                        "compliance_rate": 0.0,
                        "avg_duration_minutes": 0.0,
                        "avg_throughput": 0.0,
                        "avg_success_rate": 0.0,
                    }
                
                compliant_count = 0
                durations = []
                throughputs = []
                success_rates = []
                
                for job in jobs:
                    job_id, job_name, total_filings, processed_filings, failed_filings, start_time_str, end_time_str = job
                    
                    start_time = datetime.fromisoformat(start_time_str)
                    end_time = datetime.fromisoformat(end_time_str)
                    
                    duration_minutes = (end_time - start_time).total_seconds() / 60
                    success_rate = (processed_filings / total_filings * 100) if total_filings > 0 else 0
                    throughput = (processed_filings / duration_minutes) if duration_minutes > 0 else 0
                    
                    durations.append(duration_minutes)
                    success_rates.append(success_rate)
                    throughputs.append(throughput)
                    
                    if self.sla_target.is_compliant(duration_minutes, total_filings, success_rate, throughput):
                        compliant_count += 1
                
                return {
                    "period_days": days,
                    "total_jobs": len(jobs),
                    "compliant_jobs": compliant_count,
                    "compliance_rate": (compliant_count / len(jobs)) * 100,
                    "avg_duration_minutes": sum(durations) / len(durations),
                    "avg_throughput": sum(throughputs) / len(throughputs),
                    "avg_success_rate": sum(success_rates) / len(success_rates),
                    "sla_target": {
                        "max_duration_minutes": self.sla_target.max_duration_minutes,
                        "max_filings": self.sla_target.max_filings,
                        "success_rate_threshold": self.sla_target.success_rate_threshold,
                        "throughput_threshold": self.sla_target.throughput_threshold,
                    },
                }
                
        except Exception as e:
            self.logger.error(f"Failed to get SLA compliance summary: {e}")
            return {}
    
    async def start_monitoring(self, check_interval_seconds: int = 60) -> None:
        """Start continuous performance monitoring.
        
        Args:
            check_interval_seconds: How often to check for performance issues
        """
        self.logger.info("Starting continuous performance monitoring")
        
        monitor_task = asyncio.create_task(
            self._monitor_performance_loop(check_interval_seconds)
        )
        self._monitoring_tasks.append(monitor_task)
    
    async def _monitor_performance_loop(self, check_interval_seconds: int) -> None:
        """Main monitoring loop.
        
        Args:
            check_interval_seconds: Check interval in seconds
        """
        while not self._shutdown_event.is_set():
            try:
                # Check for long-running jobs
                await self._check_long_running_jobs()
                
                # Check for performance degradation
                await self._check_performance_trends()
                
                # Wait for next check
                await asyncio.sleep(check_interval_seconds)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Performance monitoring error: {e}")
                await asyncio.sleep(check_interval_seconds)
    
    async def _check_long_running_jobs(self) -> None:
        """Check for jobs that are running longer than expected."""
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.execute("""
                    SELECT id, job_name, total_filings, start_time
                    FROM processing_jobs 
                    WHERE status = 'running' AND start_time IS NOT NULL
                """)
                
                for row in cursor.fetchall():
                    job_id, job_name, total_filings, start_time_str = row
                    start_time = datetime.fromisoformat(start_time_str)
                    elapsed_minutes = (datetime.now(timezone.utc) - start_time).total_seconds() / 60
                    
                    # Calculate expected duration based on filing count
                    if total_filings <= self.sla_target.max_filings:
                        expected_duration = self.sla_target.max_duration_minutes
                    else:
                        expected_duration = self.sla_target.max_duration_minutes * (total_filings / self.sla_target.max_filings)
                    
                    if elapsed_minutes > expected_duration * 1.5:  # 50% over expected
                        self.logger.warning(
                            f"Long-running job detected: {job_name} (ID: {job_id}) "
                            f"has been running for {elapsed_minutes:.1f} minutes "
                            f"(expected: {expected_duration:.1f} minutes)"
                        )
                        
                        # Record long-running job metric
                        await asyncio.to_thread(
                            self.db_manager.record_metric,
                            PerformanceMetric(
                                job_id=job_id,
                                metric_name="long_running_alert",
                                metric_value=elapsed_minutes,
                                metric_unit="minutes",
                                metadata={
                                    "expected_duration": expected_duration,
                                    "alert_threshold": expected_duration * 1.5,
                                },
                            ),
                        )
        except Exception as e:
            self.logger.error(f"Failed to check long-running jobs: {e}")
    
    async def _check_performance_trends(self) -> None:
        """Check for performance degradation trends."""
        try:
            # Get recent job performance metrics
            recent_jobs = await asyncio.to_thread(self._get_recent_job_performance, 10)
            
            if len(recent_jobs) < 5:
                return  # Not enough data for trend analysis
            
            # Calculate trend metrics
            throughputs = [job["throughput"] for job in recent_jobs if job["throughput"]]
            success_rates = [job["success_rate"] for job in recent_jobs]
            
            if throughputs:
                recent_avg_throughput = sum(throughputs[-3:]) / len(throughputs[-3:])
                historical_avg_throughput = sum(throughputs[:-3]) / len(throughputs[:-3])
                
                if recent_avg_throughput < historical_avg_throughput * 0.8:  # 20% degradation
                    self.logger.warning(
                        f"Throughput degradation detected: recent avg {recent_avg_throughput:.2f} "
                        f"vs historical avg {historical_avg_throughput:.2f} filings/min"
                    )
            
            if success_rates:
                recent_avg_success = sum(success_rates[-3:]) / len(success_rates[-3:])
                historical_avg_success = sum(success_rates[:-3]) / len(success_rates[:-3])
                
                if recent_avg_success < historical_avg_success - 5:  # 5% drop
                    self.logger.warning(
                        f"Success rate degradation detected: recent avg {recent_avg_success:.1f}% "
                        f"vs historical avg {historical_avg_success:.1f}%"
                    )
        except Exception as e:
            self.logger.error(f"Failed to check performance trends: {e}")
    
    def _get_recent_job_performance(self, limit: int) -> List[Dict[str, Any]]:
        """Get performance data for recent jobs.
        
        Args:
            limit: Maximum number of jobs to return
            
        Returns:
            List of job performance data
        """
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.execute("""
                    SELECT id, total_filings, processed_filings, failed_filings,
                           start_time, end_time
                    FROM processing_jobs 
                    WHERE status = 'completed' AND end_time IS NOT NULL
                    ORDER BY end_time DESC LIMIT ?
                """, (limit,))
                
                jobs = []
                for row in cursor.fetchall():
                    job_id, total_filings, processed_filings, failed_filings, start_time_str, end_time_str = row
                    
                    start_time = datetime.fromisoformat(start_time_str)
                    end_time = datetime.fromisoformat(end_time_str)
                    
                    duration_minutes = (end_time - start_time).total_seconds() / 60
                    success_rate = (processed_filings / total_filings * 100) if total_filings > 0 else 0
                    throughput = (processed_filings / duration_minutes) if duration_minutes > 0 else 0
                    
                    jobs.append({
                        "job_id": job_id,
                        "duration_minutes": duration_minutes,
                        "success_rate": success_rate,
                        "throughput": throughput,
                    })
                
                return jobs
        except Exception as e:
            self.logger.error(f"Failed to get recent job performance: {e}")
            return []
    
    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB.
        
        Returns:
            Memory usage in megabytes
        """
        try:
            import psutil
            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024  # Convert to MB
        except ImportError:
            # psutil not available, return 0
            return 0.0
        except Exception:
            return 0.0
    
    async def shutdown(self) -> None:
        """Shutdown performance monitoring."""
        self.logger.info("Shutting down performance monitoring...")
        
        self._shutdown_event.set()
        
        # Cancel all monitoring tasks
        for task in self._monitoring_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        self._monitoring_tasks.clear()
        self.logger.info("Performance monitoring shutdown complete")