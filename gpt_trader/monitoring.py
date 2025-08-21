"""
Performance monitoring and SLA compliance tracking for GPT Trader.

This module provides comprehensive monitoring capabilities including
performance metrics, SLA tracking, and alerting.
"""

import logging
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, field
from collections import defaultdict, deque
import psutil

from .config import GPTTraderConfig

logger = logging.getLogger(__name__)


@dataclass
class PerformanceMetric:
    """Individual performance metric data point."""
    timestamp: datetime
    metric_name: str
    value: float
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class SLAEvent:
    """SLA compliance event."""
    timestamp: datetime
    event_type: str  # 'violation', 'compliance', 'warning'
    metric_name: str
    threshold: float
    actual_value: float
    severity: str = 'info'  # 'info', 'warning', 'critical'


class MetricsCollector:
    """Collects and stores performance metrics."""
    
    def __init__(self, max_metrics: int = 10000):
        self.max_metrics = max_metrics
        self.metrics: deque = deque(maxlen=max_metrics)
        self._lock = threading.Lock()
        
    def record_metric(self, name: str, value: float, labels: Dict[str, str] = None) -> None:
        """Record a performance metric."""
        metric = PerformanceMetric(
            timestamp=datetime.utcnow(),
            metric_name=name,
            value=value,
            labels=labels or {}
        )
        
        with self._lock:
            self.metrics.append(metric)
    
    def get_metrics(self, name: str = None, since: datetime = None) -> List[PerformanceMetric]:
        """Get metrics matching criteria."""
        with self._lock:
            filtered_metrics = []
            
            for metric in self.metrics:
                # Filter by name if specified
                if name and metric.metric_name != name:
                    continue
                
                # Filter by timestamp if specified
                if since and metric.timestamp < since:
                    continue
                
                filtered_metrics.append(metric)
            
            return filtered_metrics
    
    def get_latest_value(self, name: str) -> Optional[float]:
        """Get the latest value for a metric."""
        with self._lock:
            for metric in reversed(self.metrics):
                if metric.metric_name == name:
                    return metric.value
        return None
    
    def get_aggregated_metrics(self, name: str, since: datetime = None) -> Dict[str, float]:
        """Get aggregated statistics for a metric."""
        metrics = self.get_metrics(name, since)
        
        if not metrics:
            return {}
        
        values = [m.value for m in metrics]
        
        return {
            'count': len(values),
            'min': min(values),
            'max': max(values),
            'avg': sum(values) / len(values),
            'sum': sum(values)
        }


class PerformanceMonitor:
    """System performance monitoring."""
    
    def __init__(self, collection_interval: int = 30):
        self.collection_interval = collection_interval
        self.metrics_collector = MetricsCollector()
        self.is_running = False
        self.monitor_thread: Optional[threading.Thread] = None
        self.job_metrics = defaultdict(list)
        
    def start(self) -> None:
        """Start performance monitoring."""
        if self.is_running:
            return
        
        self.is_running = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, name="PerformanceMonitor")
        self.monitor_thread.start()
        
        logger.info("Performance monitoring started")
    
    def stop(self) -> None:
        """Stop performance monitoring."""
        self.is_running = False
        
        if self.monitor_thread:
            self.monitor_thread.join(timeout=30)
        
        logger.info("Performance monitoring stopped")
    
    def record_job(self, job_result) -> None:
        """Record job performance metrics."""
        # Record job duration
        self.metrics_collector.record_metric(
            name="job_duration_seconds",
            value=job_result.duration_seconds,
            labels={
                'job_type': getattr(job_result, 'job_type', 'unknown'),
                'success': str(job_result.success)
            }
        )
        
        # Record items processed
        self.metrics_collector.record_metric(
            name="items_processed_total",
            value=job_result.items_processed,
            labels={'job_uuid': job_result.job_uuid}
        )
        
        # Record failure rate
        self.metrics_collector.record_metric(
            name="items_failed_total",
            value=job_result.items_failed,
            labels={'job_uuid': job_result.job_uuid}
        )
        
        # Calculate and record processing rate
        if job_result.duration_seconds > 0:
            processing_rate = job_result.items_processed / job_result.duration_seconds
            self.metrics_collector.record_metric(
                name="processing_rate_items_per_second",
                value=processing_rate,
                labels={'job_uuid': job_result.job_uuid}
            )
    
    def _monitor_loop(self) -> None:
        """Main monitoring loop."""
        while self.is_running:
            try:
                self._collect_system_metrics()
                time.sleep(self.collection_interval)
            except Exception as e:
                logger.error(f"Performance monitoring error: {e}")
                time.sleep(self.collection_interval)
    
    def _collect_system_metrics(self) -> None:
        """Collect system-level metrics."""
        # CPU usage
        cpu_percent = psutil.cpu_percent(interval=1)
        self.metrics_collector.record_metric("system_cpu_percent", cpu_percent)
        
        # Memory usage
        memory = psutil.virtual_memory()
        self.metrics_collector.record_metric("system_memory_percent", memory.percent)
        self.metrics_collector.record_metric("system_memory_used_mb", memory.used / 1024 / 1024)
        
        # Disk usage
        disk = psutil.disk_usage('/')
        self.metrics_collector.record_metric("system_disk_percent", disk.percent)
        
        # Network I/O
        network = psutil.net_io_counters()
        self.metrics_collector.record_metric("system_network_sent_mb", network.bytes_sent / 1024 / 1024)
        self.metrics_collector.record_metric("system_network_recv_mb", network.bytes_recv / 1024 / 1024)
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current performance metrics summary."""
        now = datetime.utcnow()
        hour_ago = now - timedelta(hours=1)
        
        return {
            'system': {
                'cpu_percent': self.metrics_collector.get_latest_value("system_cpu_percent"),
                'memory_percent': self.metrics_collector.get_latest_value("system_memory_percent"),
                'disk_percent': self.metrics_collector.get_latest_value("system_disk_percent")
            },
            'jobs_last_hour': {
                'avg_duration': self.metrics_collector.get_aggregated_metrics("job_duration_seconds", hour_ago),
                'processing_rate': self.metrics_collector.get_aggregated_metrics("processing_rate_items_per_second", hour_ago),
                'total_items_processed': self.metrics_collector.get_aggregated_metrics("items_processed_total", hour_ago),
                'total_items_failed': self.metrics_collector.get_aggregated_metrics("items_failed_total", hour_ago)
            }
        }


class SLAMonitor:
    """SLA compliance monitoring and alerting."""
    
    def __init__(self, config: GPTTraderConfig):
        self.config = config
        self.sla_events: deque = deque(maxlen=1000)
        self.alert_callbacks: List[Callable[[SLAEvent], None]] = []
        self.is_running = False
        self.monitor_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        
        # SLA thresholds
        self.thresholds = {
            'processing_time_minutes': config.etl.sla_processing_time_minutes,
            'success_rate': config.etl.sla_success_rate_threshold,
            'memory_usage_percent': config.monitoring.alert_memory_usage_threshold_mb / 1024,  # Convert to %
            'failure_rate': config.monitoring.alert_failure_rate_threshold
        }
    
    def start(self) -> None:
        """Start SLA monitoring."""
        if self.is_running:
            return
        
        self.is_running = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, name="SLAMonitor")
        self.monitor_thread.start()
        
        logger.info("SLA monitoring started")
    
    def stop(self) -> None:
        """Stop SLA monitoring."""
        self.is_running = False
        
        if self.monitor_thread:
            self.monitor_thread.join(timeout=30)
        
        logger.info("SLA monitoring stopped")
    
    def record_job(self, job_result) -> None:
        """Record job result and check SLA compliance."""
        # Check processing time SLA
        processing_time_minutes = job_result.duration_seconds / 60
        if processing_time_minutes > self.thresholds['processing_time_minutes']:
            self._record_sla_event(
                event_type='violation',
                metric_name='processing_time_minutes',
                threshold=self.thresholds['processing_time_minutes'],
                actual_value=processing_time_minutes,
                severity='warning'
            )
        
        # Check success rate (need to calculate from recent jobs)
        self._check_success_rate_sla()
    
    def add_alert_callback(self, callback: Callable[[SLAEvent], None]) -> None:
        """Add callback for SLA alerts."""
        self.alert_callbacks.append(callback)
    
    def _record_sla_event(self, event_type: str, metric_name: str, 
                         threshold: float, actual_value: float, severity: str = 'info') -> None:
        """Record an SLA event."""
        event = SLAEvent(
            timestamp=datetime.utcnow(),
            event_type=event_type,
            metric_name=metric_name,
            threshold=threshold,
            actual_value=actual_value,
            severity=severity
        )
        
        with self._lock:
            self.sla_events.append(event)
        
        # Trigger alerts
        for callback in self.alert_callbacks:
            try:
                callback(event)
            except Exception as e:
                logger.error(f"Alert callback error: {e}")
        
        logger.info(f"SLA {event_type}: {metric_name} = {actual_value:.2f} (threshold: {threshold:.2f})")
    
    def _check_success_rate_sla(self) -> None:
        """Check success rate SLA compliance."""
        # This would typically look at recent job results
        # For now, we'll implement a simple check
        pass
    
    def _monitor_loop(self) -> None:
        """Main SLA monitoring loop."""
        while self.is_running:
            try:
                self._check_system_slas()
                time.sleep(60)  # Check every minute
            except Exception as e:
                logger.error(f"SLA monitoring error: {e}")
                time.sleep(60)
    
    def _check_system_slas(self) -> None:
        """Check system-level SLA compliance."""
        # Check memory usage
        memory_percent = psutil.virtual_memory().percent
        if memory_percent > self.thresholds['memory_usage_percent']:
            self._record_sla_event(
                event_type='violation',
                metric_name='memory_usage_percent',
                threshold=self.thresholds['memory_usage_percent'],
                actual_value=memory_percent,
                severity='warning'
            )
    
    def get_status(self) -> Dict[str, Any]:
        """Get current SLA status."""
        with self._lock:
            recent_events = [e for e in self.sla_events 
                           if e.timestamp > datetime.utcnow() - timedelta(hours=24)]
        
        violations = [e for e in recent_events if e.event_type == 'violation']
        
        return {
            'total_events_24h': len(recent_events),
            'violations_24h': len(violations),
            'compliance_status': 'healthy' if len(violations) == 0 else 'degraded',
            'thresholds': self.thresholds,
            'recent_violations': [
                {
                    'timestamp': e.timestamp.isoformat(),
                    'metric': e.metric_name,
                    'threshold': e.threshold,
                    'actual': e.actual_value,
                    'severity': e.severity
                }
                for e in violations[-5:]  # Last 5 violations
            ]
        }


class AlertManager:
    """Alert management and notification system."""
    
    def __init__(self, config: GPTTraderConfig):
        self.config = config
        self.notification_handlers = {}
        
    def register_handler(self, handler_type: str, handler: Callable[[SLAEvent], None]) -> None:
        """Register an alert notification handler."""
        self.notification_handlers[handler_type] = handler
    
    def handle_sla_event(self, event: SLAEvent) -> None:
        """Handle SLA event and send notifications."""
        if event.event_type == 'violation' and event.severity in ['warning', 'critical']:
            # Send notifications
            for handler_type, handler in self.notification_handlers.items():
                try:
                    handler(event)
                except Exception as e:
                    logger.error(f"Alert handler {handler_type} failed: {e}")
    
    def log_alert_handler(self, event: SLAEvent) -> None:
        """Log alert handler - writes alerts to log."""
        logger.warning(f"SLA ALERT: {event.metric_name} = {event.actual_value:.2f} "
                      f"exceeds threshold {event.threshold:.2f} (severity: {event.severity})")
    
    def console_alert_handler(self, event: SLAEvent) -> None:
        """Console alert handler - prints to console."""
        print(f"⚠️  SLA ALERT: {event.metric_name} = {event.actual_value:.2f} "
              f"exceeds threshold {event.threshold:.2f}")


class MetricsExporter:
    """Export metrics to external monitoring systems."""
    
    def __init__(self, metrics_collector: MetricsCollector):
        self.metrics_collector = metrics_collector
    
    def export_prometheus_metrics(self) -> str:
        """Export metrics in Prometheus format."""
        metrics = self.metrics_collector.get_metrics()
        
        lines = []
        for metric in metrics:
            # Convert to Prometheus format
            labels = ",".join([f'{k}="{v}"' for k, v in metric.labels.items()])
            label_str = f"{{{labels}}}" if labels else ""
            
            lines.append(f"gpt_trader_{metric.metric_name}{label_str} {metric.value} "
                        f"{int(metric.timestamp.timestamp() * 1000)}")
        
        return "\n".join(lines)
    
    def export_json_metrics(self) -> Dict[str, Any]:
        """Export metrics in JSON format."""
        metrics = self.metrics_collector.get_metrics()
        
        return {
            'timestamp': datetime.utcnow().isoformat(),
            'metrics': [
                {
                    'timestamp': m.timestamp.isoformat(),
                    'name': m.metric_name,
                    'value': m.value,
                    'labels': m.labels
                }
                for m in metrics
            ]
        }


def setup_monitoring(config: GPTTraderConfig) -> tuple[PerformanceMonitor, SLAMonitor, AlertManager]:
    """Set up complete monitoring stack."""
    # Create monitors
    performance_monitor = PerformanceMonitor()
    sla_monitor = SLAMonitor(config)
    alert_manager = AlertManager(config)
    
    # Set up alerting
    alert_manager.register_handler('log', alert_manager.log_alert_handler)
    alert_manager.register_handler('console', alert_manager.console_alert_handler)
    
    # Connect SLA monitor to alert manager
    sla_monitor.add_alert_callback(alert_manager.handle_sla_event)
    
    return performance_monitor, sla_monitor, alert_manager


if __name__ == "__main__":
    # Example usage
    from .config import GPTTraderConfig
    
    config = GPTTraderConfig()
    performance_monitor, sla_monitor, alert_manager = setup_monitoring(config)
    
    # Start monitoring
    performance_monitor.start()
    sla_monitor.start()
    
    try:
        # Keep running
        import time
        time.sleep(60)
    finally:
        # Stop monitoring
        performance_monitor.stop()
        sla_monitor.stop()