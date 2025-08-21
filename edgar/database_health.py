"""Database health checks and monitoring utilities."""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

try:
    from sqlalchemy import text
    from sqlalchemy.exc import SQLAlchemyError
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False

from .database_config import DatabaseConfig
from .database_manager import DatabaseManager


@dataclass
class HealthCheckResult:
    """Result of a health check operation."""
    
    is_healthy: bool
    response_time_ms: float
    timestamp: datetime
    error_message: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "is_healthy": self.is_healthy,
            "response_time_ms": self.response_time_ms,
            "timestamp": self.timestamp.isoformat(),
            "error_message": self.error_message,
            "details": self.details
        }


@dataclass
class ConnectionPoolStats:
    """Connection pool statistics."""
    
    size: int
    checked_in: int
    checked_out: int
    overflow: int
    invalid: int
    timestamp: datetime

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "size": self.size,
            "checked_in": self.checked_in,
            "checked_out": self.checked_out,
            "overflow": self.overflow,
            "invalid": self.invalid,
            "utilization_percent": (self.checked_out / max(self.size, 1)) * 100,
            "timestamp": self.timestamp.isoformat()
        }


class DatabaseHealthChecker:
    """Performs health checks on database connections."""

    def __init__(self, db_manager: DatabaseManager):
        """Initialize health checker.
        
        Args:
            db_manager: Database manager instance
        """
        if not SQLALCHEMY_AVAILABLE:
            raise ImportError(
                "SQLAlchemy is required for health checks. "
                "Install with: pip install sqlalchemy"
            )
        
        self.db_manager = db_manager
        self.config = db_manager.config
        self.logger = logging.getLogger(__name__)

    def check_connection(self) -> HealthCheckResult:
        """Perform a basic connection health check.
        
        Returns:
            HealthCheckResult with connection status
        """
        start_time = time.time()
        timestamp = datetime.utcnow()
        
        try:
            with self.db_manager.get_session() as session:
                # Simple query to test connection
                if self.config.database_type == "sqlite":
                    result = session.execute(text("SELECT 1 as health_check"))
                else:
                    result = session.execute(text("SELECT 1 as health_check"))
                
                # Verify we got a result
                row = result.fetchone()
                if not row or row[0] != 1:
                    raise Exception("Health check query returned unexpected result")
            
            response_time = (time.time() - start_time) * 1000
            
            return HealthCheckResult(
                is_healthy=True,
                response_time_ms=response_time,
                timestamp=timestamp,
                details={
                    "database_type": self.config.database_type,
                    "query": "SELECT 1 as health_check"
                }
            )
            
        except Exception as error:
            response_time = (time.time() - start_time) * 1000
            self.logger.warning(f"Database health check failed: {error}")
            
            return HealthCheckResult(
                is_healthy=False,
                response_time_ms=response_time,
                timestamp=timestamp,
                error_message=str(error),
                details={
                    "database_type": self.config.database_type,
                    "error_type": type(error).__name__
                }
            )

    async def async_check_connection(self) -> HealthCheckResult:
        """Perform an async connection health check.
        
        Returns:
            HealthCheckResult with connection status
        """
        start_time = time.time()
        timestamp = datetime.utcnow()
        
        try:
            async with self.db_manager.get_async_session() as session:
                # Simple query to test connection
                if self.config.database_type == "sqlite":
                    result = await session.execute(text("SELECT 1 as health_check"))
                else:
                    result = await session.execute(text("SELECT 1 as health_check"))
                
                # Verify we got a result
                row = result.fetchone()
                if not row or row[0] != 1:
                    raise Exception("Async health check query returned unexpected result")
            
            response_time = (time.time() - start_time) * 1000
            
            return HealthCheckResult(
                is_healthy=True,
                response_time_ms=response_time,
                timestamp=timestamp,
                details={
                    "database_type": self.config.database_type,
                    "query": "SELECT 1 as health_check",
                    "async": True
                }
            )
            
        except Exception as error:
            response_time = (time.time() - start_time) * 1000
            self.logger.warning(f"Async database health check failed: {error}")
            
            return HealthCheckResult(
                is_healthy=False,
                response_time_ms=response_time,
                timestamp=timestamp,
                error_message=str(error),
                details={
                    "database_type": self.config.database_type,
                    "error_type": type(error).__name__,
                    "async": True
                }
            )

    def check_read_write(self) -> HealthCheckResult:
        """Perform a read-write health check.
        
        Returns:
            HealthCheckResult with read-write capability status
        """
        start_time = time.time()
        timestamp = datetime.utcnow()
        test_id = int(time.time() * 1000)  # Use timestamp as unique ID
        
        try:
            with self.db_manager.transaction() as session:
                # Create a temporary table for testing
                if self.config.database_type == "sqlite":
                    session.execute(text("""
                        CREATE TEMPORARY TABLE health_check_test (
                            id INTEGER PRIMARY KEY,
                            test_value TEXT
                        )
                    """))
                else:
                    session.execute(text("""
                        CREATE TEMPORARY TABLE health_check_test (
                            id INTEGER PRIMARY KEY,
                            test_value VARCHAR(100)
                        )
                    """))
                
                # Insert test data
                session.execute(
                    text("INSERT INTO health_check_test (id, test_value) VALUES (:id, :value)"),
                    {"id": test_id, "value": f"test_{test_id}"}
                )
                
                # Read back test data
                result = session.execute(
                    text("SELECT test_value FROM health_check_test WHERE id = :id"),
                    {"id": test_id}
                )
                
                row = result.fetchone()
                if not row or row[0] != f"test_{test_id}":
                    raise Exception("Read-write test failed: data mismatch")
                
                # Clean up
                session.execute(text("DROP TABLE health_check_test"))
            
            response_time = (time.time() - start_time) * 1000
            
            return HealthCheckResult(
                is_healthy=True,
                response_time_ms=response_time,
                timestamp=timestamp,
                details={
                    "database_type": self.config.database_type,
                    "test_type": "read_write",
                    "operations": ["CREATE TABLE", "INSERT", "SELECT", "DROP TABLE"]
                }
            )
            
        except Exception as error:
            response_time = (time.time() - start_time) * 1000
            self.logger.warning(f"Database read-write health check failed: {error}")
            
            return HealthCheckResult(
                is_healthy=False,
                response_time_ms=response_time,
                timestamp=timestamp,
                error_message=str(error),
                details={
                    "database_type": self.config.database_type,
                    "test_type": "read_write",
                    "error_type": type(error).__name__
                }
            )

    def get_connection_pool_stats(self) -> Optional[ConnectionPoolStats]:
        """Get connection pool statistics.
        
        Returns:
            ConnectionPoolStats if available, None otherwise
        """
        try:
            engine = self.db_manager.engine
            pool = engine.pool
            
            # Different pool types have different attributes
            stats = ConnectionPoolStats(
                size=getattr(pool, 'size', lambda: 0)(),
                checked_in=getattr(pool, 'checkedin', lambda: 0)(),
                checked_out=getattr(pool, 'checkedout', lambda: 0)(),
                overflow=getattr(pool, 'overflow', lambda: 0)(),
                invalid=getattr(pool, 'invalidated', lambda: 0)(),
                timestamp=datetime.utcnow()
            )
            
            return stats
            
        except Exception as error:
            self.logger.warning(f"Failed to get connection pool stats: {error}")
            return None

    def check_database_size(self) -> HealthCheckResult:
        """Check database size and storage information.
        
        Returns:
            HealthCheckResult with database size information
        """
        start_time = time.time()
        timestamp = datetime.utcnow()
        
        try:
            with self.db_manager.get_session() as session:
                if self.config.database_type == "sqlite":
                    # Get SQLite database size
                    result = session.execute(text("PRAGMA page_count"))
                    page_count = result.scalar()
                    
                    result = session.execute(text("PRAGMA page_size"))
                    page_size = result.scalar()
                    
                    size_bytes = page_count * page_size
                    
                    details = {
                        "size_bytes": size_bytes,
                        "size_mb": round(size_bytes / (1024 * 1024), 2),
                        "page_count": page_count,
                        "page_size": page_size
                    }
                    
                else:
                    # Get PostgreSQL database size
                    result = session.execute(text(
                        "SELECT pg_database_size(current_database())"
                    ))
                    size_bytes = result.scalar()
                    
                    details = {
                        "size_bytes": size_bytes,
                        "size_mb": round(size_bytes / (1024 * 1024), 2)
                    }
            
            response_time = (time.time() - start_time) * 1000
            
            return HealthCheckResult(
                is_healthy=True,
                response_time_ms=response_time,
                timestamp=timestamp,
                details=details
            )
            
        except Exception as error:
            response_time = (time.time() - start_time) * 1000
            self.logger.warning(f"Database size check failed: {error}")
            
            return HealthCheckResult(
                is_healthy=False,
                response_time_ms=response_time,
                timestamp=timestamp,
                error_message=str(error),
                details={
                    "database_type": self.config.database_type,
                    "error_type": type(error).__name__
                }
            )

    def comprehensive_health_check(self) -> Dict[str, Any]:
        """Perform a comprehensive health check.
        
        Returns:
            Dictionary containing all health check results
        """
        start_time = time.time()
        
        results = {
            "timestamp": datetime.utcnow().isoformat(),
            "database_type": self.config.database_type,
            "checks": {}
        }
        
        # Basic connection check
        connection_result = self.check_connection()
        results["checks"]["connection"] = connection_result.to_dict()
        
        # Read-write check (only if connection is healthy)
        if connection_result.is_healthy:
            rw_result = self.check_read_write()
            results["checks"]["read_write"] = rw_result.to_dict()
            
            # Database size check
            size_result = self.check_database_size()
            results["checks"]["database_size"] = size_result.to_dict()
        
        # Connection pool stats
        pool_stats = self.get_connection_pool_stats()
        if pool_stats:
            results["connection_pool"] = pool_stats.to_dict()
        
        # Overall health status
        all_healthy = all(
            check.get("is_healthy", False)
            for check in results["checks"].values()
        )
        
        results["overall_healthy"] = all_healthy
        results["total_time_ms"] = (time.time() - start_time) * 1000
        
        return results


class DatabaseMonitor:
    """Continuous database monitoring with alerting."""

    def __init__(
        self,
        health_checker: DatabaseHealthChecker,
        check_interval: int = 60,
        alert_threshold: int = 3
    ):
        """Initialize database monitor.
        
        Args:
            health_checker: DatabaseHealthChecker instance
            check_interval: Seconds between health checks
            alert_threshold: Number of consecutive failures before alerting
        """
        self.health_checker = health_checker
        self.check_interval = check_interval
        self.alert_threshold = alert_threshold
        self.logger = logging.getLogger(__name__)
        
        self.is_monitoring = False
        self.consecutive_failures = 0
        self.last_check_result = None
        self.check_history: List[HealthCheckResult] = []
        self.alert_callbacks = []

    def add_alert_callback(self, callback):
        """Add a callback function to be called on alerts.
        
        Args:
            callback: Function to call with alert information
        """
        self.alert_callbacks.append(callback)

    def start_monitoring(self) -> None:
        """Start continuous monitoring."""
        if self.is_monitoring:
            self.logger.warning("Monitoring is already running")
            return
        
        self.is_monitoring = True
        self.logger.info(f"Starting database monitoring (interval: {self.check_interval}s)")
        
        # Start monitoring in a separate thread
        import threading
        monitor_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
        monitor_thread.start()

    def stop_monitoring(self) -> None:
        """Stop continuous monitoring."""
        self.is_monitoring = False
        self.logger.info("Stopping database monitoring")

    def _monitoring_loop(self) -> None:
        """Main monitoring loop."""
        while self.is_monitoring:
            try:
                # Perform health check
                result = self.health_checker.check_connection()
                self.last_check_result = result
                
                # Add to history (keep last 100 checks)
                self.check_history.append(result)
                if len(self.check_history) > 100:
                    self.check_history.pop(0)
                
                if result.is_healthy:
                    if self.consecutive_failures > 0:
                        self.logger.info(
                            f"Database health recovered after {self.consecutive_failures} failures"
                        )
                        self._trigger_alert("recovery", result)
                    
                    self.consecutive_failures = 0
                else:
                    self.consecutive_failures += 1
                    self.logger.warning(
                        f"Database health check failed ({self.consecutive_failures} consecutive failures): "
                        f"{result.error_message}"
                    )
                    
                    if self.consecutive_failures >= self.alert_threshold:
                        self._trigger_alert("failure", result)
                
            except Exception as error:
                self.logger.error(f"Error in monitoring loop: {error}")
                self.consecutive_failures += 1
            
            # Wait for next check
            time.sleep(self.check_interval)

    def _trigger_alert(self, alert_type: str, result: HealthCheckResult) -> None:
        """Trigger alert callbacks.
        
        Args:
            alert_type: Type of alert ("failure" or "recovery")
            result: Health check result that triggered the alert
        """
        alert_data = {
            "type": alert_type,
            "timestamp": datetime.utcnow().isoformat(),
            "consecutive_failures": self.consecutive_failures,
            "result": result.to_dict()
        }
        
        for callback in self.alert_callbacks:
            try:
                callback(alert_data)
            except Exception as error:
                self.logger.error(f"Error in alert callback: {error}")

    def get_monitoring_status(self) -> Dict[str, Any]:
        """Get current monitoring status.
        
        Returns:
            Dictionary containing monitoring status
        """
        return {
            "is_monitoring": self.is_monitoring,
            "check_interval": self.check_interval,
            "alert_threshold": self.alert_threshold,
            "consecutive_failures": self.consecutive_failures,
            "last_check": self.last_check_result.to_dict() if self.last_check_result else None,
            "total_checks": len(self.check_history),
            "success_rate": self._calculate_success_rate()
        }

    def _calculate_success_rate(self) -> float:
        """Calculate success rate from check history.
        
        Returns:
            Success rate as percentage
        """
        if not self.check_history:
            return 0.0
        
        successful_checks = sum(1 for check in self.check_history if check.is_healthy)
        return (successful_checks / len(self.check_history)) * 100

    async def async_start_monitoring(self) -> None:
        """Start async monitoring."""
        if self.is_monitoring:
            self.logger.warning("Monitoring is already running")
            return
        
        self.is_monitoring = True
        self.logger.info(f"Starting async database monitoring (interval: {self.check_interval}s)")
        
        # Start async monitoring loop
        asyncio.create_task(self._async_monitoring_loop())

    async def _async_monitoring_loop(self) -> None:
        """Async monitoring loop."""
        while self.is_monitoring:
            try:
                # Perform async health check
                result = await self.health_checker.async_check_connection()
                self.last_check_result = result
                
                # Add to history
                self.check_history.append(result)
                if len(self.check_history) > 100:
                    self.check_history.pop(0)
                
                if result.is_healthy:
                    if self.consecutive_failures > 0:
                        self.logger.info(
                            f"Database health recovered after {self.consecutive_failures} failures"
                        )
                        self._trigger_alert("recovery", result)
                    
                    self.consecutive_failures = 0
                else:
                    self.consecutive_failures += 1
                    self.logger.warning(
                        f"Async database health check failed ({self.consecutive_failures} consecutive failures): "
                        f"{result.error_message}"
                    )
                    
                    if self.consecutive_failures >= self.alert_threshold:
                        self._trigger_alert("failure", result)
                
            except Exception as error:
                self.logger.error(f"Error in async monitoring loop: {error}")
                self.consecutive_failures += 1
            
            # Wait for next check
            await asyncio.sleep(self.check_interval)