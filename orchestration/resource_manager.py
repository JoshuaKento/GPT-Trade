"""Resource management and throttling for SEC rate limits and system resources."""

import asyncio
import logging
import psutil
import threading
import time
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from threading import Event, Lock, Semaphore, Thread
from typing import Any, Dict, List, Optional, Tuple
import queue


@dataclass
class ResourceConfig:
    """Configuration for resource management."""
    
    # Rate limiting
    rate_limit_per_sec: float = 6.0  # SEC limit
    burst_limit: int = 10  # Allow burst requests
    rate_limit_window: float = 60.0  # seconds
    
    # Concurrency limits
    max_concurrent_requests: int = 6
    max_concurrent_jobs: int = 3
    
    # Memory management
    memory_limit_mb: int = 2048
    memory_check_interval: float = 10.0  # seconds
    memory_warning_threshold: float = 0.8  # 80%
    memory_critical_threshold: float = 0.95  # 95%
    
    # Disk space management
    disk_limit_gb: int = 10
    disk_check_interval: float = 30.0  # seconds
    disk_warning_threshold: float = 0.8  # 80%
    
    # Connection pooling
    max_connections: int = 20
    connection_timeout: float = 30.0  # seconds
    
    # Backpressure
    enable_backpressure: bool = True
    backpressure_threshold: float = 0.9  # 90% resource utilization
    backpressure_delay: float = 1.0  # seconds
    
    def validate(self) -> None:
        """Validate configuration parameters."""
        if self.rate_limit_per_sec <= 0:
            raise ValueError("rate_limit_per_sec must be positive")
        if self.rate_limit_per_sec > 10:
            logging.warning("rate_limit_per_sec exceeds SEC recommendations")
        if self.max_concurrent_requests <= 0:
            raise ValueError("max_concurrent_requests must be positive")
        if self.memory_limit_mb <= 0:
            raise ValueError("memory_limit_mb must be positive")


class RateLimiter:
    """Thread-safe rate limiter implementing token bucket algorithm."""
    
    def __init__(self, 
                 rate_per_sec: float = 6.0, 
                 burst_limit: int = 10,
                 window_size: float = 60.0):
        """Initialize rate limiter.
        
        Args:
            rate_per_sec: Requests per second allowed
            burst_limit: Maximum burst requests
            window_size: Time window for rate limiting (seconds)
        """
        self.rate_per_sec = rate_per_sec
        self.burst_limit = burst_limit
        self.window_size = window_size
        
        # Token bucket state
        self.tokens = float(burst_limit)
        self.last_refill = time.time()
        self.lock = Lock()
        
        # Request history for sliding window
        self.request_history = deque()
        
        # Statistics
        self.total_requests = 0
        self.rejected_requests = 0
        
        self.logger = logging.getLogger(__name__)
    
    def acquire(self, tokens: int = 1, timeout: Optional[float] = None) -> bool:
        """Acquire tokens from the rate limiter.
        
        Args:
            tokens: Number of tokens to acquire
            timeout: Maximum time to wait for tokens (None = no timeout)
            
        Returns:
            True if tokens acquired successfully
        """
        start_time = time.time()
        
        while True:
            with self.lock:
                self._refill_tokens()
                
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    self._record_request()
                    self.total_requests += 1
                    return True
                
                # Check if we should wait or reject
                if timeout is not None and (time.time() - start_time) >= timeout:
                    self.rejected_requests += 1
                    return False
                
                # Calculate wait time for next token
                tokens_needed = tokens - self.tokens
                wait_time = tokens_needed / self.rate_per_sec
            
            # Wait outside the lock
            if timeout is not None:
                remaining_timeout = timeout - (time.time() - start_time)
                wait_time = min(wait_time, remaining_timeout)
            
            if wait_time > 0:
                time.sleep(min(wait_time, 0.1))  # Sleep in small increments
    
    def try_acquire(self, tokens: int = 1) -> bool:
        """Try to acquire tokens without waiting.
        
        Args:
            tokens: Number of tokens to acquire
            
        Returns:
            True if tokens acquired successfully
        """
        return self.acquire(tokens, timeout=0)
    
    def wait_for_tokens(self, tokens: int = 1) -> None:
        """Wait until tokens are available.
        
        Args:
            tokens: Number of tokens to wait for
        """
        self.acquire(tokens)
    
    def get_current_rate(self) -> float:
        """Get current request rate (requests per second).
        
        Returns:
            Current request rate
        """
        with self.lock:
            now = time.time()
            cutoff_time = now - self.window_size
            
            # Count requests in the current window
            recent_requests = sum(1 for req_time in self.request_history 
                                if req_time > cutoff_time)
            
            return recent_requests / self.window_size
    
    def get_available_tokens(self) -> float:
        """Get number of available tokens.
        
        Returns:
            Available tokens
        """
        with self.lock:
            self._refill_tokens()
            return self.tokens
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get rate limiter statistics.
        
        Returns:
            Statistics dictionary
        """
        with self.lock:
            return {
                'total_requests': self.total_requests,
                'rejected_requests': self.rejected_requests,
                'current_rate': self.get_current_rate(),
                'available_tokens': self.tokens,
                'max_tokens': self.burst_limit,
                'rate_limit': self.rate_per_sec
            }
    
    def _refill_tokens(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_refill
        
        if elapsed > 0:
            tokens_to_add = elapsed * self.rate_per_sec
            self.tokens = min(self.burst_limit, self.tokens + tokens_to_add)
            self.last_refill = now
    
    def _record_request(self) -> None:
        """Record a request timestamp."""
        now = time.time()
        self.request_history.append(now)
        
        # Clean old requests outside the window
        cutoff_time = now - self.window_size
        while self.request_history and self.request_history[0] < cutoff_time:
            self.request_history.popleft()


class ConnectionPool:
    """Thread-safe connection pool for managing HTTP connections."""
    
    def __init__(self, max_connections: int = 20, timeout: float = 30.0):
        """Initialize connection pool.
        
        Args:
            max_connections: Maximum number of concurrent connections
            timeout: Connection timeout in seconds
        """
        self.max_connections = max_connections
        self.timeout = timeout
        
        self.semaphore = Semaphore(max_connections)
        self.active_connections = set()
        self.lock = Lock()
        
        self.logger = logging.getLogger(__name__)
    
    @contextmanager
    def acquire_connection(self):
        """Context manager for acquiring a connection."""
        if not self.semaphore.acquire(timeout=self.timeout):
            raise TimeoutError("Connection pool timeout")
        
        connection_id = id(threading.current_thread())
        
        try:
            with self.lock:
                self.active_connections.add(connection_id)
            
            yield connection_id
            
        finally:
            with self.lock:
                self.active_connections.discard(connection_id)
            
            self.semaphore.release()
    
    def get_active_count(self) -> int:
        """Get number of active connections."""
        with self.lock:
            return len(self.active_connections)
    
    def get_available_count(self) -> int:
        """Get number of available connection slots."""
        return self.max_connections - self.get_active_count()


class MemoryMonitor:
    """Monitor and manage memory usage."""
    
    def __init__(self, 
                 limit_mb: int = 2048,
                 check_interval: float = 10.0,
                 warning_threshold: float = 0.8,
                 critical_threshold: float = 0.95):
        """Initialize memory monitor.
        
        Args:
            limit_mb: Memory limit in megabytes
            check_interval: How often to check memory (seconds)
            warning_threshold: Warning threshold (0.0-1.0)
            critical_threshold: Critical threshold (0.0-1.0)
        """
        self.limit_bytes = limit_mb * 1024 * 1024
        self.check_interval = check_interval
        self.warning_threshold = warning_threshold
        self.critical_threshold = critical_threshold
        
        self.current_usage = 0
        self.peak_usage = 0
        self.warning_callbacks = []
        self.critical_callbacks = []
        
        self.lock = Lock()
        self.stop_event = Event()
        self.monitor_thread = Thread(target=self._monitor_loop, daemon=True)
        
        self.logger = logging.getLogger(__name__)
        self.monitor_thread.start()
    
    def get_current_usage(self) -> Tuple[int, float]:
        """Get current memory usage.
        
        Returns:
            Tuple of (bytes_used, percentage_used)
        """
        process = psutil.Process()
        memory_info = process.memory_info()
        usage_bytes = memory_info.rss
        usage_percent = usage_bytes / self.limit_bytes
        
        with self.lock:
            self.current_usage = usage_bytes
            self.peak_usage = max(self.peak_usage, usage_bytes)
        
        return usage_bytes, usage_percent
    
    def is_memory_available(self, required_bytes: int) -> bool:
        """Check if required memory is available.
        
        Args:
            required_bytes: Required memory in bytes
            
        Returns:
            True if memory is available
        """
        current_bytes, _ = self.get_current_usage()
        return (current_bytes + required_bytes) <= self.limit_bytes
    
    def add_warning_callback(self, callback) -> None:
        """Add callback for memory warnings."""
        self.warning_callbacks.append(callback)
    
    def add_critical_callback(self, callback) -> None:
        """Add callback for critical memory alerts."""
        self.critical_callbacks.append(callback)
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get memory statistics.
        
        Returns:
            Memory statistics dictionary
        """
        current_bytes, current_percent = self.get_current_usage()
        
        return {
            'current_bytes': current_bytes,
            'current_mb': current_bytes / 1024 / 1024,
            'current_percent': current_percent * 100,
            'peak_bytes': self.peak_usage,
            'peak_mb': self.peak_usage / 1024 / 1024,
            'limit_bytes': self.limit_bytes,
            'limit_mb': self.limit_bytes / 1024 / 1024,
            'available_bytes': self.limit_bytes - current_bytes,
            'available_mb': (self.limit_bytes - current_bytes) / 1024 / 1024
        }
    
    def shutdown(self) -> None:
        """Shutdown memory monitor."""
        self.stop_event.set()
        if self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5.0)
    
    def _monitor_loop(self) -> None:
        """Background monitoring loop."""
        while not self.stop_event.is_set():
            try:
                _, usage_percent = self.get_current_usage()
                
                if usage_percent >= self.critical_threshold:
                    for callback in self.critical_callbacks:
                        try:
                            callback(usage_percent)
                        except Exception as e:
                            self.logger.error(f"Error in critical memory callback: {e}")
                
                elif usage_percent >= self.warning_threshold:
                    for callback in self.warning_callbacks:
                        try:
                            callback(usage_percent)
                        except Exception as e:
                            self.logger.error(f"Error in warning memory callback: {e}")
                
                self.stop_event.wait(timeout=self.check_interval)
                
            except Exception as e:
                self.logger.error(f"Error in memory monitor loop: {e}")


class BackpressureManager:
    """Manage backpressure based on system resource utilization."""
    
    def __init__(self, 
                 threshold: float = 0.9,
                 delay: float = 1.0,
                 max_delay: float = 10.0):
        """Initialize backpressure manager.
        
        Args:
            threshold: Resource utilization threshold (0.0-1.0)
            delay: Base delay in seconds
            max_delay: Maximum delay in seconds
        """
        self.threshold = threshold
        self.base_delay = delay
        self.max_delay = max_delay
        
        self.active = False
        self.current_delay = 0.0
        self.lock = Lock()
        
        self.logger = logging.getLogger(__name__)
    
    def check_and_apply(self, 
                       cpu_percent: float,
                       memory_percent: float,
                       queue_size: int = 0,
                       max_queue_size: int = 100) -> bool:
        """Check if backpressure should be applied and apply delay.
        
        Args:
            cpu_percent: Current CPU utilization (0-100)
            memory_percent: Current memory utilization (0-100)
            queue_size: Current queue size
            max_queue_size: Maximum queue size
            
        Returns:
            True if backpressure was applied
        """
        # Calculate resource utilization
        cpu_utilization = cpu_percent / 100.0
        memory_utilization = memory_percent / 100.0
        queue_utilization = queue_size / max(max_queue_size, 1)
        
        # Find maximum utilization
        max_utilization = max(cpu_utilization, memory_utilization, queue_utilization)
        
        with self.lock:
            if max_utilization >= self.threshold:
                # Calculate delay based on how much we exceed threshold
                excess = max_utilization - self.threshold
                delay_multiplier = excess / (1.0 - self.threshold)
                self.current_delay = min(self.base_delay * (1 + delay_multiplier), self.max_delay)
                
                if not self.active:
                    self.logger.warning(f"Backpressure activated: {max_utilization:.2f} utilization")
                    self.active = True
                
                # Apply delay
                time.sleep(self.current_delay)
                return True
            else:
                if self.active:
                    self.logger.info("Backpressure deactivated")
                    self.active = False
                
                self.current_delay = 0.0
                return False
    
    def is_active(self) -> bool:
        """Check if backpressure is currently active."""
        with self.lock:
            return self.active
    
    def get_current_delay(self) -> float:
        """Get current backpressure delay."""
        with self.lock:
            return self.current_delay


class ResourceManager:
    """Comprehensive resource management for ETL orchestration."""
    
    def __init__(self, config: ResourceConfig):
        """Initialize resource manager.
        
        Args:
            config: Resource configuration
        """
        self.config = config
        self.config.validate()
        
        # Core components
        self.rate_limiter = RateLimiter(
            rate_per_sec=config.rate_limit_per_sec,
            burst_limit=config.burst_limit,
            window_size=config.rate_limit_window
        )
        
        self.connection_pool = ConnectionPool(
            max_connections=config.max_connections,
            timeout=config.connection_timeout
        )
        
        self.memory_monitor = MemoryMonitor(
            limit_mb=config.memory_limit_mb,
            check_interval=config.memory_check_interval,
            warning_threshold=config.memory_warning_threshold,
            critical_threshold=config.memory_critical_threshold
        )
        
        self.backpressure_manager = BackpressureManager(
            threshold=config.backpressure_threshold,
            delay=config.backpressure_delay
        ) if config.enable_backpressure else None
        
        # Request tracking
        self.request_semaphore = Semaphore(config.max_concurrent_requests)
        self.active_requests = set()
        self.request_lock = Lock()
        
        # Statistics
        self.stats = {
            'requests_made': 0,
            'requests_rejected': 0,
            'memory_warnings': 0,
            'memory_critical': 0,
            'backpressure_activations': 0
        }
        self.stats_lock = Lock()
        
        # Setup callbacks
        self.memory_monitor.add_warning_callback(self._on_memory_warning)
        self.memory_monitor.add_critical_callback(self._on_memory_critical)
        
        # Logging
        self.logger = logging.getLogger(__name__)
        
        self.logger.info("Resource manager initialized")
    
    @contextmanager
    def acquire_request_slot(self):
        """Context manager for acquiring a request slot with full resource management."""
        # Check and apply backpressure
        if self.backpressure_manager and self.config.enable_backpressure:
            cpu_percent = psutil.cpu_percent(interval=0.1)
            memory_bytes, memory_percent = self.memory_monitor.get_current_usage()
            memory_percent_val = memory_percent * 100
            
            if self.backpressure_manager.check_and_apply(cpu_percent, memory_percent_val):
                with self.stats_lock:
                    self.stats['backpressure_activations'] += 1
        
        # Acquire request slot
        if not self.request_semaphore.acquire(timeout=self.config.connection_timeout):
            with self.stats_lock:
                self.stats['requests_rejected'] += 1
            raise TimeoutError("Request slot acquisition timeout")
        
        request_id = id(threading.current_thread())
        
        try:
            with self.request_lock:
                self.active_requests.add(request_id)
            
            with self.stats_lock:
                self.stats['requests_made'] += 1
            
            # Acquire connection from pool
            with self.connection_pool.acquire_connection():
                yield request_id
        
        finally:
            with self.request_lock:
                self.active_requests.discard(request_id)
            
            self.request_semaphore.release()
    
    def wait_for_rate_limit(self, tokens: int = 1) -> None:
        """Wait for rate limit tokens to be available.
        
        Args:
            tokens: Number of tokens to wait for
        """
        self.rate_limiter.wait_for_tokens(tokens)
    
    def try_acquire_rate_limit(self, tokens: int = 1) -> bool:
        """Try to acquire rate limit tokens without waiting.
        
        Args:
            tokens: Number of tokens to acquire
            
        Returns:
            True if tokens acquired successfully
        """
        return self.rate_limiter.try_acquire(tokens)
    
    def is_rate_limited(self) -> bool:
        """Check if currently rate limited.
        
        Returns:
            True if rate limited
        """
        return self.rate_limiter.get_available_tokens() < 1
    
    def get_active_request_count(self) -> int:
        """Get number of active requests.
        
        Returns:
            Number of active requests
        """
        with self.request_lock:
            return len(self.active_requests)
    
    def get_memory_usage(self) -> float:
        """Get current memory usage in MB.
        
        Returns:
            Memory usage in megabytes
        """
        bytes_used, _ = self.memory_monitor.get_current_usage()
        return bytes_used / 1024 / 1024
    
    def is_memory_available(self, required_mb: float) -> bool:
        """Check if required memory is available.
        
        Args:
            required_mb: Required memory in megabytes
            
        Returns:
            True if memory is available
        """
        required_bytes = int(required_mb * 1024 * 1024)
        return self.memory_monitor.is_memory_available(required_bytes)
    
    def get_resource_utilization(self) -> Dict[str, Any]:
        """Get current resource utilization metrics.
        
        Returns:
            Resource utilization dictionary
        """
        # System metrics
        cpu_percent = psutil.cpu_percent(interval=0.1)
        memory_bytes, memory_percent = self.memory_monitor.get_current_usage()
        
        # Request metrics
        active_requests = self.get_active_request_count()
        available_requests = self.config.max_concurrent_requests - active_requests
        
        # Rate limiting metrics
        rate_stats = self.rate_limiter.get_statistics()
        
        # Connection pool metrics
        active_connections = self.connection_pool.get_active_count()
        available_connections = self.connection_pool.get_available_count()
        
        return {
            'timestamp': datetime.now().isoformat(),
            'cpu': {
                'percent': cpu_percent,
                'status': 'critical' if cpu_percent > 90 else 'warning' if cpu_percent > 80 else 'ok'
            },
            'memory': {
                'bytes': memory_bytes,
                'mb': memory_bytes / 1024 / 1024,
                'percent': memory_percent * 100,
                'limit_mb': self.config.memory_limit_mb,
                'available_mb': (self.config.memory_limit_mb * 1024 * 1024 - memory_bytes) / 1024 / 1024,
                'status': 'critical' if memory_percent > 0.95 else 'warning' if memory_percent > 0.8 else 'ok'
            },
            'requests': {
                'active': active_requests,
                'available': available_requests,
                'max': self.config.max_concurrent_requests,
                'utilization_percent': (active_requests / self.config.max_concurrent_requests) * 100
            },
            'rate_limiting': {
                'current_rate': rate_stats['current_rate'],
                'limit_per_sec': self.config.rate_limit_per_sec,
                'available_tokens': rate_stats['available_tokens'],
                'total_requests': rate_stats['total_requests'],
                'rejected_requests': rate_stats['rejected_requests']
            },
            'connections': {
                'active': active_connections,
                'available': available_connections,
                'max': self.config.max_connections,
                'utilization_percent': (active_connections / self.config.max_connections) * 100
            },
            'backpressure': {
                'active': self.backpressure_manager.is_active() if self.backpressure_manager else False,
                'current_delay': self.backpressure_manager.get_current_delay() if self.backpressure_manager else 0.0
            }
        }
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get comprehensive resource management statistics.
        
        Returns:
            Statistics dictionary
        """
        with self.stats_lock:
            stats = self.stats.copy()
        
        rate_stats = self.rate_limiter.get_statistics()
        memory_stats = self.memory_monitor.get_statistics()
        
        return {
            'resource_manager': stats,
            'rate_limiting': rate_stats,
            'memory': memory_stats,
            'utilization': self.get_resource_utilization()
        }
    
    @property
    def current_rate_limit(self) -> float:
        """Get current rate limit setting."""
        return self.config.rate_limit_per_sec
    
    def update_rate_limit(self, new_rate: float) -> None:
        """Update rate limit setting.
        
        Args:
            new_rate: New rate limit per second
        """
        if new_rate <= 0 or new_rate > 10:
            raise ValueError("Rate limit must be between 0 and 10 requests per second")
        
        self.config.rate_limit_per_sec = new_rate
        
        # Create new rate limiter with updated settings
        old_stats = self.rate_limiter.get_statistics()
        self.rate_limiter = RateLimiter(
            rate_per_sec=new_rate,
            burst_limit=self.config.burst_limit,
            window_size=self.config.rate_limit_window
        )
        
        # Preserve statistics
        self.rate_limiter.total_requests = old_stats['total_requests']
        self.rate_limiter.rejected_requests = old_stats['rejected_requests']
        
        self.logger.info(f"Updated rate limit to {new_rate} requests per second")
    
    def shutdown(self) -> None:
        """Shutdown resource manager."""
        self.logger.info("Shutting down resource manager...")
        
        # Shutdown memory monitor
        self.memory_monitor.shutdown()
        
        # Log final statistics
        final_stats = self.get_statistics()
        self.logger.info(f"Final resource statistics: {final_stats}")
        
        self.logger.info("Resource manager shutdown complete")
    
    def _on_memory_warning(self, usage_percent: float) -> None:
        """Handle memory warning callback."""
        with self.stats_lock:
            self.stats['memory_warnings'] += 1
        
        self.logger.warning(f"Memory usage warning: {usage_percent:.1f}%")
    
    def _on_memory_critical(self, usage_percent: float) -> None:
        """Handle memory critical callback."""
        with self.stats_lock:
            self.stats['memory_critical'] += 1
        
        self.logger.critical(f"Critical memory usage: {usage_percent:.1f}%")