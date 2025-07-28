"""Enhanced error handling and retry logic for database operations."""

import functools
import logging
import random
import time
from contextlib import contextmanager
from typing import Any, Callable, Generator, List, Optional, Tuple, Type, TypeVar, Union

try:
    from sqlalchemy.exc import (
        DatabaseError as SQLDatabaseError,
        DisconnectionError,
        IntegrityError,
        OperationalError,
        StatementError,
        TimeoutError as SQLTimeoutError,
    )
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False

from .database_config import DatabaseConfig


F = TypeVar('F', bound=Callable[..., Any])


class DatabaseOperationError(Exception):
    """Base exception for database operation errors."""
    
    def __init__(self, message: str, original_error: Optional[Exception] = None):
        super().__init__(message)
        self.original_error = original_error


class ConnectionPoolExhaustedError(DatabaseOperationError):
    """Raised when the connection pool is exhausted."""
    pass


class TransactionTimeoutError(DatabaseOperationError):
    """Raised when a transaction times out."""
    pass


class DataIntegrityError(DatabaseOperationError):
    """Raised when data integrity constraints are violated."""
    pass


class RetryableError(DatabaseOperationError):
    """Base class for errors that can be retried."""
    pass


class NonRetryableError(DatabaseOperationError):
    """Base class for errors that should not be retried."""
    pass


class ErrorClassifier:
    """Classifies database errors to determine retry behavior."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def classify_error(self, error: Exception) -> Tuple[bool, str]:
        """Classify an error to determine if it's retryable.
        
        Args:
            error: Exception to classify
            
        Returns:
            Tuple of (is_retryable, error_category)
        """
        if not SQLALCHEMY_AVAILABLE:
            return False, "unknown"

        error_type = type(error)
        error_message = str(error).lower()

        # Connection-related errors (usually retryable)
        if isinstance(error, (DisconnectionError, OperationalError)):
            if any(keyword in error_message for keyword in [
                'connection', 'network', 'timeout', 'refused', 'reset',
                'broken pipe', 'lost connection', 'server has gone away'
            ]):
                return True, "connection"

        # Pool exhaustion (retryable with backoff)
        if 'pool' in error_message and 'exhausted' in error_message:
            return True, "pool_exhausted"

        # Lock timeout (retryable)
        if any(keyword in error_message for keyword in [
            'lock timeout', 'deadlock', 'lock wait timeout'
        ]):
            return True, "lock_timeout"

        # Timeout errors (retryable)
        if isinstance(error, SQLTimeoutError) or 'timeout' in error_message:
            return True, "timeout"

        # Integrity constraint violations (not retryable)
        if isinstance(error, IntegrityError):
            return False, "integrity"

        # Statement errors (usually not retryable)
        if isinstance(error, StatementError):
            return False, "statement"

        # Authentication/authorization errors (not retryable)
        if any(keyword in error_message for keyword in [
            'authentication', 'authorization', 'access denied', 'permission'
        ]):
            return False, "auth"

        # Syntax errors (not retryable)
        if any(keyword in error_message for keyword in [
            'syntax error', 'invalid syntax', 'parse error'
        ]):
            return False, "syntax"

        # Default to non-retryable for unknown errors
        return False, "unknown"


class RetryConfig:
    """Configuration for retry behavior."""

    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
        retryable_errors: Optional[List[Type[Exception]]] = None
    ):
        """Initialize retry configuration.
        
        Args:
            max_attempts: Maximum number of retry attempts
            base_delay: Base delay between retries in seconds
            max_delay: Maximum delay between retries in seconds
            exponential_base: Base for exponential backoff
            jitter: Whether to add random jitter to delays
            retryable_errors: List of error types that should be retried
        """
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter
        self.retryable_errors = retryable_errors or []

    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for a given attempt.
        
        Args:
            attempt: Current attempt number (1-based)
            
        Returns:
            Delay in seconds
        """
        # Exponential backoff
        delay = self.base_delay * (self.exponential_base ** (attempt - 1))
        
        # Cap at max delay
        delay = min(delay, self.max_delay)
        
        # Add jitter to prevent thundering herd
        if self.jitter:
            jitter_amount = delay * 0.1  # 10% jitter
            delay += random.uniform(-jitter_amount, jitter_amount)
        
        return max(0, delay)


class DatabaseRetryHandler:
    """Handles retry logic for database operations."""

    def __init__(self, config: DatabaseConfig):
        """Initialize retry handler.
        
        Args:
            config: Database configuration
        """
        self.config = config
        self.error_classifier = ErrorClassifier()
        self.logger = logging.getLogger(__name__)
        
        # Default retry configuration
        self.retry_config = RetryConfig(
            max_attempts=config.connect_retries + 1,
            base_delay=config.connect_retry_delay,
            exponential_base=config.connect_retry_backoff,
        )

    def with_retry(
        self,
        retry_config: Optional[RetryConfig] = None,
        error_handler: Optional[Callable[[Exception, int], None]] = None
    ) -> Callable[[F], F]:
        """Decorator that adds retry logic to a function.
        
        Args:
            retry_config: Optional custom retry configuration
            error_handler: Optional custom error handler
            
        Returns:
            Decorated function with retry logic
        """
        config = retry_config or self.retry_config
        
        def decorator(func: F) -> F:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                return self._execute_with_retry(
                    func, args, kwargs, config, error_handler
                )
            return wrapper
        return decorator

    def execute_with_retry(
        self,
        func: Callable,
        *args,
        retry_config: Optional[RetryConfig] = None,
        error_handler: Optional[Callable[[Exception, int], None]] = None,
        **kwargs
    ) -> Any:
        """Execute a function with retry logic.
        
        Args:
            func: Function to execute
            *args: Function positional arguments
            retry_config: Optional custom retry configuration
            error_handler: Optional custom error handler
            **kwargs: Function keyword arguments
            
        Returns:
            Function result
        """
        config = retry_config or self.retry_config
        return self._execute_with_retry(
            func, args, kwargs, config, error_handler
        )

    def _execute_with_retry(
        self,
        func: Callable,
        args: tuple,
        kwargs: dict,
        config: RetryConfig,
        error_handler: Optional[Callable[[Exception, int], None]]
    ) -> Any:
        """Internal method to execute function with retry logic.
        
        Args:
            func: Function to execute
            args: Function positional arguments
            kwargs: Function keyword arguments
            config: Retry configuration
            error_handler: Optional custom error handler
            
        Returns:
            Function result
            
        Raises:
            DatabaseOperationError: If all retry attempts fail
        """
        last_error = None
        
        for attempt in range(1, config.max_attempts + 1):
            try:
                result = func(*args, **kwargs)
                
                # Log successful retry
                if attempt > 1:
                    self.logger.info(
                        f"Function {func.__name__} succeeded on attempt {attempt}"
                    )
                
                return result
                
            except Exception as error:
                last_error = error
                
                # Classify error
                is_retryable, error_category = self.error_classifier.classify_error(error)
                
                # Check if we should retry
                if not is_retryable or attempt >= config.max_attempts:
                    # Convert to appropriate exception type
                    if error_category == "connection":
                        raise DatabaseOperationError(
                            f"Connection error in {func.__name__}: {error}",
                            original_error=error
                        ) from error
                    elif error_category == "pool_exhausted":
                        raise ConnectionPoolExhaustedError(
                            f"Connection pool exhausted in {func.__name__}: {error}",
                            original_error=error
                        ) from error
                    elif error_category == "timeout":
                        raise TransactionTimeoutError(
                            f"Timeout in {func.__name__}: {error}",
                            original_error=error
                        ) from error
                    elif error_category == "integrity":
                        raise DataIntegrityError(
                            f"Data integrity error in {func.__name__}: {error}",
                            original_error=error
                        ) from error
                    else:
                        raise DatabaseOperationError(
                            f"Database error in {func.__name__}: {error}",
                            original_error=error
                        ) from error
                
                # Calculate delay for next attempt
                delay = config.calculate_delay(attempt)
                
                # Log retry attempt
                self.logger.warning(
                    f"Attempt {attempt} of {func.__name__} failed "
                    f"({error_category}): {error}. "
                    f"Retrying in {delay:.2f} seconds..."
                )
                
                # Call custom error handler if provided
                if error_handler:
                    try:
                        error_handler(error, attempt)
                    except Exception as handler_error:
                        self.logger.error(
                            f"Error handler failed: {handler_error}"
                        )
                
                # Wait before retry
                if delay > 0:
                    time.sleep(delay)
        
        # This should never be reached, but just in case
        raise DatabaseOperationError(
            f"Function {func.__name__} failed after {config.max_attempts} attempts",
            original_error=last_error
        )


class CircuitBreaker:
    """Circuit breaker pattern for database operations."""

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        expected_exception: Type[Exception] = Exception
    ):
        """Initialize circuit breaker.
        
        Args:
            failure_threshold: Number of failures before opening circuit
            recovery_timeout: Time to wait before trying to close circuit
            expected_exception: Exception type to monitor
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "closed"  # closed, open, half-open
        
        self.logger = logging.getLogger(__name__)

    def __call__(self, func: F) -> F:
        """Decorator that applies circuit breaker pattern.
        
        Args:
            func: Function to protect with circuit breaker
            
        Returns:
            Decorated function
        """
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return self._execute_with_circuit_breaker(func, args, kwargs)
        return wrapper

    def _execute_with_circuit_breaker(
        self,
        func: Callable,
        args: tuple,
        kwargs: dict
    ) -> Any:
        """Execute function with circuit breaker protection.
        
        Args:
            func: Function to execute
            args: Function positional arguments
            kwargs: Function keyword arguments
            
        Returns:
            Function result
            
        Raises:
            DatabaseOperationError: If circuit is open
        """
        # Check circuit state
        if self.state == "open":
            if self._should_attempt_reset():
                self.state = "half-open"
                self.logger.info("Circuit breaker entering half-open state")
            else:
                raise DatabaseOperationError(
                    f"Circuit breaker is open for {func.__name__}. "
                    f"Last failure: {self.last_failure_time}"
                )
        
        try:
            result = func(*args, **kwargs)
            
            # Success - reset circuit if it was half-open
            if self.state == "half-open":
                self._reset()
                self.logger.info("Circuit breaker reset to closed state")
            
            return result
            
        except self.expected_exception as error:
            self._record_failure()
            
            if self.state == "half-open":
                self.state = "open"
                self.logger.warning("Circuit breaker re-opened due to failure")
            elif self.failure_count >= self.failure_threshold:
                self.state = "open"
                self.logger.warning(
                    f"Circuit breaker opened after {self.failure_count} failures"
                )
            
            raise

    def _should_attempt_reset(self) -> bool:
        """Check if circuit breaker should attempt to reset.
        
        Returns:
            True if enough time has passed to attempt reset
        """
        if self.last_failure_time is None:
            return True
        
        return (time.time() - self.last_failure_time) >= self.recovery_timeout

    def _record_failure(self) -> None:
        """Record a failure."""
        self.failure_count += 1
        self.last_failure_time = time.time()

    def _reset(self) -> None:
        """Reset circuit breaker to closed state."""
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "closed"

    def get_state(self) -> dict:
        """Get current circuit breaker state.
        
        Returns:
            Dictionary containing circuit breaker state
        """
        return {
            "state": self.state,
            "failure_count": self.failure_count,
            "last_failure_time": self.last_failure_time,
            "failure_threshold": self.failure_threshold,
            "recovery_timeout": self.recovery_timeout,
        }


@contextmanager
def error_context(
    operation_name: str,
    reraise_as: Optional[Type[Exception]] = None,
    logger: Optional[logging.Logger] = None
) -> Generator[None, None, None]:
    """Context manager for consistent error handling and logging.
    
    Args:
        operation_name: Name of the operation for logging
        reraise_as: Optional exception type to reraise as
        logger: Optional logger instance
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    try:
        yield
    except Exception as error:
        logger.error(f"Error in {operation_name}: {error}")
        
        if reraise_as:
            raise reraise_as(f"Failed {operation_name}: {error}") from error
        else:
            raise


def create_error_handler(
    error_mapping: Optional[dict] = None,
    default_handler: Optional[Callable[[Exception], Exception]] = None
) -> Callable[[Exception], Exception]:
    """Create a custom error handler function.
    
    Args:
        error_mapping: Dictionary mapping error types to handler functions
        default_handler: Default handler for unmapped errors
        
    Returns:
        Error handler function
    """
    mapping = error_mapping or {}
    
    def handler(error: Exception) -> Exception:
        error_type = type(error)
        
        if error_type in mapping:
            return mapping[error_type](error)
        elif default_handler:
            return default_handler(error)
        else:
            return DatabaseOperationError(
                f"Unhandled database error: {error}",
                original_error=error
            )
    
    return handler


class DatabaseErrorReporter:
    """Collects and reports database error statistics."""

    def __init__(self):
        self.error_counts = {}
        self.error_history = []
        self.logger = logging.getLogger(__name__)

    def record_error(self, error: Exception, context: str = "") -> None:
        """Record an error occurrence.
        
        Args:
            error: Exception that occurred
            context: Optional context information
        """
        error_type = type(error).__name__
        self.error_counts[error_type] = self.error_counts.get(error_type, 0) + 1
        
        self.error_history.append({
            "timestamp": time.time(),
            "error_type": error_type,
            "message": str(error),
            "context": context
        })
        
        # Keep only recent errors (last 1000)
        if len(self.error_history) > 1000:
            self.error_history = self.error_history[-1000:]

    def get_error_summary(self) -> dict:
        """Get summary of recorded errors.
        
        Returns:
            Dictionary containing error statistics
        """
        return {
            "total_errors": sum(self.error_counts.values()),
            "error_counts": self.error_counts.copy(),
            "recent_errors": self.error_history[-10:],  # Last 10 errors
        }

    def clear_errors(self) -> None:
        """Clear all recorded errors."""
        self.error_counts.clear()
        self.error_history.clear()
        self.logger.info("Error history cleared")