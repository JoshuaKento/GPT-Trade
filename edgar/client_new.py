"""Enhanced SEC EDGAR API client with rate limiting and proper resource management."""

import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
from urllib3.util.retry import Retry


class OptimizedHTTPAdapter(HTTPAdapter):
    """HTTP adapter with optimized connection pooling and keep-alive settings."""

    def __init__(self, config: "ClientConfig", *args, **kwargs):
        self.config = config
        super().__init__(*args, **kwargs)

    def init_poolmanager(self, *args, **kwargs):
        """Initialize pool manager with optimized settings."""
        kwargs.update(
            {
                "num_pools": self.config.pool_connections,
                "maxsize": self.config.pool_maxsize,
                "block": False,
            }
        )

        if self.config.socket_options:
            # Enable TCP keep-alive
            import socket

            kwargs["socket_options"] = [
                (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
                (
                    socket.IPPROTO_TCP,
                    socket.TCP_KEEPIDLE,
                    self.config.keepalive_timeout,
                ),
                (socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10),
                (socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 6),
            ]

        return super().init_poolmanager(*args, **kwargs)


class SyncAdaptiveRateLimiter:
    """Synchronous adaptive rate limiter for the sync client."""

    def __init__(
        self, initial_rate: float, min_rate: float = 1.0, max_rate: float = 10.0
    ):
        self._initial_rate = initial_rate
        self._current_rate = initial_rate
        self._min_rate = min_rate
        self._max_rate = max_rate
        self._last_request = 0.0
        self._consecutive_successes = 0
        self._consecutive_failures = 0
        self._last_adjustment = 0
        self._adjustment_cooldown = 30  # seconds

    def wait_if_needed(self) -> None:
        """Wait if needed based on current rate limit."""
        interval = 1.0 / self._current_rate
        wait_time = interval - (time.time() - self._last_request)
        if wait_time > 0:
            time.sleep(wait_time)
        self._last_request = time.time()

    def report_success(self) -> None:
        """Report a successful request."""
        self._consecutive_successes += 1
        self._consecutive_failures = 0

        if (
            self._consecutive_successes >= 20
            and self._current_rate < self._max_rate
            and self._should_adjust()
        ):
            self._increase_rate()

    def report_failure(self, status_code: int = None) -> None:
        """Report a failed request."""
        self._consecutive_failures += 1
        self._consecutive_successes = 0

        if (
            status_code in [429, 503, 504] or self._consecutive_failures >= 3
        ) and self._should_adjust():
            self._decrease_rate()

    def _should_adjust(self) -> bool:
        """Check if enough time has passed since last adjustment."""
        current_time = time.time()
        if current_time - self._last_adjustment >= self._adjustment_cooldown:
            self._last_adjustment = current_time
            return True
        return False

    def _increase_rate(self) -> None:
        """Increase the rate limit."""
        old_rate = self._current_rate
        self._current_rate = min(self._current_rate + 0.5, self._max_rate)
        if self._current_rate != old_rate:
            print(f"Increased rate limit from {old_rate} to {self._current_rate}")

    def _decrease_rate(self) -> None:
        """Decrease the rate limit."""
        old_rate = self._current_rate
        self._current_rate = max(self._current_rate - 0.5, self._min_rate)
        if self._current_rate != old_rate:
            print(f"Decreased rate limit from {old_rate} to {self._current_rate}")

    @property
    def current_rate(self) -> float:
        """Get current rate limit."""
        return self._current_rate


@dataclass
class ClientConfig:
    """Configuration for EDGAR client."""

    rate_limit_per_sec: float = 6.0
    timeout: int = 30
    user_agent: str = ""  # Must be set via environment variable or explicitly
    max_retries: int = 3
    backoff_factor: float = 0.5
    # Connection pooling settings
    pool_connections: int = 50
    pool_maxsize: int = 100
    # Keep-alive settings
    socket_options: bool = True
    keepalive_timeout: int = 30
    # Adaptive rate limiting
    adaptive_rate_limiting: bool = True
    min_rate: float = 1.0
    max_rate: float = 10.0

    def __post_init__(self):
        """Validate configuration after initialization."""
        self._validate_user_agent()

    def _validate_user_agent(self) -> None:
        """Validate user agent for security and SEC compliance."""
        if not self.user_agent or not self.user_agent.strip():
            raise ValueError(
                "user_agent is required by SEC. Please set SEC_USER_AGENT environment variable "
                "or provide user_agent with format: 'AppName (contact@email.com)'"
            )

        # Validate email presence and format
        if "@" not in self.user_agent:
            raise ValueError(
                "SEC requires contact email in User-Agent header. "
                "Format: 'AppName (contact@email.com)'. "
                f"Current value: {self.user_agent}"
            )

        # Check for placeholder emails that could expose system information
        placeholder_patterns = [
            "your-email@example.com",
            "example@example.com",
            "test@test.com",
            "user@domain.com",
            "email@example.org",
            "contact@yourcompany.com",
        ]

        user_agent_lower = self.user_agent.lower()
        for pattern in placeholder_patterns:
            if pattern in user_agent_lower:
                raise ValueError(
                    f"Placeholder email detected in user_agent: '{pattern}'. "
                    "Please provide a real contact email address."
                )

        # Basic email format validation
        import re

        email_pattern = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
        if not re.search(email_pattern, self.user_agent):
            raise ValueError(
                "Invalid email format in user_agent. "
                "Please ensure a valid email address is included."
            )

    # Connection pooling settings
    pool_connections: int = 50
    pool_maxsize: int = 100
    # Keep-alive settings
    socket_options: bool = True
    keepalive_timeout: int = 30


class EdgarClient:
    """Thread-safe SEC EDGAR API client with rate limiting and retry logic."""

    SEC_BASE = "https://data.sec.gov"
    SEC_ARCHIVES = "https://www.sec.gov/Archives"

    def __init__(self, config: Optional[ClientConfig] = None):
        # Use environment variable for user agent if available
        user_agent = os.environ.get("SEC_USER_AGENT", "")

        if config is None:
            if user_agent:
                self.config = ClientConfig(user_agent=user_agent)
            else:
                raise ValueError(
                    "No configuration provided and SEC_USER_AGENT environment variable not set. "
                    "Please set SEC_USER_AGENT or provide ClientConfig with valid user_agent."
                )
        else:
            self.config = config
            # Override with environment variable if set
            if user_agent:
                self.config.user_agent = user_agent

        # Rate limiting state
        self._lock = threading.Lock()

        # Initialize adaptive rate limiter if enabled
        if self.config.adaptive_rate_limiting:
            self._rate_limiter = SyncAdaptiveRateLimiter(
                initial_rate=self.config.rate_limit_per_sec,
                min_rate=self.config.min_rate,
                max_rate=self.config.max_rate,
            )
        else:
            self._rate_limiter = None
            self._last_request = 0.0

        # Create session with retry strategy
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": self.config.user_agent})

        # Configure retry strategy
        retry_strategy = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            # Add connection error retries
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            raise_on_status=False,
        )

        # Use optimized adapter with connection pooling
        adapter = OptimizedHTTPAdapter(
            config=self.config,
            max_retries=retry_strategy,
            pool_connections=self.config.pool_connections,
            pool_maxsize=self.config.pool_maxsize,
        )
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)

    def get(self, url: str, **kwargs) -> requests.Response:
        """GET request with adaptive rate limiting and error handling."""
        # Apply rate limiting
        with self._lock:
            if self._rate_limiter:
                self._rate_limiter.wait_if_needed()
            else:
                # Fallback to simple rate limiting
                interval = 1.0 / self.config.rate_limit_per_sec
                wait_time = interval - (time.time() - self._last_request)
                if wait_time > 0:
                    time.sleep(wait_time)
                self._last_request = time.time()

        # Set default timeout
        kwargs.setdefault("timeout", self.config.timeout)

        try:
            response = self._session.get(url, **kwargs)

            # Report to adaptive rate limiter
            if self._rate_limiter:
                if response.status_code == 200:
                    self._rate_limiter.report_success()
                else:
                    self._rate_limiter.report_failure(response.status_code)

            response.raise_for_status()
            return response

        except requests.exceptions.RequestException as e:
            # Report failure to adaptive rate limiter
            if self._rate_limiter:
                status_code = (
                    getattr(e.response, "status_code", None)
                    if hasattr(e, "response")
                    else None
                )
                self._rate_limiter.report_failure(status_code)
            raise RuntimeError(f"Failed to fetch {url}: {e}")

    def get_json(self, url: str, **kwargs) -> Dict[str, Any]:
        """GET request returning JSON data."""
        response = self.get(url, **kwargs)
        try:
            return response.json()
        except ValueError as e:
            raise ValueError(f"Invalid JSON response from {url}: {e}")

    def get_stream(
        self, url: str, chunk_size: int = 8192, **kwargs
    ) -> requests.Response:
        """GET request with streaming response for large files.

        Args:
            url: URL to request
            chunk_size: Size of chunks to read
            **kwargs: Additional arguments for the request

        Returns:
            Response object configured for streaming
        """
        # Apply rate limiting
        with self._lock:
            if self._rate_limiter:
                self._rate_limiter.wait_if_needed()
            else:
                # Fallback to simple rate limiting
                interval = 1.0 / self.config.rate_limit_per_sec
                wait_time = interval - (time.time() - self._last_request)
                if wait_time > 0:
                    time.sleep(wait_time)
                self._last_request = time.time()

        # Set default timeout and enable streaming
        kwargs.setdefault("timeout", self.config.timeout)
        kwargs["stream"] = True

        try:
            response = self._session.get(url, **kwargs)

            # Report to adaptive rate limiter
            if self._rate_limiter:
                if response.status_code == 200:
                    self._rate_limiter.report_success()
                else:
                    self._rate_limiter.report_failure(response.status_code)

            response.raise_for_status()
            return response

        except requests.exceptions.RequestException as e:
            # Report failure to adaptive rate limiter
            if self._rate_limiter:
                status_code = (
                    getattr(e.response, "status_code", None)
                    if hasattr(e, "response")
                    else None
                )
                self._rate_limiter.report_failure(status_code)
            raise RuntimeError(f"Failed to fetch {url}: {e}")

    def close(self):
        """Clean up session resources."""
        if hasattr(self, "_session"):
            self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
