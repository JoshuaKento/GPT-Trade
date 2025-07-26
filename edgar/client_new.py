"""Enhanced SEC EDGAR API client with rate limiting and proper resource management."""
import os
import time
import threading
from typing import Dict, Optional, Any
from dataclasses import dataclass
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@dataclass
class ClientConfig:
    """Configuration for EDGAR client."""
    rate_limit_per_sec: float = 6.0
    timeout: int = 30
    user_agent: str = "GPT-Trade-Agent (your-email@example.com)"
    max_retries: int = 3
    backoff_factor: float = 0.5


class EdgarClient:
    """Thread-safe SEC EDGAR API client with rate limiting and retry logic."""
    
    SEC_BASE = "https://data.sec.gov"
    SEC_ARCHIVES = "https://www.sec.gov/Archives"
    
    def __init__(self, config: Optional[ClientConfig] = None):
        self.config = config or ClientConfig()
        
        # Use environment variable for user agent if available
        if "SEC_USER_AGENT" in os.environ:
            self.config.user_agent = os.environ["SEC_USER_AGENT"]
        
        # Rate limiting state
        self._last_request = 0.0
        self._lock = threading.Lock()
        
        # Create session with retry strategy
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": self.config.user_agent})
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)
    
    def get(self, url: str, **kwargs) -> requests.Response:
        """GET request with automatic rate limiting and error handling."""
        # Apply rate limiting
        with self._lock:
            interval = 1.0 / self.config.rate_limit_per_sec
            wait_time = interval - (time.time() - self._last_request)
            if wait_time > 0:
                time.sleep(wait_time)
            self._last_request = time.time()
        
        # Set default timeout
        kwargs.setdefault('timeout', self.config.timeout)
        
        try:
            response = self._session.get(url, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Failed to fetch {url}: {e}")
    
    def get_json(self, url: str, **kwargs) -> Dict[str, Any]:
        """GET request returning JSON data."""
        response = self.get(url, **kwargs)
        try:
            return response.json()
        except ValueError as e:
            raise ValueError(f"Invalid JSON response from {url}: {e}")
    
    def close(self):
        """Clean up session resources."""
        if hasattr(self, '_session'):
            self._session.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()