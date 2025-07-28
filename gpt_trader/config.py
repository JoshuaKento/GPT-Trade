"""
Configuration management for GPT Trader platform.

This module provides comprehensive configuration management for the ETL pipeline,
database connections, and processing parameters.
"""

import os
import json
import logging
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from pathlib import Path

from edgar.config_manager import EdgarConfig

logger = logging.getLogger(__name__)


@dataclass
class DatabaseConfig:
    """Database configuration settings."""
    url: str = ""
    pool_size: int = 20
    max_overflow: int = 30
    pool_timeout: int = 30
    pool_recycle: int = 3600
    echo_sql: bool = False
    
    def __post_init__(self):
        if not self.url:
            self.url = self._get_default_url()
    
    def _get_default_url(self) -> str:
        """Get default database URL from environment."""
        if os.getenv("DATABASE_URL"):
            return os.getenv("DATABASE_URL")
        
        pg_host = os.getenv("POSTGRES_HOST")
        if pg_host:
            pg_user = os.getenv("POSTGRES_USER", "postgres")
            pg_password = os.getenv("POSTGRES_PASSWORD", "")
            pg_db = os.getenv("POSTGRES_DB", "gpt_trader")
            pg_port = os.getenv("POSTGRES_PORT", "5432")
            return f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
        
        return "sqlite:///gpt_trader.db"


@dataclass
class ETLConfig:
    """ETL pipeline configuration settings."""
    batch_size: int = 50
    max_concurrent_jobs: int = 10
    job_timeout_minutes: int = 30
    retry_attempts: int = 3
    retry_delay_seconds: int = 60
    enable_progress_tracking: bool = True
    enable_performance_monitoring: bool = True
    
    # SLA settings
    sla_processing_time_minutes: int = 30
    sla_success_rate_threshold: float = 0.95
    
    # Resource limits
    max_memory_mb: int = 2048
    max_disk_usage_gb: int = 10


@dataclass
class S3Config:
    """S3 storage configuration settings."""
    bucket_name: str = ""
    region: str = "us-east-1"
    key_prefix: str = "gpt-trader"
    multipart_threshold_mb: int = 100
    max_concurrent_uploads: int = 10
    enable_encryption: bool = True
    
    def __post_init__(self):
        if not self.bucket_name:
            self.bucket_name = os.getenv("S3_BUCKET", "gpt-trader-data")


@dataclass
class MonitoringConfig:
    """Monitoring and observability configuration."""
    enable_metrics: bool = True
    metrics_port: int = 8080
    log_level: str = "INFO"
    enable_structured_logging: bool = True
    enable_performance_profiling: bool = False
    
    # Alert thresholds
    alert_failure_rate_threshold: float = 0.1
    alert_processing_time_threshold_minutes: int = 35
    alert_memory_usage_threshold_mb: int = 1500


@dataclass
class TickerConfig:
    """Ticker and company configuration."""
    ticker: str
    cik: str
    priority: int = 1
    is_active: bool = True
    forms_to_process: List[str] = field(default_factory=lambda: ["10-K", "10-Q", "8-K"])
    max_historical_days: int = 365
    
    def __post_init__(self):
        # Ensure CIK is properly formatted
        if self.cik:
            self.cik = self.cik.zfill(10)


@dataclass  
class GPTTraderConfig:
    """Main configuration class for GPT Trader platform."""
    
    # Core configurations
    edgar: EdgarConfig = field(default_factory=EdgarConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    etl: ETLConfig = field(default_factory=ETLConfig)
    s3: S3Config = field(default_factory=S3Config)
    monitoring: MonitoringConfig = field(default_factory=MonitoringConfig)
    
    # Ticker configurations
    tickers: List[TickerConfig] = field(default_factory=list)
    
    # Environment settings
    environment: str = "development"
    debug: bool = False
    
    def __post_init__(self):
        self._load_environment_overrides()
        self._validate_config()
    
    def _load_environment_overrides(self):
        """Load configuration overrides from environment variables."""
        # Environment
        self.environment = os.getenv("ENVIRONMENT", self.environment)
        self.debug = os.getenv("DEBUG", "false").lower() == "true"
        
        # Database overrides
        if os.getenv("DATABASE_POOL_SIZE"):
            self.database.pool_size = int(os.getenv("DATABASE_POOL_SIZE"))
        if os.getenv("DATABASE_ECHO"):
            self.database.echo_sql = os.getenv("DATABASE_ECHO").lower() == "true"
        
        # ETL overrides
        if os.getenv("ETL_BATCH_SIZE"):
            self.etl.batch_size = int(os.getenv("ETL_BATCH_SIZE"))
        if os.getenv("ETL_MAX_CONCURRENT"):
            self.etl.max_concurrent_jobs = int(os.getenv("ETL_MAX_CONCURRENT"))
        
        # S3 overrides
        if os.getenv("S3_BUCKET"):
            self.s3.bucket_name = os.getenv("S3_BUCKET")
        if os.getenv("S3_REGION"):
            self.s3.region = os.getenv("S3_REGION")
        
        # Monitoring overrides
        if os.getenv("LOG_LEVEL"):
            self.monitoring.log_level = os.getenv("LOG_LEVEL")
    
    def _validate_config(self):
        """Validate configuration settings."""
        errors = []
        
        # Validate ETL settings
        if self.etl.batch_size <= 0:
            errors.append("ETL batch_size must be positive")
        if self.etl.max_concurrent_jobs <= 0:
            errors.append("ETL max_concurrent_jobs must be positive")
        if self.etl.sla_processing_time_minutes <= 0:
            errors.append("ETL SLA processing time must be positive")
        
        # Validate S3 settings
        if not self.s3.bucket_name:
            errors.append("S3 bucket_name is required")
        
        # Validate database URL
        if not self.database.url:
            errors.append("Database URL is required")
        
        if errors:
            raise ValueError(f"Configuration validation failed: {'; '.join(errors)}")
    
    @classmethod
    def from_file(cls, config_path: str) -> 'GPTTraderConfig':
        """Load configuration from JSON file."""
        try:
            with open(config_path, 'r') as f:
                config_data = json.load(f)
            
            # Convert ticker data to TickerConfig objects
            if 'tickers' in config_data:
                tickers = []
                for ticker_data in config_data['tickers']:
                    tickers.append(TickerConfig(**ticker_data))
                config_data['tickers'] = tickers
            
            # Create nested config objects
            if 'edgar' in config_data:
                config_data['edgar'] = EdgarConfig(**config_data['edgar'])
            if 'database' in config_data:
                config_data['database'] = DatabaseConfig(**config_data['database'])
            if 'etl' in config_data:
                config_data['etl'] = ETLConfig(**config_data['etl'])
            if 's3' in config_data:
                config_data['s3'] = S3Config(**config_data['s3'])
            if 'monitoring' in config_data:
                config_data['monitoring'] = MonitoringConfig(**config_data['monitoring'])
            
            return cls(**config_data)
            
        except FileNotFoundError:
            logger.warning(f"Config file not found: {config_path}, using defaults")
            return cls()
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in config file {config_path}: {e}")
            raise ValueError(f"Invalid JSON in config file: {e}")
        except Exception as e:
            logger.error(f"Failed to load config from {config_path}: {e}")
            raise ValueError(f"Failed to load config: {e}")
    
    def to_file(self, config_path: str) -> None:
        """Save configuration to JSON file."""
        try:
            # Convert to dictionary for JSON serialization
            config_dict = self.to_dict()
            
            # Ensure directory exists
            Path(config_path).parent.mkdir(parents=True, exist_ok=True)
            
            with open(config_path, 'w') as f:
                json.dump(config_dict, f, indent=2, default=str)
            
            logger.info(f"Configuration saved to {config_path}")
            
        except Exception as e:
            logger.error(f"Failed to save config to {config_path}: {e}")
            raise ValueError(f"Failed to save config: {e}")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        from dataclasses import asdict
        return asdict(self)
    
    def get_active_tickers(self) -> List[TickerConfig]:
        """Get list of active ticker configurations."""
        return [ticker for ticker in self.tickers if ticker.is_active]
    
    def get_ticker_by_symbol(self, symbol: str) -> Optional[TickerConfig]:
        """Get ticker configuration by symbol."""
        for ticker in self.tickers:
            if ticker.ticker.upper() == symbol.upper():
                return ticker
        return None
    
    def get_ticker_by_cik(self, cik: str) -> Optional[TickerConfig]:
        """Get ticker configuration by CIK."""
        formatted_cik = cik.zfill(10)
        for ticker in self.tickers:
            if ticker.cik == formatted_cik:
                return ticker
        return None
    
    def add_ticker(self, ticker: str, cik: str, priority: int = 1, 
                   forms: List[str] = None) -> None:
        """Add a new ticker configuration."""
        if forms is None:
            forms = ["10-K", "10-Q", "8-K"]
        
        ticker_config = TickerConfig(
            ticker=ticker.upper(),
            cik=cik.zfill(10),
            priority=priority,
            forms_to_process=forms
        )
        self.tickers.append(ticker_config)
    
    def remove_ticker(self, ticker: str) -> bool:
        """Remove ticker configuration by symbol."""
        for i, ticker_config in enumerate(self.tickers):
            if ticker_config.ticker.upper() == ticker.upper():
                del self.tickers[i]
                return True
        return False
    
    def get_high_priority_tickers(self) -> List[TickerConfig]:
        """Get tickers sorted by priority (highest first)."""
        active_tickers = self.get_active_tickers()
        return sorted(active_tickers, key=lambda t: t.priority, reverse=True)


class ConfigManager:
    """Configuration manager with file watching and hot reloading."""
    
    def __init__(self, config_path: str = None):
        self.config_path = config_path or self._get_default_config_path()
        self._config: Optional[GPTTraderConfig] = None
        self._load_config()
    
    def _get_default_config_path(self) -> str:
        """Get default configuration file path."""
        # Try environment variable first
        if os.getenv("GPT_TRADER_CONFIG"):
            return os.getenv("GPT_TRADER_CONFIG")
        
        # Try common locations
        paths = [
            "gpt_trader_config.json",
            "config/gpt_trader_config.json", 
            os.path.expanduser("~/.gpt_trader/config.json"),
            "/etc/gpt_trader/config.json"
        ]
        
        for path in paths:
            if os.path.exists(path):
                return path
        
        # Default to current directory
        return "gpt_trader_config.json"
    
    def _load_config(self) -> None:
        """Load configuration from file."""
        try:
            if os.path.exists(self.config_path):
                self._config = GPTTraderConfig.from_file(self.config_path)
                logger.info(f"Configuration loaded from {self.config_path}")
            else:
                self._config = GPTTraderConfig()
                logger.info("Using default configuration")
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            self._config = GPTTraderConfig()
    
    @property
    def config(self) -> GPTTraderConfig:
        """Get current configuration."""
        return self._config
    
    def reload_config(self) -> None:
        """Reload configuration from file."""
        self._load_config()
        logger.info("Configuration reloaded")
    
    def save_config(self) -> None:
        """Save current configuration to file."""
        if self._config:
            self._config.to_file(self.config_path)


# Global configuration manager
_config_manager: Optional[ConfigManager] = None


def get_config_manager() -> ConfigManager:
    """Get global configuration manager instance."""
    global _config_manager
    if _config_manager is None:
        _config_manager = ConfigManager()
    return _config_manager


def get_config() -> GPTTraderConfig:
    """Get current configuration."""
    return get_config_manager().config


def reload_config() -> None:
    """Reload configuration from file."""
    get_config_manager().reload_config()


if __name__ == "__main__":
    # CLI for configuration management
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python -m gpt_trader.config [show|validate|create-sample]")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "show":
        config = get_config()
        print(json.dumps(config.to_dict(), indent=2, default=str))
    elif command == "validate":
        try:
            config = get_config()
            print("Configuration validation: PASSED")
        except Exception as e:
            print(f"Configuration validation: FAILED - {e}")
            sys.exit(1)
    elif command == "create-sample":
        # Create sample configuration
        config = GPTTraderConfig()
        # Add sample tickers
        config.add_ticker("AAPL", "0000320193", priority=1)
        config.add_ticker("MSFT", "0000789019", priority=1) 
        config.add_ticker("GOOGL", "0001652044", priority=2)
        
        config.to_file("sample_config.json")
        print("Sample configuration created: sample_config.json")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)