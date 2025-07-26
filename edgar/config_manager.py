"""Enhanced configuration management with validation and type safety."""
import os
import json
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field
from pathlib import Path
import logging


@dataclass
class EdgarConfig:
    """Configuration class with validation and type safety."""
    
    # Rate limiting
    rate_limit_per_sec: float = 6.0
    
    # Concurrency
    num_workers: int = 6
    max_pool_connections: int = 50
    
    # S3 settings
    s3_prefix: str = "edgar"
    s3_region: Optional[str] = None
    
    # Filing filters
    form_types: List[str] = field(default_factory=list)
    
    # HTTP settings
    timeout: int = 30
    max_retries: int = 3
    backoff_factor: float = 0.5
    
    # User agent (required by SEC)
    user_agent: str = "GPT-Trade-Agent (your-email@example.com)"
    
    # Logging
    log_level: str = "INFO"
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        self._validate()
    
    def _validate(self) -> None:
        """Validate configuration values."""
        if self.rate_limit_per_sec <= 0:
            raise ValueError("rate_limit_per_sec must be positive")
        
        if self.rate_limit_per_sec > 10:
            logging.warning(
                f"Rate limit {self.rate_limit_per_sec}/sec exceeds SEC recommendations (6/sec)"
            )
        
        if self.num_workers < 1:
            raise ValueError("num_workers must be at least 1")
        
        if self.timeout < 1:
            raise ValueError("timeout must be at least 1 second")
        
        if self.max_retries < 0:
            raise ValueError("max_retries cannot be negative")
        
        if not self.user_agent or not self.user_agent.strip():
            raise ValueError("user_agent is required by SEC")
        
        if "@" not in self.user_agent:
            logging.warning(
                "SEC requires contact email in User-Agent header. "
                f"Current value: {self.user_agent}"
            )
        
        # Validate form types
        valid_forms = {
            "10-K", "10-Q", "8-K", "10-K/A", "10-Q/A", "8-K/A",
            "20-F", "6-K", "F-1", "F-3", "S-1", "S-3", "DEF 14A"
        }
        
        for form_type in self.form_types:
            if form_type.upper() not in valid_forms:
                logging.warning(f"Unknown form type: {form_type}")
        
        # Validate log level
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if self.log_level.upper() not in valid_levels:
            raise ValueError(f"Invalid log_level: {self.log_level}")
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EdgarConfig":
        """Create configuration from dictionary with type conversion.
        
        Args:
            data: Configuration dictionary
            
        Returns:
            EdgarConfig instance
        """
        # Convert types as needed
        converted = {}
        
        for key, value in data.items():
            if key == "rate_limit_per_sec":
                converted[key] = float(value)
            elif key in ("num_workers", "timeout", "max_retries", "max_pool_connections"):
                converted[key] = int(value)
            elif key == "backoff_factor":
                converted[key] = float(value)
            elif key == "form_types":
                if isinstance(value, str):
                    # Handle comma-separated string
                    converted[key] = [f.strip() for f in value.split(",") if f.strip()]
                elif isinstance(value, list):
                    converted[key] = value
                else:
                    raise ValueError(f"form_types must be list or comma-separated string")
            else:
                converted[key] = value
        
        return cls(**converted)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            "rate_limit_per_sec": self.rate_limit_per_sec,
            "num_workers": self.num_workers,
            "max_pool_connections": self.max_pool_connections,
            "s3_prefix": self.s3_prefix,
            "s3_region": self.s3_region,
            "form_types": self.form_types,
            "timeout": self.timeout,
            "max_retries": self.max_retries,
            "backoff_factor": self.backoff_factor,
            "user_agent": self.user_agent,
            "log_level": self.log_level,
        }


class ConfigManager:
    """Manages configuration loading and environment variable integration."""
    
    def __init__(self):
        self._config: Optional[EdgarConfig] = None
        self.logger = logging.getLogger(__name__)
    
    def load_config(
        self,
        config_path: Optional[Union[str, Path]] = None,
        use_env_vars: bool = True
    ) -> EdgarConfig:
        """Load configuration from file and environment variables.
        
        Args:
            config_path: Path to configuration JSON file
            use_env_vars: Whether to override with environment variables
            
        Returns:
            EdgarConfig instance
        """
        # Start with defaults
        config_data = {}
        
        # Load from file if specified
        if config_path:
            config_data = self._load_from_file(config_path)
        else:
            # Try default locations
            default_paths = [
                "config.json",
                "edgar_config.json",
                Path.home() / ".edgar" / "config.json"
            ]
            
            for path in default_paths:
                if Path(path).exists():
                    config_data = self._load_from_file(path)
                    self.logger.info(f"Loaded configuration from {path}")
                    break
        
        # Override with environment variables if requested
        if use_env_vars:
            config_data.update(self._load_from_env())
        
        # Create and cache configuration
        self._config = EdgarConfig.from_dict(config_data)
        return self._config
    
    def get_config(self) -> EdgarConfig:
        """Get current configuration, loading defaults if not already loaded.
        
        Returns:
            EdgarConfig instance
        """
        if self._config is None:
            self._config = self.load_config()
        return self._config
    
    def _load_from_file(self, config_path: Union[str, Path]) -> Dict[str, Any]:
        """Load configuration from JSON file.
        
        Args:
            config_path: Path to configuration file
            
        Returns:
            Configuration dictionary
        """
        try:
            path = Path(config_path)
            if not path.exists():
                self.logger.warning(f"Configuration file not found: {path}")
                return {}
            
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if not isinstance(data, dict):
                raise ValueError("Configuration file must contain a JSON object")
            
            self.logger.info(f"Loaded configuration from {path}")
            return data
            
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in configuration file {config_path}: {e}")
        except Exception as e:
            raise RuntimeError(f"Failed to load configuration file {config_path}: {e}")
    
    def _load_from_env(self) -> Dict[str, Any]:
        """Load configuration from environment variables.
        
        Returns:
            Configuration dictionary from environment variables
        """
        env_config = {}
        
        # Map environment variables to config keys
        env_mappings = {
            "EDGAR_RATE_LIMIT": "rate_limit_per_sec",
            "EDGAR_NUM_WORKERS": "num_workers",
            "EDGAR_MAX_POOL_CONNECTIONS": "max_pool_connections",
            "EDGAR_S3_PREFIX": "s3_prefix",
            "EDGAR_S3_REGION": "s3_region",
            "EDGAR_FORM_TYPES": "form_types",
            "EDGAR_TIMEOUT": "timeout",
            "EDGAR_MAX_RETRIES": "max_retries",
            "EDGAR_BACKOFF_FACTOR": "backoff_factor",
            "SEC_USER_AGENT": "user_agent",
            "EDGAR_LOG_LEVEL": "log_level",
            "LOG_LEVEL": "log_level",  # Common alternative
        }
        
        for env_var, config_key in env_mappings.items():
            value = os.environ.get(env_var)
            if value is not None:
                env_config[config_key] = value
                self.logger.debug(f"Using environment variable {env_var} for {config_key}")
        
        return env_config
    
    def save_config(
        self,
        config: EdgarConfig,
        config_path: Union[str, Path]
    ) -> None:
        """Save configuration to JSON file.
        
        Args:
            config: Configuration to save
            config_path: Path to save configuration file
        """
        try:
            path = Path(config_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(config.to_dict(), f, indent=2)
            
            self.logger.info(f"Saved configuration to {path}")
            
        except Exception as e:
            raise RuntimeError(f"Failed to save configuration to {config_path}: {e}")
    
    def reset(self) -> None:
        """Reset cached configuration."""
        self._config = None


# Global instance for backward compatibility
_config_manager = ConfigManager()


def load_config(path: Optional[str] = None) -> Dict[str, Any]:
    """Legacy function for backward compatibility.
    
    Args:
        path: Optional path to configuration file
        
    Returns:
        Configuration dictionary
    """
    config = _config_manager.load_config(path)
    return config.to_dict()


def get_config() -> Dict[str, Any]:
    """Legacy function for backward compatibility.
    
    Returns:
        Configuration dictionary
    """
    config = _config_manager.get_config()
    return config.to_dict()


def get_config_manager() -> ConfigManager:
    """Get the global configuration manager instance.
    
    Returns:
        ConfigManager instance
    """
    return _config_manager