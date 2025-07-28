"""Database configuration management with validation and type safety."""

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Literal, Optional, Union
from urllib.parse import urlparse


DatabaseType = Literal["sqlite", "postgresql"]


@dataclass
class DatabaseConfig:
    """Database configuration class with validation and type safety."""

    # Database type and connection
    database_type: DatabaseType = "sqlite"
    database_url: Optional[str] = None
    
    # SQLite specific settings
    sqlite_path: str = "data/gpt_trader.db"
    sqlite_timeout: float = 30.0
    sqlite_check_same_thread: bool = False
    
    # PostgreSQL specific settings
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_database: str = "gpt_trader"
    postgres_username: str = "postgres"
    postgres_password: str = ""
    postgres_schema: str = "public"
    
    # Connection pooling
    pool_size: int = 10
    max_overflow: int = 20
    pool_timeout: float = 30.0
    pool_recycle: int = 3600  # 1 hour
    pool_pre_ping: bool = True
    
    # Connection retry settings
    connect_retries: int = 3
    connect_retry_delay: float = 1.0
    connect_retry_backoff: float = 2.0
    
    # Transaction settings
    isolation_level: str = "READ_COMMITTED"
    autocommit: bool = False
    
    # Performance settings
    echo: bool = False  # SQL logging
    echo_pool: bool = False  # Connection pool logging
    
    # Migration settings
    migration_directory: str = "migrations"
    migration_table: str = "schema_migrations"
    
    # Health check settings
    health_check_interval: int = 60  # seconds
    health_check_timeout: float = 5.0

    def __post_init__(self):
        """Validate configuration after initialization."""
        self._validate()

    def _validate(self) -> None:
        """Validate configuration values."""
        # Validate database type
        if self.database_type not in ["sqlite", "postgresql"]:
            raise ValueError(f"Invalid database_type: {self.database_type}")

        # Validate pool settings
        if self.pool_size < 1:
            raise ValueError("pool_size must be at least 1")
        
        if self.max_overflow < 0:
            raise ValueError("max_overflow cannot be negative")
        
        if self.pool_timeout <= 0:
            raise ValueError("pool_timeout must be positive")
        
        if self.pool_recycle < 0:
            raise ValueError("pool_recycle cannot be negative")

        # Validate retry settings
        if self.connect_retries < 0:
            raise ValueError("connect_retries cannot be negative")
        
        if self.connect_retry_delay < 0:
            raise ValueError("connect_retry_delay cannot be negative")
        
        if self.connect_retry_backoff <= 0:
            raise ValueError("connect_retry_backoff must be positive")

        # Validate SQLite settings
        if self.database_type == "sqlite":
            if self.sqlite_timeout <= 0:
                raise ValueError("sqlite_timeout must be positive")
            
            # Ensure SQLite path directory exists or can be created
            sqlite_dir = Path(self.sqlite_path).parent
            if not sqlite_dir.exists():
                try:
                    sqlite_dir.mkdir(parents=True, exist_ok=True)
                except Exception as e:
                    raise ValueError(f"Cannot create SQLite directory {sqlite_dir}: {e}")

        # Validate PostgreSQL settings
        if self.database_type == "postgresql":
            if self.postgres_port < 1 or self.postgres_port > 65535:
                raise ValueError("postgres_port must be between 1 and 65535")
            
            if not self.postgres_host:
                raise ValueError("postgres_host is required for PostgreSQL")
            
            if not self.postgres_database:
                raise ValueError("postgres_database is required for PostgreSQL")
            
            if not self.postgres_username:
                raise ValueError("postgres_username is required for PostgreSQL")

        # Validate database URL if provided
        if self.database_url:
            self._validate_database_url()

        # Validate isolation level
        valid_isolation_levels = {
            "READ_UNCOMMITTED",
            "READ_COMMITTED", 
            "REPEATABLE_READ",
            "SERIALIZABLE"
        }
        if self.isolation_level not in valid_isolation_levels:
            raise ValueError(f"Invalid isolation_level: {self.isolation_level}")

        # Validate health check settings
        if self.health_check_interval < 1:
            raise ValueError("health_check_interval must be at least 1 second")
        
        if self.health_check_timeout <= 0:
            raise ValueError("health_check_timeout must be positive")

    def _validate_database_url(self) -> None:
        """Validate the database URL format."""
        if not self.database_url:
            return
            
        try:
            parsed = urlparse(self.database_url)
            
            if self.database_type == "sqlite":
                if not parsed.scheme.startswith("sqlite"):
                    raise ValueError("SQLite URLs must start with 'sqlite://'")
            elif self.database_type == "postgresql":
                if parsed.scheme not in ["postgresql", "postgres"]:
                    raise ValueError("PostgreSQL URLs must start with 'postgresql://' or 'postgres://'")
                    
        except Exception as e:
            raise ValueError(f"Invalid database_url format: {e}")

    def get_connection_url(self) -> str:
        """Generate the appropriate database connection URL.
        
        Returns:
            Database connection URL string
        """
        if self.database_url:
            return self.database_url
            
        if self.database_type == "sqlite":
            # Convert relative path to absolute
            sqlite_path = Path(self.sqlite_path).resolve()
            return f"sqlite:///{sqlite_path}"
        
        elif self.database_type == "postgresql":
            # Build PostgreSQL URL
            url = f"postgresql://{self.postgres_username}"
            if self.postgres_password:
                url += f":{self.postgres_password}"
            url += f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_database}"
            if self.postgres_schema != "public":
                url += f"?options=-csearch_path%3D{self.postgres_schema}"
            return url
        
        else:
            raise ValueError(f"Unsupported database type: {self.database_type}")

    def get_pool_config(self) -> Dict[str, Any]:
        """Get connection pool configuration.
        
        Returns:
            Dictionary of pool configuration parameters
        """
        config = {
            "pool_size": self.pool_size,
            "max_overflow": self.max_overflow,
            "pool_timeout": self.pool_timeout,
            "pool_recycle": self.pool_recycle,
            "pool_pre_ping": self.pool_pre_ping,
            "echo": self.echo,
            "echo_pool": self.echo_pool,
        }
        
        # Add SQLite specific settings
        if self.database_type == "sqlite":
            config.update({
                "connect_args": {
                    "timeout": self.sqlite_timeout,
                    "check_same_thread": self.sqlite_check_same_thread,
                }
            })
        
        return config

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DatabaseConfig":
        """Create configuration from dictionary with type conversion.

        Args:
            data: Configuration dictionary

        Returns:
            DatabaseConfig instance
        """
        # Convert types as needed
        converted = {}

        type_mappings = {
            "postgres_port": int,
            "pool_size": int,
            "max_overflow": int,
            "pool_timeout": float,
            "pool_recycle": int,
            "pool_pre_ping": bool,
            "connect_retries": int,
            "connect_retry_delay": float,
            "connect_retry_backoff": float,
            "autocommit": bool,
            "echo": bool,
            "echo_pool": bool,
            "sqlite_timeout": float,
            "sqlite_check_same_thread": bool,
            "health_check_interval": int,
            "health_check_timeout": float,
        }

        for key, value in data.items():
            if key in type_mappings:
                try:
                    converted[key] = type_mappings[key](value)
                except (ValueError, TypeError) as e:
                    raise ValueError(f"Invalid value for {key}: {value} ({e})")
            else:
                converted[key] = value

        return cls(**converted)

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            "database_type": self.database_type,
            "database_url": self.database_url,
            "sqlite_path": self.sqlite_path,
            "sqlite_timeout": self.sqlite_timeout,
            "sqlite_check_same_thread": self.sqlite_check_same_thread,
            "postgres_host": self.postgres_host,
            "postgres_port": self.postgres_port,
            "postgres_database": self.postgres_database,
            "postgres_username": self.postgres_username,
            "postgres_password": self.postgres_password,
            "postgres_schema": self.postgres_schema,
            "pool_size": self.pool_size,
            "max_overflow": self.max_overflow,
            "pool_timeout": self.pool_timeout,
            "pool_recycle": self.pool_recycle,
            "pool_pre_ping": self.pool_pre_ping,
            "connect_retries": self.connect_retries,
            "connect_retry_delay": self.connect_retry_delay,
            "connect_retry_backoff": self.connect_retry_backoff,
            "isolation_level": self.isolation_level,
            "autocommit": self.autocommit,
            "echo": self.echo,
            "echo_pool": self.echo_pool,
            "migration_directory": self.migration_directory,
            "migration_table": self.migration_table,
            "health_check_interval": self.health_check_interval,
            "health_check_timeout": self.health_check_timeout,
        }

    def get_environment_mappings(self) -> Dict[str, str]:
        """Get mapping of environment variables to config keys.
        
        Returns:
            Dictionary mapping environment variable names to config keys
        """
        return {
            # Database connection
            "DATABASE_TYPE": "database_type",
            "DATABASE_URL": "database_url",
            
            # SQLite settings
            "SQLITE_PATH": "sqlite_path",
            "SQLITE_TIMEOUT": "sqlite_timeout",
            "SQLITE_CHECK_SAME_THREAD": "sqlite_check_same_thread",
            
            # PostgreSQL settings
            "POSTGRES_HOST": "postgres_host",
            "POSTGRES_PORT": "postgres_port",
            "POSTGRES_DATABASE": "postgres_database",
            "POSTGRES_USER": "postgres_username",
            "POSTGRES_USERNAME": "postgres_username",
            "POSTGRES_PASSWORD": "postgres_password",
            "POSTGRES_SCHEMA": "postgres_schema",
            
            # Connection pooling
            "DB_POOL_SIZE": "pool_size",
            "DB_MAX_OVERFLOW": "max_overflow",
            "DB_POOL_TIMEOUT": "pool_timeout",
            "DB_POOL_RECYCLE": "pool_recycle",
            "DB_POOL_PRE_PING": "pool_pre_ping",
            
            # Retry settings
            "DB_CONNECT_RETRIES": "connect_retries",
            "DB_CONNECT_RETRY_DELAY": "connect_retry_delay",
            "DB_CONNECT_RETRY_BACKOFF": "connect_retry_backoff",
            
            # Transaction settings
            "DB_ISOLATION_LEVEL": "isolation_level",
            "DB_AUTOCOMMIT": "autocommit",
            
            # Logging
            "DB_ECHO": "echo",
            "DB_ECHO_POOL": "echo_pool",
            
            # Migration settings
            "DB_MIGRATION_DIR": "migration_directory",
            "DB_MIGRATION_TABLE": "migration_table",
            
            # Health check settings
            "DB_HEALTH_CHECK_INTERVAL": "health_check_interval",
            "DB_HEALTH_CHECK_TIMEOUT": "health_check_timeout",
        }

    @classmethod
    def from_environment(cls) -> "DatabaseConfig":
        """Create configuration from environment variables.
        
        Returns:
            DatabaseConfig instance with values from environment
        """
        config = cls()
        env_mappings = config.get_environment_mappings()
        env_data = {}
        
        for env_var, config_key in env_mappings.items():
            value = os.environ.get(env_var)
            if value is not None:
                env_data[config_key] = value
        
        if env_data:
            return cls.from_dict(env_data)
        return config

    def mask_sensitive_data(self) -> Dict[str, Any]:
        """Get configuration dictionary with sensitive data masked.
        
        Returns:
            Configuration dictionary with passwords masked
        """
        config = self.to_dict()
        if config.get("postgres_password"):
            config["postgres_password"] = "***masked***"
        if config.get("database_url") and "://" in config["database_url"]:
            # Mask password in URL
            url = config["database_url"]
            if "@" in url and "://" in url:
                scheme, rest = url.split("://", 1)
                if "@" in rest:
                    auth, host_part = rest.split("@", 1)
                    if ":" in auth:
                        user, _ = auth.split(":", 1)
                        config["database_url"] = f"{scheme}://{user}:***masked***@{host_part}"
        return config