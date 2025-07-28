"""Main database integration module for GPT Trader."""

import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

from .database_config import DatabaseConfig
from .database_errors import DatabaseRetryHandler, CircuitBreaker, DatabaseErrorReporter
from .database_health import DatabaseHealthChecker, DatabaseMonitor
from .database_manager import DatabaseManager
from .database_migrations import MigrationManager
from .database_session import SessionManager, TransactionManager


class Database:
    """Main database integration class that provides a unified interface."""

    def __init__(self, config: Optional[DatabaseConfig] = None):
        """Initialize database integration.
        
        Args:
            config: Optional database configuration. If None, loads from environment.
        """
        self.logger = logging.getLogger(__name__)
        
        # Load configuration
        if config is None:
            config = self._load_default_config()
        self.config = config
        
        # Initialize core components
        self.manager = DatabaseManager(config)
        self.session_manager = SessionManager(self.manager)
        self.transaction_manager = TransactionManager(self.session_manager)
        self.migration_manager = MigrationManager(self.manager, self.session_manager)
        
        # Initialize monitoring and error handling
        self.health_checker = DatabaseHealthChecker(self.manager)
        self.monitor = DatabaseMonitor(self.health_checker)
        self.retry_handler = DatabaseRetryHandler(config)
        self.circuit_breaker = CircuitBreaker()
        self.error_reporter = DatabaseErrorReporter()
        
        self.logger.info(f"Database integration initialized with {config.database_type}")

    def _load_default_config(self) -> DatabaseConfig:
        """Load default configuration from environment variables."""
        # Check for explicit configuration
        if os.getenv("DATABASE_URL"):
            return DatabaseConfig.from_environment()
        
        # Default to SQLite for development
        return DatabaseConfig(
            database_type="sqlite",
            sqlite_path="data/gpt_trader.db",
        )

    def initialize(self) -> None:
        """Initialize the database system."""
        try:
            # Create data directory if using SQLite
            if self.config.database_type == "sqlite":
                Path(self.config.sqlite_path).parent.mkdir(parents=True, exist_ok=True)
            
            # Initialize migration system
            self.migration_manager.initialize()
            
            # Run any pending migrations
            self.migrate()
            
            # Perform initial health check
            health_result = self.health_checker.check_connection()
            if not health_result.is_healthy:
                self.logger.warning(
                    f"Initial health check failed: {health_result.error_message}"
                )
            
            self.logger.info("Database system initialized successfully")
            
        except Exception as error:
            self.logger.error(f"Failed to initialize database system: {error}")
            raise

    def migrate(self, target_version: Optional[str] = None) -> None:
        """Run database migrations.
        
        Args:
            target_version: Optional target migration version
        """
        try:
            self.migration_manager.migrate(target_version)
        except Exception as error:
            self.error_reporter.record_error(error, "migration")
            raise

    def rollback(self, target_version: Optional[str] = None, steps: Optional[int] = None) -> None:
        """Rollback database migrations.
        
        Args:
            target_version: Rollback to this version
            steps: Number of migrations to rollback
        """
        try:
            self.migration_manager.rollback(target_version, steps)
        except Exception as error:
            self.error_reporter.record_error(error, "rollback")
            raise

    def health_check(self) -> Dict[str, Any]:
        """Perform comprehensive health check.
        
        Returns:
            Health check results
        """
        return self.health_checker.comprehensive_health_check()

    def start_monitoring(self) -> None:
        """Start continuous health monitoring."""
        # Add error reporting to monitoring alerts
        def alert_callback(alert_data):
            if alert_data["type"] == "failure":
                self.error_reporter.record_error(
                    Exception(alert_data["result"]["error_message"]),
                    "health_check"
                )
        
        self.monitor.add_alert_callback(alert_callback)
        self.monitor.start_monitoring()

    def stop_monitoring(self) -> None:
        """Stop continuous health monitoring."""
        self.monitor.stop_monitoring()

    def get_status(self) -> Dict[str, Any]:
        """Get comprehensive database status.
        
        Returns:
            Dictionary containing database status information
        """
        status = {
            "config": self.config.mask_sensitive_data(),
            "health": self.health_checker.check_connection().to_dict(),
            "connection_info": self.manager.get_connection_info(),
            "monitoring": self.monitor.get_monitoring_status(),
            "migrations": self.migration_manager.status(),
            "errors": self.error_reporter.get_error_summary(),
            "circuit_breaker": self.circuit_breaker.get_state(),
        }
        
        # Add connection pool stats if available
        pool_stats = self.health_checker.get_connection_pool_stats()
        if pool_stats:
            status["connection_pool"] = pool_stats.to_dict()
        
        return status

    def execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute a SQL query with error handling and retry logic.
        
        Args:
            query: SQL query string
            params: Optional query parameters
            
        Returns:
            Query result
        """
        @self.retry_handler.with_retry()
        @self.circuit_breaker
        def _execute():
            return self.session_manager.execute_in_session(query, params)
        
        try:
            return _execute()
        except Exception as error:
            self.error_reporter.record_error(error, f"execute: {query[:50]}...")
            raise

    def execute_transaction(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute a SQL query within a transaction.
        
        Args:
            query: SQL query string
            params: Optional query parameters
            
        Returns:
            Query result
        """
        @self.retry_handler.with_retry()
        @self.circuit_breaker
        def _execute():
            return self.session_manager.execute_in_transaction(query, params)
        
        try:
            return _execute()
        except Exception as error:
            self.error_reporter.record_error(error, f"transaction: {query[:50]}...")
            raise

    def bulk_insert(self, model_class: Any, data: list, batch_size: int = 1000) -> None:
        """Perform bulk insert with error handling.
        
        Args:
            model_class: SQLAlchemy model class
            data: List of dictionaries containing data to insert
            batch_size: Number of records to insert in each batch
        """
        @self.retry_handler.with_retry()
        @self.circuit_breaker
        def _bulk_insert():
            return self.session_manager.bulk_insert(model_class, data, batch_size)
        
        try:
            return _bulk_insert()
        except Exception as error:
            self.error_reporter.record_error(error, f"bulk_insert: {len(data)} records")
            raise

    def close(self) -> None:
        """Close database connections and cleanup resources."""
        try:
            self.stop_monitoring()
            self.session_manager.close()
            self.logger.info("Database connections closed")
        except Exception as error:
            self.logger.error(f"Error closing database connections: {error}")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        self.close()


# Convenience functions for common use cases
def create_sqlite_database(db_path: str = "data/gpt_trader.db") -> Database:
    """Create a SQLite database instance.
    
    Args:
        db_path: Path to SQLite database file
        
    Returns:
        Database instance configured for SQLite
    """
    config = DatabaseConfig(
        database_type="sqlite",
        sqlite_path=db_path
    )
    return Database(config)


def create_postgres_database(
    host: str = "localhost",
    port: int = 5432,
    database: str = "gpt_trader",
    username: str = "postgres",
    password: str = "",
    **kwargs
) -> Database:
    """Create a PostgreSQL database instance.
    
    Args:
        host: PostgreSQL host
        port: PostgreSQL port
        database: Database name
        username: Username
        password: Password
        **kwargs: Additional configuration options
        
    Returns:
        Database instance configured for PostgreSQL
    """
    config = DatabaseConfig(
        database_type="postgresql",
        postgres_host=host,
        postgres_port=port,
        postgres_database=database,
        postgres_username=username,
        postgres_password=password,
        **kwargs
    )
    return Database(config)


def create_database_from_url(database_url: str, **kwargs) -> Database:
    """Create a database instance from a connection URL.
    
    Args:
        database_url: Database connection URL
        **kwargs: Additional configuration options
        
    Returns:
        Database instance configured from URL
    """
    # Determine database type from URL
    if database_url.startswith("sqlite"):
        database_type = "sqlite"
    elif database_url.startswith(("postgresql", "postgres")):
        database_type = "postgresql"
    else:
        raise ValueError(f"Unsupported database URL: {database_url}")
    
    config = DatabaseConfig(
        database_type=database_type,
        database_url=database_url,
        **kwargs
    )
    return Database(config)


# Global database instance (optional, for convenience)
_global_database: Optional[Database] = None


def get_database() -> Optional[Database]:
    """Get the global database instance.
    
    Returns:
        Global Database instance or None if not initialized
    """
    return _global_database


def set_database(database: Database) -> None:
    """Set the global database instance.
    
    Args:
        database: Database instance to set as global
    """
    global _global_database
    _global_database = database


def initialize_global_database(config: Optional[DatabaseConfig] = None) -> Database:
    """Initialize the global database instance.
    
    Args:
        config: Optional database configuration
        
    Returns:
        Initialized Database instance
    """
    global _global_database
    _global_database = Database(config)
    _global_database.initialize()
    return _global_database