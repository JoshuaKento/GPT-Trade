"""
Database connection and session management for GPT Trader.

This module provides database connectivity, connection pooling, and session
management for the GPT Trader platform.
"""

import os
import logging
from contextlib import contextmanager
from typing import Optional, Generator
from urllib.parse import urlparse

from sqlalchemy import create_engine, event, MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool, StaticPool
from sqlalchemy.exc import SQLAlchemyError, DisconnectionError

from .models import Base

logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    """Base database error class."""
    pass


class DatabaseConnectionError(DatabaseError):
    """Database connection error."""
    pass


class DatabaseConfig:
    """Database configuration management."""
    
    def __init__(self):
        self.database_url = self._get_database_url()
        self.echo_sql = os.getenv("DATABASE_ECHO", "false").lower() == "true"
        self.pool_size = int(os.getenv("DATABASE_POOL_SIZE", "20"))
        self.max_overflow = int(os.getenv("DATABASE_MAX_OVERFLOW", "30"))
        self.pool_timeout = int(os.getenv("DATABASE_POOL_TIMEOUT", "30"))
        self.pool_recycle = int(os.getenv("DATABASE_POOL_RECYCLE", "3600"))
        
    def _get_database_url(self) -> str:
        """Get database URL from environment or default to SQLite."""
        # Try PostgreSQL first
        if os.getenv("DATABASE_URL"):
            return os.getenv("DATABASE_URL")
        
        # Try individual PostgreSQL components
        pg_host = os.getenv("POSTGRES_HOST")
        if pg_host:
            pg_user = os.getenv("POSTGRES_USER", "postgres")
            pg_password = os.getenv("POSTGRES_PASSWORD", "")
            pg_db = os.getenv("POSTGRES_DB", "gpt_trader")
            pg_port = os.getenv("POSTGRES_PORT", "5432")
            
            return f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
        
        # Default to SQLite for development
        sqlite_path = os.getenv("SQLITE_PATH", "gpt_trader.db")
        return f"sqlite:///{sqlite_path}"
    
    @property
    def is_sqlite(self) -> bool:
        """Check if using SQLite database."""
        return self.database_url.startswith("sqlite:")
    
    @property
    def is_postgresql(self) -> bool:
        """Check if using PostgreSQL database."""
        return self.database_url.startswith("postgresql:")


class DatabaseManager:
    """Database connection and session manager."""
    
    def __init__(self, config: Optional[DatabaseConfig] = None):
        self.config = config or DatabaseConfig()
        self._engine: Optional[Engine] = None
        self._session_factory: Optional[sessionmaker] = None
        self._metadata = MetaData()
        
    @property
    def engine(self) -> Engine:
        """Get database engine, creating if necessary."""
        if self._engine is None:
            self._create_engine()
        return self._engine
    
    @property
    def session_factory(self) -> sessionmaker:
        """Get session factory, creating if necessary."""
        if self._session_factory is None:
            self._create_session_factory()
        return self._session_factory
    
    def _create_engine(self) -> None:
        """Create database engine with appropriate configuration."""
        try:
            engine_kwargs = {
                "echo": self.config.echo_sql,
            }
            
            if self.config.is_sqlite:
                # SQLite configuration
                engine_kwargs.update({
                    "poolclass": StaticPool,
                    "connect_args": {
                        "check_same_thread": False,
                        "timeout": 30,
                    }
                })
            else:
                # PostgreSQL configuration
                engine_kwargs.update({
                    "poolclass": QueuePool,
                    "pool_size": self.config.pool_size,
                    "max_overflow": self.config.max_overflow,
                    "pool_timeout": self.config.pool_timeout,
                    "pool_recycle": self.config.pool_recycle,
                    "pool_pre_ping": True,  # Verify connections before use
                })
            
            self._engine = create_engine(self.config.database_url, **engine_kwargs)
            
            # Add connection event listeners
            self._add_event_listeners()
            
            logger.info(f"Database engine created: {self._get_safe_url()}")
            
        except Exception as e:
            logger.error(f"Failed to create database engine: {e}")
            raise DatabaseConnectionError(f"Failed to create database engine: {e}")
    
    def _create_session_factory(self) -> None:
        """Create session factory."""
        self._session_factory = sessionmaker(
            bind=self.engine,
            autocommit=False,
            autoflush=False,
        )
    
    def _add_event_listeners(self) -> None:
        """Add SQLAlchemy event listeners."""
        
        @event.listens_for(self._engine, "connect")
        def on_connect(dbapi_connection, connection_record):
            """Handle new database connections."""
            if self.config.is_sqlite:
                # Enable foreign key constraints for SQLite
                cursor = dbapi_connection.cursor()
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.close()
            
            logger.debug("New database connection established")
        
        @event.listens_for(self._engine, "checkout")
        def on_checkout(dbapi_connection, connection_record, connection_proxy):
            """Handle connection checkout from pool."""
            logger.debug("Database connection checked out from pool")
        
        @event.listens_for(self._engine, "checkin")
        def on_checkin(dbapi_connection, connection_record):
            """Handle connection checkin to pool."""
            logger.debug("Database connection checked in to pool")
    
    def _get_safe_url(self) -> str:
        """Get database URL with password masked."""
        parsed = urlparse(self.config.database_url)
        if parsed.password:
            safe_url = self.config.database_url.replace(parsed.password, "***")
        else:
            safe_url = self.config.database_url
        return safe_url
    
    def create_all_tables(self) -> None:
        """Create all database tables."""
        try:
            Base.metadata.create_all(bind=self.engine)
            logger.info("All database tables created successfully")
        except Exception as e:
            logger.error(f"Failed to create database tables: {e}")
            raise DatabaseError(f"Failed to create database tables: {e}")
    
    def drop_all_tables(self) -> None:
        """Drop all database tables. Use with caution!"""
        try:
            Base.metadata.drop_all(bind=self.engine)
            logger.warning("All database tables dropped")
        except Exception as e:
            logger.error(f"Failed to drop database tables: {e}")
            raise DatabaseError(f"Failed to drop database tables: {e}")
    
    def get_session(self) -> Session:
        """Get a new database session."""
        try:
            return self.session_factory()
        except Exception as e:
            logger.error(f"Failed to create database session: {e}")
            raise DatabaseError(f"Failed to create database session: {e}")
    
    @contextmanager
    def session_scope(self) -> Generator[Session, None, None]:
        """Provide a transactional scope around a series of operations."""
        session = self.get_session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error: {e}")
            raise
        finally:
            session.close()
    
    def health_check(self) -> bool:
        """Perform a health check on the database connection."""
        try:
            with self.session_scope() as session:
                # Simple query to test connection
                session.execute("SELECT 1")
            logger.debug("Database health check passed")
            return True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False
    
    def get_connection_info(self) -> dict:
        """Get connection information for monitoring."""
        pool = getattr(self.engine.pool, 'size', lambda: None)
        return {
            "database_url": self._get_safe_url(),
            "is_sqlite": self.config.is_sqlite,
            "is_postgresql": self.config.is_postgresql,
            "pool_size": self.config.pool_size if not self.config.is_sqlite else "N/A",
            "max_overflow": self.config.max_overflow if not self.config.is_sqlite else "N/A",
            "current_pool_size": pool() if callable(pool) else "N/A",
            "echo_sql": self.config.echo_sql,
        }
    
    def close(self) -> None:
        """Close all database connections."""
        if self._engine:
            self._engine.dispose()
            logger.info("Database connections closed")


# Global database manager instance
_db_manager: Optional[DatabaseManager] = None


def get_database_manager() -> DatabaseManager:
    """Get the global database manager instance."""
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager


def get_session() -> Session:
    """Get a new database session from the global manager."""
    return get_database_manager().get_session()


@contextmanager
def session_scope() -> Generator[Session, None, None]:
    """Get a session with automatic transaction management."""
    with get_database_manager().session_scope() as session:
        yield session


def initialize_database(drop_existing: bool = False) -> None:
    """Initialize the database schema."""
    db_manager = get_database_manager()
    
    if drop_existing:
        logger.warning("Dropping existing database tables")
        db_manager.drop_all_tables()
    
    db_manager.create_all_tables()
    logger.info("Database initialized successfully")


def health_check() -> bool:
    """Perform a database health check."""
    return get_database_manager().health_check()


def get_connection_info() -> dict:
    """Get database connection information."""
    return get_database_manager().get_connection_info()


# Migration utilities
def run_migrations() -> None:
    """Run database migrations (placeholder for future Alembic integration)."""
    # This is a placeholder for when we add Alembic migrations
    logger.info("Running database migrations...")
    initialize_database(drop_existing=False)
    logger.info("Database migrations completed")


if __name__ == "__main__":
    # CLI for database operations
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python -m gpt_trader.database [init|health|info|migrate]")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "init":
        initialize_database()
        print("Database initialized successfully")
    elif command == "health":
        if health_check():
            print("Database health check: PASSED")
        else:
            print("Database health check: FAILED")
            sys.exit(1)
    elif command == "info":
        info = get_connection_info()
        for key, value in info.items():
            print(f"{key}: {value}")
    elif command == "migrate":
        run_migrations()
        print("Database migrations completed")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)