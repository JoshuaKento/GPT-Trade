"""
Database configuration and connection management for GPT Trader.

This module provides database configuration, connection pooling, and
session management optimized for financial data processing workloads.
"""

import os
import logging
from typing import Optional, Dict, Any
from contextlib import contextmanager
from urllib.parse import quote_plus

from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError

from database.models import Base

logger = logging.getLogger(__name__)


class DatabaseConfig:
    """Database configuration with performance optimizations."""
    
    def __init__(self):
        """Initialize database configuration from environment variables."""
        self.host = os.getenv('DB_HOST', 'localhost')
        self.port = int(os.getenv('DB_PORT', '5432'))
        self.database = os.getenv('DB_NAME', 'gpt_trader')
        self.username = os.getenv('DB_USER', 'postgres')
        self.password = os.getenv('DB_PASSWORD', '')
        self.ssl_mode = os.getenv('DB_SSL_MODE', 'prefer')
        
        # Connection pool settings
        self.pool_size = int(os.getenv('DB_POOL_SIZE', '10'))
        self.max_overflow = int(os.getenv('DB_MAX_OVERFLOW', '20'))
        self.pool_timeout = int(os.getenv('DB_POOL_TIMEOUT', '30'))
        self.pool_recycle = int(os.getenv('DB_POOL_RECYCLE', '3600'))  # 1 hour
        
        # Performance settings
        self.echo = os.getenv('DB_ECHO', 'false').lower() == 'true'
        self.echo_pool = os.getenv('DB_ECHO_POOL', 'false').lower() == 'true'
        
        # Application settings
        self.application_name = os.getenv('DB_APPLICATION_NAME', 'gpt-trader')
        self.connect_timeout = int(os.getenv('DB_CONNECT_TIMEOUT', '10'))
        self.statement_timeout = int(os.getenv('DB_STATEMENT_TIMEOUT', '300000'))  # 5 minutes
        
    @property
    def database_url(self) -> str:
        """Get the complete database URL."""
        # URL encode password to handle special characters
        encoded_password = quote_plus(self.password) if self.password else ''
        
        url = f"postgresql://{self.username}"
        if encoded_password:
            url += f":{encoded_password}"
        url += f"@{self.host}:{self.port}/{self.database}"
        
        # Add connection parameters
        params = [
            f"sslmode={self.ssl_mode}",
            f"application_name={self.application_name}",
            f"connect_timeout={self.connect_timeout}",
        ]
        
        if params:
            url += "?" + "&".join(params)
            
        return url
    
    @property
    def engine_kwargs(self) -> Dict[str, Any]:
        """Get engine configuration parameters."""
        return {
            'poolclass': QueuePool,
            'pool_size': self.pool_size,
            'max_overflow': self.max_overflow,
            'pool_timeout': self.pool_timeout,
            'pool_recycle': self.pool_recycle,
            'pool_pre_ping': True,  # Validate connections before use
            'echo': self.echo,
            'echo_pool': self.echo_pool,
            'connect_args': {
                'options': f'-c statement_timeout={self.statement_timeout}ms',
                'sslmode': self.ssl_mode,
                'application_name': self.application_name,
            }
        }


class DatabaseManager:
    """Database connection and session management."""
    
    def __init__(self, config: Optional[DatabaseConfig] = None):
        """Initialize database manager with configuration."""
        self.config = config or DatabaseConfig()
        self._engine: Optional[Engine] = None
        self._session_factory: Optional[sessionmaker] = None
        
    @property
    def engine(self) -> Engine:
        """Get or create the database engine."""
        if self._engine is None:
            self._engine = self._create_engine()
        return self._engine
    
    @property
    def session_factory(self) -> sessionmaker:
        """Get or create the session factory."""
        if self._session_factory is None:
            self._session_factory = sessionmaker(
                bind=self.engine,
                autocommit=False,
                autoflush=False,
                expire_on_commit=False  # Keep objects usable after commit
            )
        return self._session_factory
    
    def _create_engine(self) -> Engine:
        """Create and configure the database engine."""
        logger.info(f"Creating database engine for {self.config.database} at {self.config.host}:{self.config.port}")
        
        engine = create_engine(
            self.config.database_url,
            **self.config.engine_kwargs
        )
        
        # Set up event listeners for performance monitoring
        self._setup_engine_events(engine)
        
        return engine
    
    def _setup_engine_events(self, engine: Engine) -> None:
        """Set up engine event listeners for monitoring and optimization."""
        
        @event.listens_for(engine, "connect")
        def set_postgresql_search_path(dbapi_connection, connection_record):
            """Set the PostgreSQL search path to include our schemas."""
            with dbapi_connection.cursor() as cursor:
                cursor.execute("SET search_path TO edgar, trading, audit, public")
                # Set additional PostgreSQL settings for performance
                cursor.execute("SET random_page_cost = 1.1")  # SSD optimization
                cursor.execute("SET effective_io_concurrency = 200")  # SSD concurrent I/O
        
        @event.listens_for(engine, "checkout")
        def receive_checkout(dbapi_connection, connection_record, connection_proxy):
            """Log when a connection is checked out from the pool."""
            if self.config.echo_pool:
                logger.debug("Connection checked out from pool")
        
        @event.listens_for(engine, "checkin")
        def receive_checkin(dbapi_connection, connection_record):
            """Log when a connection is returned to the pool."""
            if self.config.echo_pool:
                logger.debug("Connection returned to pool")
    
    @contextmanager
    def get_session(self):
        """Get a database session with automatic cleanup."""
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error: {e}")
            raise
        finally:
            session.close()
    
    def create_all_tables(self) -> None:
        """Create all database tables."""
        logger.info("Creating all database tables")
        try:
            # Create schemas first
            with self.engine.connect() as conn:
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS edgar"))
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS trading"))
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS audit"))
                conn.commit()
            
            # Create all tables
            Base.metadata.create_all(bind=self.engine)
            logger.info("All database tables created successfully")
        except SQLAlchemyError as e:
            logger.error(f"Failed to create database tables: {e}")
            raise
    
    def drop_all_tables(self) -> None:
        """Drop all database tables (use with caution!)."""
        logger.warning("Dropping all database tables")
        try:
            Base.metadata.drop_all(bind=self.engine)
            logger.info("All database tables dropped successfully")
        except SQLAlchemyError as e:
            logger.error(f"Failed to drop database tables: {e}")
            raise
    
    def check_connection(self) -> bool:
        """Check if database connection is working."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                return result.scalar() == 1
        except Exception as e:
            logger.error(f"Database connection check failed: {e}")
            return False
    
    def get_database_info(self) -> Dict[str, Any]:
        """Get database information and statistics."""
        try:
            with self.get_session() as session:
                # Get database version
                version_result = session.execute(text("SELECT version()"))
                version = version_result.scalar()
                
                # Get database size
                size_result = session.execute(text(
                    "SELECT pg_size_pretty(pg_database_size(current_database()))"
                ))
                database_size = size_result.scalar()
                
                # Get connection count
                conn_result = session.execute(text(
                    "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()"
                ))
                active_connections = conn_result.scalar()
                
                # Get table count for our schemas
                table_result = session.execute(text("""
                    SELECT count(*) 
                    FROM information_schema.tables 
                    WHERE table_schema IN ('edgar', 'trading', 'audit')
                """))
                table_count = table_result.scalar()
                
                return {
                    'version': version,
                    'database_size': database_size,
                    'active_connections': active_connections,
                    'table_count': table_count,
                    'pool_size': self.engine.pool.size(),
                    'checked_out_connections': self.engine.pool.checkedout(),
                }
        except Exception as e:
            logger.error(f"Failed to get database info: {e}")
            return {}
    
    def execute_raw_sql(self, sql: str, parameters: Optional[Dict] = None) -> Any:
        """Execute raw SQL with parameters."""
        try:
            with self.get_session() as session:
                result = session.execute(text(sql), parameters or {})
                return result.fetchall()
        except Exception as e:
            logger.error(f"Failed to execute SQL: {e}")
            raise
    
    def close(self) -> None:
        """Close all database connections."""
        if self._engine:
            self._engine.dispose()
            logger.info("Database engine disposed")


# Global database manager instance
_db_manager: Optional[DatabaseManager] = None


def get_database_manager() -> DatabaseManager:
    """Get the global database manager instance."""
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager


def get_session() -> Session:
    """Get a new database session."""
    return get_database_manager().session_factory()


@contextmanager
def db_session():
    """Context manager for database sessions."""
    session = get_session()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Database session error: {e}")
        raise
    finally:
        session.close()


# Health check functions
def check_database_health() -> Dict[str, Any]:
    """Comprehensive database health check."""
    db_manager = get_database_manager()
    
    health = {
        'status': 'unknown',
        'connection': False,
        'info': {},
        'errors': []
    }
    
    try:
        # Test basic connection
        health['connection'] = db_manager.check_connection()
        
        if health['connection']:
            # Get database information
            health['info'] = db_manager.get_database_info()
            
            # Check for any immediate issues
            if health['info'].get('active_connections', 0) > 100:
                health['errors'].append("High number of active connections")
            
            if health['info'].get('checked_out_connections', 0) > db_manager.config.pool_size * 0.8:
                health['errors'].append("Connection pool nearly exhausted")
            
            health['status'] = 'healthy' if not health['errors'] else 'warning'
        else:
            health['status'] = 'unhealthy'
            health['errors'].append("Database connection failed")
            
    except Exception as e:
        health['status'] = 'unhealthy'
        health['errors'].append(f"Health check failed: {str(e)}")
        logger.error(f"Database health check failed: {e}")
    
    return health


def initialize_database() -> None:
    """Initialize the database with schema and initial data."""
    logger.info("Initializing database")
    
    try:
        db_manager = get_database_manager()
        
        # Check connection
        if not db_manager.check_connection():
            raise RuntimeError("Cannot connect to database")
        
        # Create tables if they don't exist
        db_manager.create_all_tables()
        
        logger.info("Database initialized successfully")
        
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise


# Configuration validation
def validate_database_config() -> bool:
    """Validate database configuration."""
    config = DatabaseConfig()
    
    required_vars = ['DB_HOST', 'DB_NAME', 'DB_USER']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {missing_vars}")
        return False
    
    # Test connection
    try:
        db_manager = DatabaseManager(config)
        return db_manager.check_connection()
    except Exception as e:
        logger.error(f"Database configuration validation failed: {e}")
        return False