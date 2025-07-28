"""Database connection management with pooling and retry logic."""

import asyncio
import logging
import time
from contextlib import asynccontextmanager, contextmanager
from typing import Any, AsyncGenerator, Dict, Generator, Optional, Union

try:
    import sqlalchemy as sa
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import Engine
    from sqlalchemy.exc import DisconnectionError, OperationalError, SQLAlchemyError
    from sqlalchemy.orm import sessionmaker, Session
    from sqlalchemy.pool import QueuePool, StaticPool
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False

try:
    import asyncpg
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
    from sqlalchemy.ext.asyncio.engine import AsyncEngine
    ASYNCPG_AVAILABLE = True
except ImportError:
    ASYNCPG_AVAILABLE = False

from .database_config import DatabaseConfig


class DatabaseError(Exception):
    """Base exception for database operations."""
    pass


class ConnectionError(DatabaseError):
    """Raised when database connection fails."""
    pass


class TransactionError(DatabaseError):
    """Raised when transaction operations fail."""
    pass


class DatabaseManager:
    """Manages database connections with pooling and retry logic."""

    def __init__(self, config: DatabaseConfig):
        """Initialize database manager.
        
        Args:
            config: Database configuration
            
        Raises:
            ImportError: If required database libraries are not installed
        """
        if not SQLALCHEMY_AVAILABLE:
            raise ImportError(
                "SQLAlchemy is required for database operations. "
                "Install with: pip install sqlalchemy"
            )
        
        self.config = config
        self.logger = logging.getLogger(__name__)
        self._engine: Optional[Engine] = None
        self._async_engine: Optional[AsyncEngine] = None
        self._session_maker: Optional[sessionmaker] = None
        self._async_session_maker: Optional[async_sessionmaker] = None
        self._last_health_check = 0.0
        self._is_healthy = False

    @property
    def engine(self) -> Engine:
        """Get or create the database engine.
        
        Returns:
            SQLAlchemy Engine instance
            
        Raises:
            ConnectionError: If engine creation fails
        """
        if self._engine is None:
            self._engine = self._create_engine()
        return self._engine

    @property
    def async_engine(self) -> AsyncEngine:
        """Get or create the async database engine.
        
        Returns:
            SQLAlchemy AsyncEngine instance
            
        Raises:
            ImportError: If async libraries are not available
            ConnectionError: If engine creation fails
        """
        if not ASYNCPG_AVAILABLE and self.config.database_type == "postgresql":
            raise ImportError(
                "asyncpg is required for async PostgreSQL operations. "
                "Install with: pip install asyncpg"
            )
        
        if self._async_engine is None:
            self._async_engine = self._create_async_engine()
        return self._async_engine

    @property
    def session_maker(self) -> sessionmaker:
        """Get or create the session maker.
        
        Returns:
            SQLAlchemy sessionmaker
        """
        if self._session_maker is None:
            self._session_maker = sessionmaker(
                bind=self.engine,
                autocommit=self.config.autocommit,
                expire_on_commit=False
            )
        return self._session_maker

    @property
    def async_session_maker(self) -> async_sessionmaker:
        """Get or create the async session maker.
        
        Returns:
            SQLAlchemy async_sessionmaker
        """
        if self._async_session_maker is None:
            self._async_session_maker = async_sessionmaker(
                bind=self.async_engine,
                expire_on_commit=False
            )
        return self._async_session_maker

    def _create_engine(self) -> Engine:
        """Create and configure the database engine.
        
        Returns:
            Configured SQLAlchemy Engine
            
        Raises:
            ConnectionError: If engine creation fails
        """
        try:
            connection_url = self.config.get_connection_url()
            pool_config = self.config.get_pool_config()
            
            # Configure pool class based on database type
            if self.config.database_type == "sqlite":
                # SQLite doesn't support connection pooling in the traditional sense
                pool_config["poolclass"] = StaticPool
                # Remove pool settings that don't apply to SQLite
                for key in ["pool_size", "max_overflow"]:
                    pool_config.pop(key, None)
            else:
                pool_config["poolclass"] = QueuePool

            engine = create_engine(connection_url, **pool_config)
            
            # Test the connection
            self._test_connection(engine)
            
            self.logger.info(
                f"Created database engine for {self.config.database_type} "
                f"with pool_size={self.config.pool_size}"
            )
            
            return engine
            
        except Exception as e:
            self.logger.error(f"Failed to create database engine: {e}")
            raise ConnectionError(f"Failed to create database engine: {e}") from e

    def _create_async_engine(self) -> AsyncEngine:
        """Create and configure the async database engine.
        
        Returns:
            Configured SQLAlchemy AsyncEngine
            
        Raises:
            ConnectionError: If engine creation fails
        """
        try:
            connection_url = self.config.get_connection_url()
            
            # Convert to async URL
            if connection_url.startswith("postgresql://"):
                connection_url = connection_url.replace("postgresql://", "postgresql+asyncpg://")
            elif connection_url.startswith("sqlite:///"):
                connection_url = connection_url.replace("sqlite:///", "sqlite+aiosqlite:///")
            
            pool_config = self.config.get_pool_config()
            
            # Configure pool for async engine
            if self.config.database_type == "sqlite":
                pool_config["poolclass"] = StaticPool
                for key in ["pool_size", "max_overflow"]:
                    pool_config.pop(key, None)
            else:
                pool_config["poolclass"] = QueuePool

            engine = create_async_engine(connection_url, **pool_config)
            
            self.logger.info(
                f"Created async database engine for {self.config.database_type}"
            )
            
            return engine
            
        except Exception as e:
            self.logger.error(f"Failed to create async database engine: {e}")
            raise ConnectionError(f"Failed to create async database engine: {e}") from e

    def _test_connection(self, engine: Engine) -> None:
        """Test database connection.
        
        Args:
            engine: SQLAlchemy engine to test
            
        Raises:
            ConnectionError: If connection test fails
        """
        retries = 0
        max_retries = self.config.connect_retries
        delay = self.config.connect_retry_delay
        
        while retries <= max_retries:
            try:
                with engine.connect() as conn:
                    # Simple query to test connection
                    if self.config.database_type == "sqlite":
                        conn.execute(text("SELECT 1"))
                    else:
                        conn.execute(text("SELECT 1 as test"))
                    
                self.logger.debug("Database connection test successful")
                self._is_healthy = True
                return
                
            except (OperationalError, DisconnectionError) as e:
                retries += 1
                if retries > max_retries:
                    self.logger.error(f"Database connection failed after {retries} attempts: {e}")
                    raise ConnectionError(f"Database connection failed: {e}") from e
                
                self.logger.warning(
                    f"Database connection attempt {retries} failed: {e}. "
                    f"Retrying in {delay} seconds..."
                )
                time.sleep(delay)
                delay *= self.config.connect_retry_backoff

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """Get a database session with automatic cleanup.
        
        Yields:
            SQLAlchemy Session instance
            
        Raises:
            DatabaseError: If session creation or operation fails
        """
        session = self.session_maker()
        try:
            yield session
        except SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"Database session error: {e}")
            raise DatabaseError(f"Database operation failed: {e}") from e
        except Exception as e:
            session.rollback()
            self.logger.error(f"Unexpected error in database session: {e}")
            raise
        finally:
            session.close()

    @asynccontextmanager
    async def get_async_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Get an async database session with automatic cleanup.
        
        Yields:
            SQLAlchemy AsyncSession instance
            
        Raises:
            DatabaseError: If session creation or operation fails
        """
        session = self.async_session_maker()
        try:
            yield session
        except SQLAlchemyError as e:
            await session.rollback()
            self.logger.error(f"Async database session error: {e}")
            raise DatabaseError(f"Async database operation failed: {e}") from e
        except Exception as e:
            await session.rollback()
            self.logger.error(f"Unexpected error in async database session: {e}")
            raise
        finally:
            await session.close()

    @contextmanager
    def transaction(self) -> Generator[Session, None, None]:
        """Execute operations within a database transaction.
        
        Yields:
            SQLAlchemy Session instance within a transaction
            
        Raises:
            TransactionError: If transaction fails
        """
        with self.get_session() as session:
            trans = session.begin()
            try:
                yield session
                trans.commit()
                self.logger.debug("Transaction committed successfully")
            except Exception as e:
                trans.rollback()
                self.logger.error(f"Transaction rolled back due to error: {e}")
                raise TransactionError(f"Transaction failed: {e}") from e

    @asynccontextmanager
    async def async_transaction(self) -> AsyncGenerator[AsyncSession, None]:
        """Execute operations within an async database transaction.
        
        Yields:
            SQLAlchemy AsyncSession instance within a transaction
            
        Raises:
            TransactionError: If transaction fails
        """
        async with self.get_async_session() as session:
            trans = await session.begin()
            try:
                yield session
                await trans.commit()
                self.logger.debug("Async transaction committed successfully")
            except Exception as e:
                await trans.rollback()
                self.logger.error(f"Async transaction rolled back due to error: {e}")
                raise TransactionError(f"Async transaction failed: {e}") from e

    def execute(self, query: Union[str, Any], params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute a SQL query with retry logic.
        
        Args:
            query: SQL query string or SQLAlchemy construct
            params: Optional query parameters
            
        Returns:
            Query result
            
        Raises:
            DatabaseError: If query execution fails
        """
        retries = 0
        max_retries = self.config.connect_retries
        delay = self.config.connect_retry_delay
        
        while retries <= max_retries:
            try:
                with self.get_session() as session:
                    if isinstance(query, str):
                        result = session.execute(text(query), params or {})
                    else:
                        result = session.execute(query, params or {})
                    session.commit()
                    return result
                    
            except (OperationalError, DisconnectionError) as e:
                retries += 1
                if retries > max_retries:
                    self.logger.error(f"Query execution failed after {retries} attempts: {e}")
                    raise DatabaseError(f"Query execution failed: {e}") from e
                
                self.logger.warning(
                    f"Query execution attempt {retries} failed: {e}. "
                    f"Retrying in {delay} seconds..."
                )
                time.sleep(delay)
                delay *= self.config.connect_retry_backoff
                
                # Recreate engine on connection errors
                self._engine = None
                
            except SQLAlchemyError as e:
                self.logger.error(f"SQL error: {e}")
                raise DatabaseError(f"SQL execution failed: {e}") from e

    async def async_execute(self, query: Union[str, Any], params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute an async SQL query with retry logic.
        
        Args:
            query: SQL query string or SQLAlchemy construct
            params: Optional query parameters
            
        Returns:
            Query result
            
        Raises:
            DatabaseError: If query execution fails
        """
        retries = 0
        max_retries = self.config.connect_retries
        delay = self.config.connect_retry_delay
        
        while retries <= max_retries:
            try:
                async with self.get_async_session() as session:
                    if isinstance(query, str):
                        result = await session.execute(text(query), params or {})
                    else:
                        result = await session.execute(query, params or {})
                    await session.commit()
                    return result
                    
            except (OperationalError, DisconnectionError) as e:
                retries += 1
                if retries > max_retries:
                    self.logger.error(f"Async query execution failed after {retries} attempts: {e}")
                    raise DatabaseError(f"Async query execution failed: {e}") from e
                
                self.logger.warning(
                    f"Async query execution attempt {retries} failed: {e}. "
                    f"Retrying in {delay} seconds..."
                )
                await asyncio.sleep(delay)
                delay *= self.config.connect_retry_backoff
                
                # Recreate engine on connection errors
                self._async_engine = None
                
            except SQLAlchemyError as e:
                self.logger.error(f"Async SQL error: {e}")
                raise DatabaseError(f"Async SQL execution failed: {e}") from e

    def health_check(self) -> bool:
        """Perform a health check on the database connection.
        
        Returns:
            True if database is healthy, False otherwise
        """
        current_time = time.time()
        
        # Check if we need to perform a health check
        if (current_time - self._last_health_check) < self.config.health_check_interval:
            return self._is_healthy
        
        try:
            with self.engine.connect() as conn:
                # Set timeout for health check
                conn = conn.execution_options(timeout=self.config.health_check_timeout)
                
                if self.config.database_type == "sqlite":
                    conn.execute(text("SELECT 1"))
                else:
                    conn.execute(text("SELECT 1 as health_check"))
                
            self._is_healthy = True
            self._last_health_check = current_time
            self.logger.debug("Database health check passed")
            
        except Exception as e:
            self._is_healthy = False
            self.logger.warning(f"Database health check failed: {e}")
        
        return self._is_healthy

    async def async_health_check(self) -> bool:
        """Perform an async health check on the database connection.
        
        Returns:
            True if database is healthy, False otherwise
        """
        current_time = time.time()
        
        # Check if we need to perform a health check
        if (current_time - self._last_health_check) < self.config.health_check_interval:
            return self._is_healthy
        
        try:
            async with self.async_engine.connect() as conn:
                if self.config.database_type == "sqlite":
                    await conn.execute(text("SELECT 1"))
                else:
                    await conn.execute(text("SELECT 1 as health_check"))
                
            self._is_healthy = True
            self._last_health_check = current_time
            self.logger.debug("Async database health check passed")
            
        except Exception as e:
            self._is_healthy = False
            self.logger.warning(f"Async database health check failed: {e}")
        
        return self._is_healthy

    def get_connection_info(self) -> Dict[str, Any]:
        """Get information about the database connection.
        
        Returns:
            Dictionary containing connection information
        """
        info = {
            "database_type": self.config.database_type,
            "is_healthy": self._is_healthy,
            "last_health_check": self._last_health_check,
            "pool_size": self.config.pool_size,
            "max_overflow": self.config.max_overflow,
        }
        
        if self._engine:
            try:
                pool = self._engine.pool
                info.update({
                    "pool_checked_in": pool.checkedin(),
                    "pool_checked_out": pool.checkedout(),
                    "pool_overflow": pool.overflow(),
                })
            except AttributeError:
                # Some pool types don't have these methods
                pass
        
        return info

    def close(self) -> None:
        """Close database connections and cleanup resources."""
        if self._engine:
            self._engine.dispose()
            self._engine = None
            self.logger.info("Database engine disposed")
        
        if self._async_engine:
            # Note: async engine disposal should be done in async context
            # This is a synchronous cleanup for emergency cases
            self._async_engine = None

    async def async_close(self) -> None:
        """Close async database connections and cleanup resources."""
        if self._async_engine:
            await self._async_engine.dispose()
            self._async_engine = None
            self.logger.info("Async database engine disposed")
        
        # Also close sync engine if it exists
        self.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        self.close()

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit with cleanup."""
        await self.async_close()