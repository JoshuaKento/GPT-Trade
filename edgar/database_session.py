"""Database session management and transaction handling utilities."""

import functools
import logging
from contextlib import contextmanager
from typing import Any, Callable, Dict, Generator, Optional, TypeVar, Union

try:
    from sqlalchemy.orm import Session
    from sqlalchemy.exc import SQLAlchemyError
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False

from .database_config import DatabaseConfig
from .database_manager import DatabaseManager, DatabaseError, TransactionError


F = TypeVar('F', bound=Callable[..., Any])


class SessionManager:
    """Manages database sessions and provides transaction utilities."""

    def __init__(self, db_manager: DatabaseManager):
        """Initialize session manager.
        
        Args:
            db_manager: Database manager instance
        """
        if not SQLALCHEMY_AVAILABLE:
            raise ImportError(
                "SQLAlchemy is required for session management. "
                "Install with: pip install sqlalchemy"
            )
        
        self.db_manager = db_manager
        self.config = db_manager.config
        self.logger = logging.getLogger(__name__)

    @contextmanager
    def session_scope(self) -> Generator[Session, None, None]:
        """Provide a transactional scope around a series of operations.
        
        This is the recommended way to handle database sessions. It ensures
        that the session is properly committed or rolled back and closed.
        
        Yields:
            Session: SQLAlchemy session instance
            
        Example:
            with session_manager.session_scope() as session:
                user = session.query(User).filter_by(id=1).first()
                user.name = "Updated Name"
                # Session is automatically committed and closed
        """
        session = self.db_manager.session_maker()
        try:
            yield session
            session.commit()
            self.logger.debug("Session committed successfully")
        except Exception as e:
            session.rollback()
            self.logger.error(f"Session rolled back due to error: {e}")
            raise
        finally:
            session.close()

    @contextmanager
    def transaction_scope(self) -> Generator[Session, None, None]:
        """Provide explicit transaction control with savepoints.
        
        This provides more granular control over transactions and supports
        nested transactions using savepoints.
        
        Yields:
            Session: SQLAlchemy session instance within a transaction
            
        Example:
            with session_manager.transaction_scope() as session:
                # Operations here are within an explicit transaction
                user = User(name="New User")
                session.add(user)
                # Transaction is committed when context exits
        """
        with self.db_manager.transaction() as session:
            yield session

    def with_session(self, func: F) -> F:
        """Decorator that provides a database session as the first argument.
        
        Args:
            func: Function to decorate
            
        Returns:
            Decorated function with session injection
            
        Example:
            @session_manager.with_session
            def get_user(session, user_id):
                return session.query(User).filter_by(id=user_id).first()
        """
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with self.session_scope() as session:
                return func(session, *args, **kwargs)
        return wrapper

    def with_transaction(self, func: F) -> F:
        """Decorator that provides a transactional database session.
        
        Args:
            func: Function to decorate
            
        Returns:
            Decorated function with transaction handling
            
        Example:
            @session_manager.with_transaction
            def create_user(session, name, email):
                user = User(name=name, email=email)
                session.add(user)
                return user
        """
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with self.transaction_scope() as session:
                return func(session, *args, **kwargs)
        return wrapper

    def retry_on_database_error(self, max_retries: int = 3, delay: float = 1.0) -> Callable:
        """Decorator that retries database operations on transient errors.
        
        Args:
            max_retries: Maximum number of retry attempts
            delay: Initial delay between retries in seconds
            
        Returns:
            Decorator function
            
        Example:
            @session_manager.retry_on_database_error(max_retries=3)
            @session_manager.with_session
            def get_user_with_retry(session, user_id):
                return session.query(User).filter_by(id=user_id).first()
        """
        def decorator(func: F) -> F:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                retries = 0
                current_delay = delay
                
                while retries <= max_retries:
                    try:
                        return func(*args, **kwargs)
                    except (DatabaseError, SQLAlchemyError) as e:
                        retries += 1
                        if retries > max_retries:
                            self.logger.error(
                                f"Function {func.__name__} failed after {retries} attempts: {e}"
                            )
                            raise
                        
                        self.logger.warning(
                            f"Function {func.__name__} attempt {retries} failed: {e}. "
                            f"Retrying in {current_delay} seconds..."
                        )
                        
                        import time
                        time.sleep(current_delay)
                        current_delay *= self.config.connect_retry_backoff
                        
            return wrapper
        return decorator

    def execute_in_session(self, query: Union[str, Any], params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute a query within a managed session.
        
        Args:
            query: SQL query string or SQLAlchemy construct
            params: Optional query parameters
            
        Returns:
            Query result
            
        Raises:
            DatabaseError: If query execution fails
        """
        with self.session_scope() as session:
            try:
                if isinstance(query, str):
                    from sqlalchemy import text
                    result = session.execute(text(query), params or {})
                else:
                    result = session.execute(query, params or {})
                return result
            except SQLAlchemyError as e:
                self.logger.error(f"Query execution failed: {e}")
                raise DatabaseError(f"Query execution failed: {e}") from e

    def execute_in_transaction(self, query: Union[str, Any], params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute a query within a managed transaction.
        
        Args:
            query: SQL query string or SQLAlchemy construct
            params: Optional query parameters
            
        Returns:
            Query result
            
        Raises:
            TransactionError: If query execution fails
        """
        with self.transaction_scope() as session:
            try:
                if isinstance(query, str):
                    from sqlalchemy import text
                    result = session.execute(text(query), params or {})
                else:
                    result = session.execute(query, params or {})
                return result
            except SQLAlchemyError as e:
                self.logger.error(f"Transaction query execution failed: {e}")
                raise TransactionError(f"Transaction query execution failed: {e}") from e

    def bulk_insert(self, model_class: Any, data: list, batch_size: int = 1000) -> None:
        """Perform bulk insert operations efficiently.
        
        Args:
            model_class: SQLAlchemy model class
            data: List of dictionaries containing data to insert
            batch_size: Number of records to insert in each batch
            
        Raises:
            DatabaseError: If bulk insert fails
        """
        if not data:
            return

        with self.transaction_scope() as session:
            try:
                for i in range(0, len(data), batch_size):
                    batch = data[i:i + batch_size]
                    session.bulk_insert_mappings(model_class, batch)
                    
                    self.logger.debug(
                        f"Inserted batch {i // batch_size + 1}: "
                        f"{len(batch)} records"
                    )
                
                self.logger.info(f"Bulk insert completed: {len(data)} records")
                
            except SQLAlchemyError as e:
                self.logger.error(f"Bulk insert failed: {e}")
                raise DatabaseError(f"Bulk insert failed: {e}") from e

    def bulk_update(self, model_class: Any, data: list, batch_size: int = 1000) -> None:
        """Perform bulk update operations efficiently.
        
        Args:
            model_class: SQLAlchemy model class
            data: List of dictionaries containing data to update
            batch_size: Number of records to update in each batch
            
        Raises:
            DatabaseError: If bulk update fails
        """
        if not data:
            return

        with self.transaction_scope() as session:
            try:
                for i in range(0, len(data), batch_size):
                    batch = data[i:i + batch_size]
                    session.bulk_update_mappings(model_class, batch)
                    
                    self.logger.debug(
                        f"Updated batch {i // batch_size + 1}: "
                        f"{len(batch)} records"
                    )
                
                self.logger.info(f"Bulk update completed: {len(data)} records")
                
            except SQLAlchemyError as e:
                self.logger.error(f"Bulk update failed: {e}")
                raise DatabaseError(f"Bulk update failed: {e}") from e

    def execute_script(self, script_path: str, params: Optional[Dict[str, Any]] = None) -> None:
        """Execute a SQL script file.
        
        Args:
            script_path: Path to SQL script file
            params: Optional parameters for script execution
            
        Raises:
            DatabaseError: If script execution fails
            FileNotFoundError: If script file doesn't exist
        """
        from pathlib import Path
        
        script_file = Path(script_path)
        if not script_file.exists():
            raise FileNotFoundError(f"SQL script not found: {script_path}")
        
        try:
            script_content = script_file.read_text(encoding='utf-8')
            
            # Split script into individual statements
            statements = [stmt.strip() for stmt in script_content.split(';') if stmt.strip()]
            
            with self.transaction_scope() as session:
                for statement in statements:
                    if statement:
                        from sqlalchemy import text
                        session.execute(text(statement), params or {})
                        
            self.logger.info(f"Executed SQL script: {script_path} ({len(statements)} statements)")
            
        except Exception as e:
            self.logger.error(f"Failed to execute SQL script {script_path}: {e}")
            raise DatabaseError(f"Failed to execute SQL script: {e}") from e

    def get_session_stats(self) -> Dict[str, Any]:
        """Get statistics about session usage.
        
        Returns:
            Dictionary containing session statistics
        """
        stats = {
            "database_type": self.config.database_type,
            "pool_size": self.config.pool_size,
            "max_overflow": self.config.max_overflow,
            "is_healthy": self.db_manager.health_check(),
        }
        
        # Add connection pool stats if available
        connection_info = self.db_manager.get_connection_info()
        stats.update(connection_info)
        
        return stats

    def validate_session(self, session: Session) -> bool:
        """Validate that a session is still active and healthy.
        
        Args:
            session: SQLAlchemy session to validate
            
        Returns:
            True if session is valid, False otherwise
        """
        try:
            # Try to execute a simple query
            from sqlalchemy import text
            session.execute(text("SELECT 1"))
            return True
        except Exception as e:
            self.logger.warning(f"Session validation failed: {e}")
            return False

    def refresh_connection(self) -> None:
        """Refresh the database connection pool.
        
        This can be useful when connections have been idle for a long time
        or when recovering from connection issues.
        """
        try:
            # Force recreation of the engine
            self.db_manager._engine = None
            
            # Test the new connection
            with self.session_scope() as session:
                from sqlalchemy import text
                session.execute(text("SELECT 1"))
                
            self.logger.info("Database connection refreshed successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to refresh database connection: {e}")
            raise DatabaseError(f"Failed to refresh connection: {e}") from e

    def close(self) -> None:
        """Close the session manager and underlying database manager."""
        self.db_manager.close()
        self.logger.info("Session manager closed")


class TransactionManager:
    """Utility class for advanced transaction management."""

    def __init__(self, session_manager: SessionManager):
        """Initialize transaction manager.
        
        Args:
            session_manager: Session manager instance
        """
        self.session_manager = session_manager
        self.logger = logging.getLogger(__name__)

    @contextmanager
    def savepoint(self, session: Session, name: Optional[str] = None) -> Generator[Any, None, None]:
        """Create a savepoint within an existing transaction.
        
        Args:
            session: Active SQLAlchemy session
            name: Optional savepoint name
            
        Yields:
            Savepoint object
            
        Example:
            with session_manager.transaction_scope() as session:
                # Do some work
                with transaction_manager.savepoint(session, "checkpoint1"):
                    # This work can be rolled back to the savepoint
                    risky_operation()
        """
        if name is None:
            import uuid
            name = f"sp_{uuid.uuid4().hex[:8]}"
        
        savepoint = session.begin_nested()
        try:
            self.logger.debug(f"Created savepoint: {name}")
            yield savepoint
            self.logger.debug(f"Savepoint {name} committed")
        except Exception as e:
            savepoint.rollback()
            self.logger.warning(f"Savepoint {name} rolled back due to error: {e}")
            raise

    def execute_with_savepoint(self, session: Session, func: Callable, *args, **kwargs) -> Any:
        """Execute a function within a savepoint.
        
        Args:
            session: Active SQLAlchemy session
            func: Function to execute
            *args: Function positional arguments
            **kwargs: Function keyword arguments
            
        Returns:
            Function result
            
        Raises:
            Exception: Any exception raised by the function
        """
        with self.savepoint(session):
            return func(session, *args, **kwargs)

    def batch_operation(self, operations: list, batch_size: int = 100) -> None:
        """Execute a series of operations in batches with savepoints.
        
        Args:
            operations: List of callable operations
            batch_size: Number of operations per batch
            
        Example:
            operations = [
                lambda session: session.add(User(name=f"User {i}"))
                for i in range(1000)
            ]
            transaction_manager.batch_operation(operations, batch_size=50)
        """
        with self.session_manager.transaction_scope() as session:
            for i in range(0, len(operations), batch_size):
                batch = operations[i:i + batch_size]
                
                with self.savepoint(session, f"batch_{i // batch_size}"):
                    for operation in batch:
                        operation(session)
                    
                    self.logger.debug(
                        f"Completed batch {i // batch_size + 1}: "
                        f"{len(batch)} operations"
                    )
            
            self.logger.info(f"Batch operation completed: {len(operations)} total operations")