"""Database migration system for schema management."""

import hashlib
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table, text
    from sqlalchemy.exc import SQLAlchemyError
    from sqlalchemy.orm import Session
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False

from .database_config import DatabaseConfig
from .database_manager import DatabaseManager, DatabaseError
from .database_session import SessionManager


class MigrationError(DatabaseError):
    """Raised when migration operations fail."""
    pass


class Migration:
    """Represents a database migration."""

    def __init__(
        self,
        version: str,
        name: str,
        up_sql: str,
        down_sql: str = "",
        description: str = "",
        checksum: Optional[str] = None
    ):
        """Initialize migration.
        
        Args:
            version: Migration version (e.g., "001", "20240101_120000")
            name: Migration name
            up_sql: SQL for applying the migration
            down_sql: SQL for reverting the migration
            description: Optional migration description
            checksum: Optional migration checksum
        """
        self.version = version
        self.name = name
        self.up_sql = up_sql
        self.down_sql = down_sql
        self.description = description
        self.checksum = checksum or self._calculate_checksum()
        self.created_at = datetime.utcnow()

    def _calculate_checksum(self) -> str:
        """Calculate migration checksum based on content."""
        content = f"{self.version}{self.name}{self.up_sql}{self.down_sql}"
        return hashlib.sha256(content.encode('utf-8')).hexdigest()[:16]

    def __repr__(self) -> str:
        return f"Migration(version='{self.version}', name='{self.name}')"


class MigrationManager:
    """Manages database schema migrations."""

    def __init__(self, db_manager: DatabaseManager, session_manager: SessionManager):
        """Initialize migration manager.
        
        Args:
            db_manager: Database manager instance
            session_manager: Session manager instance
        """
        if not SQLALCHEMY_AVAILABLE:
            raise ImportError(
                "SQLAlchemy is required for migration management. "
                "Install with: pip install sqlalchemy"
            )
        
        self.db_manager = db_manager
        self.session_manager = session_manager
        self.config = db_manager.config
        self.logger = logging.getLogger(__name__)
        
        # Migration directory
        self.migration_dir = Path(self.config.migration_directory)
        self.migration_dir.mkdir(parents=True, exist_ok=True)
        
        # Migration table metadata
        self._migration_table = None

    @property
    def migration_table(self) -> Table:
        """Get or create the migration table metadata."""
        if self._migration_table is None:
            metadata = MetaData()
            self._migration_table = Table(
                self.config.migration_table,
                metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('version', String(255), nullable=False, unique=True),
                Column('name', String(255), nullable=False),
                Column('checksum', String(32), nullable=False),
                Column('applied_at', DateTime, nullable=False, default=datetime.utcnow),
                Column('execution_time', Integer, nullable=True),  # milliseconds
                Column('description', String(1000), nullable=True),
            )
        return self._migration_table

    def initialize(self) -> None:
        """Initialize the migration system by creating the migration table."""
        try:
            # Create migration table if it doesn't exist
            with self.session_manager.transaction_scope() as session:
                # Check if table exists
                if self.config.database_type == "sqlite":
                    result = session.execute(text(
                        "SELECT name FROM sqlite_master "
                        "WHERE type='table' AND name=:table_name"
                    ), {"table_name": self.config.migration_table})
                else:
                    result = session.execute(text(
                        "SELECT table_name FROM information_schema.tables "
                        "WHERE table_name=:table_name AND table_schema=:schema"
                    ), {
                        "table_name": self.config.migration_table,
                        "schema": self.config.postgres_schema
                    })
                
                if not result.fetchone():
                    # Create the migration table
                    self.migration_table.create(self.db_manager.engine)
                    self.logger.info(f"Created migration table: {self.config.migration_table}")
                else:
                    self.logger.debug(f"Migration table already exists: {self.config.migration_table}")
                    
        except SQLAlchemyError as e:
            self.logger.error(f"Failed to initialize migration system: {e}")
            raise MigrationError(f"Failed to initialize migration system: {e}") from e

    def create_migration(
        self,
        name: str,
        up_sql: str = "",
        down_sql: str = "",
        description: str = ""
    ) -> Migration:
        """Create a new migration file.
        
        Args:
            name: Migration name
            up_sql: SQL for applying the migration
            down_sql: SQL for reverting the migration
            description: Optional migration description
            
        Returns:
            Created Migration instance
        """
        # Generate version based on timestamp
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        
        # Sanitize name for filename
        safe_name = re.sub(r'[^\w\-_]', '_', name.lower())
        version = f"{timestamp}_{safe_name}"
        
        migration = Migration(
            version=version,
            name=name,
            up_sql=up_sql,
            down_sql=down_sql,
            description=description
        )
        
        # Create migration file
        migration_file = self.migration_dir / f"{version}.sql"
        migration_content = self._generate_migration_file_content(migration)
        
        migration_file.write_text(migration_content, encoding='utf-8')
        self.logger.info(f"Created migration file: {migration_file}")
        
        return migration

    def _generate_migration_file_content(self, migration: Migration) -> str:
        """Generate migration file content.
        
        Args:
            migration: Migration instance
            
        Returns:
            Migration file content
        """
        content = f"""-- Migration: {migration.name}
-- Version: {migration.version}
-- Description: {migration.description}
-- Created: {migration.created_at.isoformat()}
-- Checksum: {migration.checksum}

-- +migrate Up
{migration.up_sql if migration.up_sql else "-- Add your UP migration SQL here"}

-- +migrate Down
{migration.down_sql if migration.down_sql else "-- Add your DOWN migration SQL here"}
"""
        return content

    def load_migrations(self) -> List[Migration]:
        """Load all migration files from the migration directory.
        
        Returns:
            List of Migration instances sorted by version
        """
        migrations = []
        
        for migration_file in sorted(self.migration_dir.glob("*.sql")):
            try:
                migration = self._parse_migration_file(migration_file)
                migrations.append(migration)
            except Exception as e:
                self.logger.warning(f"Failed to parse migration file {migration_file}: {e}")
        
        return sorted(migrations, key=lambda m: m.version)

    def _parse_migration_file(self, file_path: Path) -> Migration:
        """Parse a migration file.
        
        Args:
            file_path: Path to migration file
            
        Returns:
            Migration instance
        """
        content = file_path.read_text(encoding='utf-8')
        
        # Extract metadata from comments
        version_match = re.search(r'-- Version: (.+)', content)
        name_match = re.search(r'-- Migration: (.+)', content)
        description_match = re.search(r'-- Description: (.+)', content)
        checksum_match = re.search(r'-- Checksum: (.+)', content)
        
        version = version_match.group(1).strip() if version_match else file_path.stem
        name = name_match.group(1).strip() if name_match else file_path.stem
        description = description_match.group(1).strip() if description_match else ""
        stored_checksum = checksum_match.group(1).strip() if checksum_match else None
        
        # Extract UP and DOWN SQL
        up_match = re.search(r'-- \+migrate Up\s*\n(.*?)(?=-- \+migrate Down|\Z)', content, re.DOTALL)
        down_match = re.search(r'-- \+migrate Down\s*\n(.*)', content, re.DOTALL)
        
        up_sql = up_match.group(1).strip() if up_match else ""
        down_sql = down_match.group(1).strip() if down_match else ""
        
        migration = Migration(
            version=version,
            name=name,
            up_sql=up_sql,
            down_sql=down_sql,
            description=description
        )
        
        # Verify checksum if stored
        if stored_checksum and migration.checksum != stored_checksum:
            self.logger.warning(
                f"Checksum mismatch for migration {version}: "
                f"expected {stored_checksum}, got {migration.checksum}"
            )
        
        return migration

    def get_applied_migrations(self) -> List[str]:
        """Get list of applied migration versions.
        
        Returns:
            List of applied migration versions
        """
        try:
            with self.session_manager.session_scope() as session:
                result = session.execute(text(
                    f"SELECT version FROM {self.config.migration_table} ORDER BY version"
                ))
                return [row[0] for row in result.fetchall()]
                
        except SQLAlchemyError as e:
            self.logger.error(f"Failed to get applied migrations: {e}")
            raise MigrationError(f"Failed to get applied migrations: {e}") from e

    def get_pending_migrations(self) -> List[Migration]:
        """Get list of pending migrations.
        
        Returns:
            List of Migration instances that haven't been applied
        """
        all_migrations = self.load_migrations()
        applied_versions = set(self.get_applied_migrations())
        
        return [m for m in all_migrations if m.version not in applied_versions]

    def apply_migration(self, migration: Migration) -> None:
        """Apply a single migration.
        
        Args:
            migration: Migration to apply
            
        Raises:
            MigrationError: If migration fails
        """
        start_time = datetime.utcnow()
        
        try:
            with self.session_manager.transaction_scope() as session:
                # Check if migration is already applied
                result = session.execute(text(
                    f"SELECT COUNT(*) FROM {self.config.migration_table} WHERE version = :version"
                ), {"version": migration.version})
                
                if result.scalar() > 0:
                    self.logger.warning(f"Migration {migration.version} is already applied")
                    return
                
                # Execute migration SQL
                if migration.up_sql.strip():
                    # Split SQL into individual statements
                    statements = self._split_sql(migration.up_sql)
                    
                    for statement in statements:
                        if statement.strip():
                            session.execute(text(statement))
                
                # Record migration as applied
                execution_time = int((datetime.utcnow() - start_time).total_seconds() * 1000)
                session.execute(text(
                    f"INSERT INTO {self.config.migration_table} "
                    "(version, name, checksum, applied_at, execution_time, description) "
                    "VALUES (:version, :name, :checksum, :applied_at, :execution_time, :description)"
                ), {
                    "version": migration.version,
                    "name": migration.name,
                    "checksum": migration.checksum,
                    "applied_at": datetime.utcnow(),
                    "execution_time": execution_time,
                    "description": migration.description
                })
                
                self.logger.info(
                    f"Applied migration {migration.version}: {migration.name} "
                    f"(execution time: {execution_time}ms)"
                )
                
        except SQLAlchemyError as e:
            self.logger.error(f"Failed to apply migration {migration.version}: {e}")
            raise MigrationError(f"Failed to apply migration {migration.version}: {e}") from e

    def rollback_migration(self, migration: Migration) -> None:
        """Rollback a single migration.
        
        Args:
            migration: Migration to rollback
            
        Raises:
            MigrationError: If rollback fails
        """
        try:
            with self.session_manager.transaction_scope() as session:
                # Check if migration is applied
                result = session.execute(text(
                    f"SELECT COUNT(*) FROM {self.config.migration_table} WHERE version = :version"
                ), {"version": migration.version})
                
                if result.scalar() == 0:
                    self.logger.warning(f"Migration {migration.version} is not applied")
                    return
                
                # Execute rollback SQL
                if migration.down_sql.strip():
                    statements = self._split_sql(migration.down_sql)
                    
                    for statement in statements:
                        if statement.strip():
                            session.execute(text(statement))
                
                # Remove migration record
                session.execute(text(
                    f"DELETE FROM {self.config.migration_table} WHERE version = :version"
                ), {"version": migration.version})
                
                self.logger.info(f"Rolled back migration {migration.version}: {migration.name}")
                
        except SQLAlchemyError as e:
            self.logger.error(f"Failed to rollback migration {migration.version}: {e}")
            raise MigrationError(f"Failed to rollback migration {migration.version}: {e}") from e

    def migrate(self, target_version: Optional[str] = None) -> None:
        """Apply all pending migrations up to target version.
        
        Args:
            target_version: Optional target version. If None, applies all pending migrations
        """
        self.initialize()
        
        pending_migrations = self.get_pending_migrations()
        
        if target_version:
            # Filter migrations up to target version
            pending_migrations = [m for m in pending_migrations if m.version <= target_version]
        
        if not pending_migrations:
            self.logger.info("No pending migrations to apply")
            return
        
        self.logger.info(f"Applying {len(pending_migrations)} migrations...")
        
        for migration in pending_migrations:
            try:
                self.apply_migration(migration)
            except MigrationError:
                self.logger.error(f"Migration failed. Stopping at {migration.version}")
                raise
        
        self.logger.info("All migrations applied successfully")

    def rollback(self, target_version: Optional[str] = None, steps: Optional[int] = None) -> None:
        """Rollback migrations.
        
        Args:
            target_version: Rollback to this version (exclusive)
            steps: Number of migrations to rollback (if target_version not specified)
        """
        applied_versions = self.get_applied_migrations()
        all_migrations = {m.version: m for m in self.load_migrations()}
        
        if target_version:
            # Rollback to target version
            to_rollback = [v for v in reversed(applied_versions) if v > target_version]
        elif steps:
            # Rollback specified number of steps
            to_rollback = list(reversed(applied_versions))[:steps]
        else:
            # Rollback one step
            to_rollback = list(reversed(applied_versions))[:1]
        
        if not to_rollback:
            self.logger.info("No migrations to rollback")
            return
        
        self.logger.info(f"Rolling back {len(to_rollback)} migrations...")
        
        for version in to_rollback:
            migration = all_migrations.get(version)
            if migration:
                try:
                    self.rollback_migration(migration)
                except MigrationError:
                    self.logger.error(f"Rollback failed. Stopping at {version}")
                    raise
            else:
                self.logger.warning(f"Migration file not found for version {version}")
        
        self.logger.info("Rollback completed successfully")

    def status(self) -> Dict[str, Any]:
        """Get migration status.
        
        Returns:
            Dictionary containing migration status information
        """
        all_migrations = self.load_migrations()
        applied_versions = set(self.get_applied_migrations())
        
        status = {
            "total_migrations": len(all_migrations),
            "applied_migrations": len(applied_versions),
            "pending_migrations": len(all_migrations) - len(applied_versions),
            "migrations": []
        }
        
        for migration in all_migrations:
            status["migrations"].append({
                "version": migration.version,
                "name": migration.name,
                "description": migration.description,
                "applied": migration.version in applied_versions,
                "checksum": migration.checksum
            })
        
        return status

    def _split_sql(self, sql: str) -> List[str]:
        """Split SQL content into individual statements.
        
        Args:
            sql: SQL content
            
        Returns:
            List of SQL statements
        """
        # Simple SQL statement splitting (handles most cases)
        # For more complex SQL, consider using a proper SQL parser
        statements = []
        current_statement = []
        
        for line in sql.split('\n'):
            line = line.strip()
            
            # Skip comments and empty lines
            if not line or line.startswith('--'):
                continue
            
            current_statement.append(line)
            
            # Check if statement ends with semicolon
            if line.endswith(';'):
                statements.append('\n'.join(current_statement))
                current_statement = []
        
        # Add any remaining statement
        if current_statement:
            statement = '\n'.join(current_statement).strip()
            if statement:
                statements.append(statement)
        
        return statements

    def validate_migrations(self) -> Tuple[bool, List[str]]:
        """Validate all migration files.
        
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        migrations = self.load_migrations()
        
        # Check for duplicate versions
        versions = [m.version for m in migrations]
        if len(versions) != len(set(versions)):
            errors.append("Duplicate migration versions found")
        
        # Validate each migration
        for migration in migrations:
            if not migration.up_sql.strip():
                errors.append(f"Migration {migration.version} has empty UP SQL")
            
            # Check for common SQL issues
            if 'DROP TABLE' in migration.up_sql.upper() and not migration.down_sql.strip():
                errors.append(f"Migration {migration.version} drops table but has no DOWN SQL")
        
        return len(errors) == 0, errors