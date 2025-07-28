"""
Alembic environment configuration for GPT Trader database migrations.

This file configures the Alembic migration environment with proper
SQLAlchemy models, PostgreSQL-specific features, and schema support.
"""

import os
import sys
from logging.config import fileConfig
from pathlib import Path

from sqlalchemy import engine_from_config, pool
from alembic import context

# Add project root to path to import models
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Import all models to ensure they're registered with SQLAlchemy
from database.models import Base

# This is the Alembic Config object
config = context.config

# Interpret the config file for Python logging
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Set target metadata for autogenerate support
target_metadata = Base.metadata

# Set schemas to include in migration
SCHEMAS_TO_INCLUDE = ['edgar', 'trading', 'audit']


def get_database_url():
    """Get database URL from environment variable or config."""
    db_url = os.environ.get('DATABASE_URL')
    if db_url:
        return db_url
    
    # Fallback to config file or default
    return config.get_main_option("sqlalchemy.url")


def include_object(object, name, type_, reflected, compare_to):
    """
    Filter objects to include in migrations.
    
    This function determines which database objects should be included
    in the migration process, filtering by schema and object type.
    """
    # Include all objects from our schemas
    if hasattr(object, 'schema') and object.schema in SCHEMAS_TO_INCLUDE:
        return True
    
    # Include objects without schema (like types, functions)
    if not hasattr(object, 'schema') or object.schema is None:
        # Skip pg_* and information_schema objects
        if name and (name.startswith('pg_') or name.startswith('information_schema')):
            return False
        return True
    
    # Exclude everything else
    return False


def compare_type(context, inspected_column, metadata_column, inspected_type, metadata_type):
    """
    Compare column types for migration detection.
    
    Returns True if the types are different and a migration is needed.
    """
    # Handle PostgreSQL-specific type comparisons
    if hasattr(metadata_type, 'python_type'):
        # Handle UUID types
        if str(inspected_type).lower() == 'uuid' and str(metadata_type).lower().startswith('uuid'):
            return False
        
        # Handle JSON/JSONB types
        if str(inspected_type).lower() in ('json', 'jsonb') and str(metadata_type).lower() in ('json', 'jsonb'):
            return False
    
    # Default comparison
    return context.impl.compare_type(inspected_column, metadata_column)


def run_migrations_offline():
    """
    Run migrations in 'offline' mode.

    This configures the context with just a URL and not an Engine,
    though an Engine is acceptable here as well. By skipping the Engine
    creation we don't even need a DBAPI to be available.
    """
    url = get_database_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_schemas=True,
        include_object=include_object,
        compare_type=compare_type,
        render_as_batch=False,  # PostgreSQL supports transactional DDL
        transactional_ddl=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """
    Run migrations in 'online' mode.

    In this scenario we need to create an Engine and associate a connection
    with the context. This is the more common way to run migrations.
    """
    # Override the database URL if provided via environment
    config_section = config.get_section(config.config_ini_section)
    if config_section is None:
        config_section = {}
    
    database_url = get_database_url()
    if database_url:
        config_section['sqlalchemy.url'] = database_url

    connectable = engine_from_config(
        config_section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_schemas=True,
            include_object=include_object,
            compare_type=compare_type,
            render_as_batch=False,  # PostgreSQL supports transactional DDL
            transactional_ddl=True,
            # PostgreSQL-specific configuration
            version_table_schema='trading',  # Store version table in trading schema
        )

        with context.begin_transaction():
            # Create schemas if they don't exist
            for schema in SCHEMAS_TO_INCLUDE:
                connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            
            context.run_migrations()


# Determine which migration mode to use
if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()