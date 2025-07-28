"""Initial database schema for GPT Trader

Revision ID: 20240127_001
Revises: 
Create Date: 2024-01-27 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB, ARRAY

# revision identifiers, used by Alembic.
revision: str = '20240127_001'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Apply initial schema migration."""
    
    # Enable required extensions
    op.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')
    op.execute('CREATE EXTENSION IF NOT EXISTS "pgcrypto"')
    op.execute('CREATE EXTENSION IF NOT EXISTS "pg_trgm"')
    
    # Create schemas
    op.execute('CREATE SCHEMA IF NOT EXISTS edgar')
    op.execute('CREATE SCHEMA IF NOT EXISTS trading')
    op.execute('CREATE SCHEMA IF NOT EXISTS audit')
    
    # =============================================================================
    # EDGAR SCHEMA TABLES
    # =============================================================================
    
    # Companies table
    op.create_table(
        'companies',
        sa.Column('id', UUID(as_uuid=True), primary_key=True, 
                  server_default=sa.text('uuid_generate_v4()'), nullable=False),
        sa.Column('cik', sa.String(10), nullable=False),
        sa.Column('ticker', sa.String(10), nullable=True),
        sa.Column('company_name', sa.Text, nullable=False),
        sa.Column('sic', sa.String(4), nullable=True),
        sa.Column('sector', sa.String(100), nullable=True),
        sa.Column('industry', sa.String(200), nullable=True),
        sa.Column('exchange', sa.String(10), nullable=True),
        sa.Column('state_of_incorporation', sa.String(2), nullable=True),
        sa.Column('fiscal_year_end', sa.String(4), nullable=True),
        sa.Column('market_cap', sa.BigInteger, nullable=True),
        sa.Column('employees', sa.Integer, nullable=True),
        sa.Column('is_active', sa.Boolean, nullable=False, default=True),
        sa.Column('processing_enabled', sa.Boolean, nullable=False, default=True),
        sa.Column('last_filing_check', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, 
                  server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False, 
                  server_default=sa.func.now()),
        sa.Column('created_by', sa.String(100), default='system'),
        sa.Column('updated_by', sa.String(100), default='system'),
        sa.UniqueConstraint('cik', name='uq_companies_cik'),
        schema='edgar'
    )
    
    # Filing types lookup table
    op.create_table(
        'filing_types',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('form_type', sa.String(20), nullable=False),
        sa.Column('description', sa.Text, nullable=True),
        sa.Column('priority', sa.Integer, nullable=False, default=10),
        sa.Column('retention_days', sa.Integer, default=2555),
        sa.Column('is_active', sa.Boolean, nullable=False, default=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, 
                  server_default=sa.func.now()),
        sa.UniqueConstraint('form_type', name='uq_filing_types_form_type'),
        schema='edgar'
    )
    
    # Filings table
    op.create_table(
        'filings',
        sa.Column('id', UUID(as_uuid=True), primary_key=True, 
                  server_default=sa.text('uuid_generate_v4()'), nullable=False),
        sa.Column('company_id', UUID(as_uuid=True), 
                  sa.ForeignKey('edgar.companies.id', ondelete='CASCADE'), nullable=False),
        sa.Column('accession_number', sa.String(20), nullable=False),
        sa.Column('form_type', sa.String(20), nullable=False),
        sa.Column('filing_date', sa.Date, nullable=False),
        sa.Column('report_date', sa.Date, nullable=True),
        sa.Column('acceptance_datetime', sa.DateTime(timezone=True), nullable=True),
        sa.Column('primary_document', sa.String(255), nullable=True),
        sa.Column('document_count', sa.Integer, default=0),
        sa.Column('size_bytes', sa.BigInteger, nullable=True),
        sa.Column('edgar_url', sa.Text, nullable=True),
        sa.Column('s3_bucket', sa.String(100), nullable=True),
        sa.Column('s3_key_prefix', sa.String(500), nullable=True),
        sa.Column('processing_status', sa.String(20), nullable=False, default='discovered'),
        sa.Column('processing_priority', sa.Integer, nullable=False, default=10),
        sa.Column('processing_started_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('processing_completed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('processing_duration_ms', sa.Integer, nullable=True),
        sa.Column('error_count', sa.Integer, nullable=False, default=0),
        sa.Column('last_error_message', sa.Text, nullable=True),
        sa.Column('last_error_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('retry_after', sa.DateTime(timezone=True), nullable=True),
        sa.Column('has_parsed_content', sa.Boolean, nullable=False, default=False),
        sa.Column('has_embeddings', sa.Boolean, nullable=False, default=False),
        sa.Column('text_extraction_method', sa.String(50), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, 
                  server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False, 
                  server_default=sa.func.now()),
        sa.Column('processed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_by', sa.String(100), default='system'),
        sa.Column('updated_by', sa.String(100), default='system'),
        sa.UniqueConstraint('accession_number', name='uq_filings_accession_number'),
        sa.CheckConstraint(
            "processing_status IN ('discovered', 'pending', 'processing', 'completed', 'failed', 'archived')",
            name='check_filing_processing_status'
        ),
        sa.CheckConstraint('processing_priority BETWEEN 1 AND 10', name='check_filing_priority_range'),
        schema='edgar'
    )
    
    # Documents table
    op.create_table(
        'documents',
        sa.Column('id', UUID(as_uuid=True), primary_key=True, 
                  server_default=sa.text('uuid_generate_v4()'), nullable=False),
        sa.Column('filing_id', UUID(as_uuid=True), 
                  sa.ForeignKey('edgar.filings.id', ondelete='CASCADE'), nullable=False),
        sa.Column('document_name', sa.String(255), nullable=False),
        sa.Column('document_type', sa.String(50), nullable=True),
        sa.Column('description', sa.Text, nullable=True),
        sa.Column('sequence', sa.Integer, nullable=True),
        sa.Column('size_bytes', sa.BigInteger, nullable=True),
        sa.Column('s3_bucket', sa.String(100), nullable=True),
        sa.Column('s3_key', sa.String(500), nullable=True),
        sa.Column('local_path', sa.Text, nullable=True),
        sa.Column('content_type', sa.String(100), nullable=True),
        sa.Column('extracted_text_length', sa.Integer, nullable=True),
        sa.Column('has_tables', sa.Boolean, default=False),
        sa.Column('has_images', sa.Boolean, default=False),
        sa.Column('processing_status', sa.String(20), nullable=False, default='pending'),
        sa.Column('extraction_method', sa.String(50), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, 
                  server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False, 
                  server_default=sa.func.now()),
        sa.Column('processed_at', sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "processing_status IN ('pending', 'processing', 'completed', 'failed')",
            name='check_document_processing_status'
        ),
        schema='edgar'
    )
    
    # =============================================================================
    # TRADING SCHEMA TABLES
    # =============================================================================
    
    # Job types table
    op.create_table(
        'job_types',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('name', sa.String(50), nullable=False),
        sa.Column('description', sa.Text, nullable=True),
        sa.Column('default_timeout_minutes', sa.Integer, nullable=False, default=30),
        sa.Column('max_retries', sa.Integer, nullable=False, default=3),
        sa.Column('is_active', sa.Boolean, nullable=False, default=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, 
                  server_default=sa.func.now()),
        sa.UniqueConstraint('name', name='uq_job_types_name'),
        schema='trading'
    )
    
    # Jobs table
    op.create_table(
        'jobs',
        sa.Column('id', UUID(as_uuid=True), primary_key=True, 
                  server_default=sa.text('uuid_generate_v4()'), nullable=False),
        sa.Column('job_type_id', sa.Integer, 
                  sa.ForeignKey('trading.job_types.id'), nullable=False),
        sa.Column('company_id', UUID(as_uuid=True), 
                  sa.ForeignKey('edgar.companies.id'), nullable=True),
        sa.Column('filing_id', UUID(as_uuid=True), 
                  sa.ForeignKey('edgar.filings.id'), nullable=True),
        sa.Column('parameters', JSONB, nullable=True),
        sa.Column('status', sa.String(20), nullable=False, default='pending'),
        sa.Column('priority', sa.Integer, nullable=False, default=10),
        sa.Column('scheduled_at', sa.DateTime(timezone=True), nullable=False, 
                  server_default=sa.func.now()),
        sa.Column('started_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('progress_current', sa.Integer, default=0),
        sa.Column('progress_total', sa.Integer, default=1),
        sa.Column('progress_message', sa.Text, nullable=True),
        sa.Column('worker_id', sa.String(100), nullable=True),
        sa.Column('execution_node', sa.String(100), nullable=True),
        sa.Column('memory_usage_mb', sa.Integer, nullable=True),
        sa.Column('cpu_time_ms', sa.Integer, nullable=True),
        sa.Column('error_message', sa.Text, nullable=True),
        sa.Column('retry_count', sa.Integer, nullable=False, default=0),
        sa.Column('max_retries', sa.Integer, nullable=False, default=3),
        sa.Column('next_retry_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('result_data', JSONB, nullable=True),
        sa.Column('output_files', ARRAY(sa.Text), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, 
                  server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False, 
                  server_default=sa.func.now()),
        sa.Column('created_by', sa.String(100), default='system'),
        sa.CheckConstraint(
            "status IN ('pending', 'running', 'completed', 'failed', 'cancelled')",
            name='check_job_status'
        ),
        sa.CheckConstraint('priority BETWEEN 1 AND 10', name='check_job_priority_range'),
        schema='trading'
    )
    
    # Processing state table
    op.create_table(
        'processing_state',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('key', sa.String(100), nullable=False),
        sa.Column('value', sa.Text, nullable=True),
        sa.Column('value_type', sa.String(20), nullable=False, default='string'),
        sa.Column('description', sa.Text, nullable=True),
        sa.Column('is_system', sa.Boolean, nullable=False, default=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, 
                  server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False, 
                  server_default=sa.func.now()),
        sa.Column('updated_by', sa.String(100), default='system'),
        sa.UniqueConstraint('key', name='uq_processing_state_key'),
        sa.CheckConstraint(
            "value_type IN ('string', 'integer', 'boolean', 'json', 'timestamp')",
            name='check_processing_state_value_type'
        ),
        schema='trading'
    )
    
    # =============================================================================
    # AUDIT SCHEMA TABLES
    # =============================================================================
    
    # Error logs table
    op.create_table(
        'error_logs',
        sa.Column('id', UUID(as_uuid=True), primary_key=True, 
                  server_default=sa.text('uuid_generate_v4()'), nullable=False),
        sa.Column('job_id', UUID(as_uuid=True), 
                  sa.ForeignKey('trading.jobs.id'), nullable=True),
        sa.Column('filing_id', UUID(as_uuid=True), 
                  sa.ForeignKey('edgar.filings.id'), nullable=True),
        sa.Column('company_id', UUID(as_uuid=True), 
                  sa.ForeignKey('edgar.companies.id'), nullable=True),
        sa.Column('error_type', sa.String(50), nullable=False),
        sa.Column('error_code', sa.String(20), nullable=True),
        sa.Column('error_message', sa.Text, nullable=False),
        sa.Column('stack_trace', sa.Text, nullable=True),
        sa.Column('severity', sa.String(10), nullable=False, default='ERROR'),
        sa.Column('category', sa.String(50), nullable=True),
        sa.Column('is_retryable', sa.Boolean, nullable=False, default=True),
        sa.Column('request_data', JSONB, nullable=True),
        sa.Column('response_data', JSONB, nullable=True),
        sa.Column('environment_data', JSONB, nullable=True),
        sa.Column('is_resolved', sa.Boolean, nullable=False, default=False),
        sa.Column('resolved_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('resolution_notes', sa.Text, nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, 
                  server_default=sa.func.now()),
        sa.Column('source_module', sa.String(100), nullable=True),
        sa.Column('source_function', sa.String(100), nullable=True),
        sa.CheckConstraint(
            "severity IN ('DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL')",
            name='check_error_log_severity'
        ),
        schema='audit'
    )
    
    # Performance metrics table
    op.create_table(
        'performance_metrics',
        sa.Column('id', UUID(as_uuid=True), primary_key=True, 
                  server_default=sa.text('uuid_generate_v4()'), nullable=False),
        sa.Column('metric_name', sa.String(100), nullable=False),
        sa.Column('metric_type', sa.String(20), nullable=False),
        sa.Column('value_numeric', sa.DECIMAL(15, 6), nullable=True),
        sa.Column('value_text', sa.Text, nullable=True),
        sa.Column('company_id', UUID(as_uuid=True), 
                  sa.ForeignKey('edgar.companies.id'), nullable=True),
        sa.Column('job_type', sa.String(50), nullable=True),
        sa.Column('source_module', sa.String(100), nullable=True),
        sa.Column('tags', JSONB, nullable=True),
        sa.Column('timestamp', sa.DateTime(timezone=True), nullable=False, 
                  server_default=sa.func.now()),
        sa.Column('metadata', JSONB, nullable=True),
        sa.CheckConstraint(
            "metric_type IN ('counter', 'gauge', 'histogram', 'timing')",
            name='check_performance_metric_type'
        ),
        schema='audit'
    )
    
    # Data quality checks table
    op.create_table(
        'data_quality_checks',
        sa.Column('id', UUID(as_uuid=True), primary_key=True, 
                  server_default=sa.text('uuid_generate_v4()'), nullable=False),
        sa.Column('table_name', sa.String(100), nullable=False),
        sa.Column('column_name', sa.String(100), nullable=True),
        sa.Column('check_type', sa.String(50), nullable=False),
        sa.Column('check_name', sa.String(200), nullable=False),
        sa.Column('check_query', sa.Text, nullable=False),
        sa.Column('threshold_value', sa.DECIMAL(10, 4), nullable=True),
        sa.Column('threshold_operator', sa.String(10), nullable=True),
        sa.Column('is_active', sa.Boolean, nullable=False, default=True),
        sa.Column('severity', sa.String(10), nullable=False, default='WARN'),
        sa.Column('last_run_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('last_result', sa.DECIMAL(10, 4), nullable=True),
        sa.Column('last_status', sa.String(20), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, 
                  server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False, 
                  server_default=sa.func.now()),
        sa.CheckConstraint(
            "check_type IN ('completeness', 'accuracy', 'consistency', 'timeliness')",
            name='check_data_quality_check_type'
        ),
        sa.CheckConstraint(
            "threshold_operator IN ('>', '<', '=', '>=', '<=')",
            name='check_threshold_operator'
        ),
        sa.CheckConstraint(
            "severity IN ('DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL')",
            name='check_data_quality_severity'
        ),
        sa.CheckConstraint(
            "last_status IN ('passed', 'failed', 'error')",
            name='check_last_status'
        ),
        schema='audit'
    )


def downgrade() -> None:
    """Revert initial schema migration."""
    
    # Drop tables in reverse order to handle foreign keys
    op.drop_table('data_quality_checks', schema='audit')
    op.drop_table('performance_metrics', schema='audit')
    op.drop_table('error_logs', schema='audit')
    
    op.drop_table('processing_state', schema='trading')
    op.drop_table('jobs', schema='trading')
    op.drop_table('job_types', schema='trading')
    
    op.drop_table('documents', schema='edgar')
    op.drop_table('filings', schema='edgar')
    op.drop_table('filing_types', schema='edgar')
    op.drop_table('companies', schema='edgar')
    
    # Drop schemas
    op.execute('DROP SCHEMA IF EXISTS audit CASCADE')
    op.execute('DROP SCHEMA IF EXISTS trading CASCADE')
    op.execute('DROP SCHEMA IF EXISTS edgar CASCADE')
    
    # Drop extensions (optional - might be used by other databases)
    # op.execute('DROP EXTENSION IF EXISTS "pg_trgm"')
    # op.execute('DROP EXTENSION IF EXISTS "pgcrypto"')
    # op.execute('DROP EXTENSION IF EXISTS "uuid-ossp"')