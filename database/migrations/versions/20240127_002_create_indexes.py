"""Create optimized indexes for performance

Revision ID: 20240127_002
Revises: 20240127_001
Create Date: 2024-01-27 12:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '20240127_002'
down_revision: Union[str, None] = '20240127_001'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create optimized indexes for query performance."""
    
    # =============================================================================
    # EDGAR SCHEMA INDEXES
    # =============================================================================
    
    # Companies table indexes
    op.create_index(
        'idx_companies_cik', 'edgar.companies', ['cik'],
        unique=True, schema='edgar'
    )
    op.create_index(
        'idx_companies_ticker', 'edgar.companies', ['ticker'],
        postgresql_where=sa.text('ticker IS NOT NULL'),
        schema='edgar'
    )
    op.create_index(
        'idx_companies_active', 'edgar.companies', 
        ['is_active', 'processing_enabled'],
        schema='edgar'
    )
    op.create_index(
        'idx_companies_sector', 'edgar.companies', ['sector'],
        postgresql_where=sa.text('sector IS NOT NULL'),
        schema='edgar'
    )
    op.create_index(
        'idx_companies_last_check', 'edgar.companies', ['last_filing_check'],
        schema='edgar'
    )
    
    # Filings table indexes
    op.create_index(
        'idx_filings_accession', 'edgar.filings', ['accession_number'],
        unique=True, schema='edgar'
    )
    op.create_index(
        'idx_filings_company_form_date', 'edgar.filings', 
        ['company_id', 'form_type', 'filing_date'],
        postgresql_using='btree',
        schema='edgar'
    )
    op.create_index(
        'idx_filings_status_priority', 'edgar.filings', 
        ['processing_status', 'processing_priority'],
        schema='edgar'
    )
    op.create_index(
        'idx_filings_date_range', 'edgar.filings', ['filing_date'],
        postgresql_where=sa.text("processing_status != 'completed'"),
        schema='edgar'
    )
    op.create_index(
        'idx_filings_error_retry', 'edgar.filings', ['retry_after'],
        postgresql_where=sa.text('retry_after IS NOT NULL'),
        schema='edgar'
    )
    op.create_index(
        'idx_filings_processing_active', 'edgar.filings', ['processing_started_at'],
        postgresql_where=sa.text('processing_completed_at IS NULL'),
        schema='edgar'
    )
    op.create_index(
        'idx_filings_unprocessed', 'edgar.filings', ['company_id', 'filing_date'],
        postgresql_where=sa.text("processing_status IN ('discovered', 'pending', 'processing', 'failed')"),
        schema='edgar'
    )
    
    # Performance-critical composite index for daily processing
    op.create_index(
        'idx_filings_daily_processing', 'edgar.filings',
        ['filing_date', 'processing_status', 'processing_priority'],
        postgresql_where=sa.text("processing_status IN ('discovered', 'pending', 'processing')"),
        schema='edgar'
    )
    
    # SLA monitoring index
    op.create_index(
        'idx_filings_sla_monitoring', 'edgar.filings',
        ['filing_date', 'processing_completed_at', 'processing_duration_ms'],
        postgresql_where=sa.text("processing_status = 'completed' AND processing_completed_at IS NOT NULL"),
        schema='edgar'
    )
    
    # Documents table indexes
    op.create_index(
        'idx_documents_filing', 'edgar.documents', ['filing_id'],
        schema='edgar'
    )
    op.create_index(
        'idx_documents_type_status', 'edgar.documents', 
        ['document_type', 'processing_status'],
        schema='edgar'
    )
    op.create_index(
        'idx_documents_size', 'edgar.documents', ['size_bytes'],
        postgresql_using='btree',
        schema='edgar'
    )
    
    # =============================================================================
    # TRADING SCHEMA INDEXES
    # =============================================================================
    
    # Jobs table indexes
    op.create_index(
        'idx_jobs_status_priority', 'trading.jobs', 
        ['status', 'priority', 'scheduled_at'],
        schema='trading'
    )
    op.create_index(
        'idx_jobs_company_type', 'trading.jobs', 
        ['company_id', 'job_type_id', 'status'],
        schema='trading'
    )
    op.create_index(
        'idx_jobs_retry', 'trading.jobs', ['next_retry_at'],
        postgresql_where=sa.text('next_retry_at IS NOT NULL'),
        schema='trading'
    )
    op.create_index(
        'idx_jobs_active', 'trading.jobs', ['started_at'],
        postgresql_where=sa.text('completed_at IS NULL'),
        schema='trading'
    )
    op.create_index(
        'idx_jobs_performance', 'trading.jobs', ['job_type_id', 'completed_at'],
        postgresql_where=sa.text('completed_at IS NOT NULL'),
        schema='trading'
    )
    
    # Optimized job queue index
    op.create_index(
        'idx_jobs_queue_optimization', 'trading.jobs',
        ['status', 'priority', 'scheduled_at', 'next_retry_at'],
        postgresql_where=sa.text("status IN ('pending', 'failed') AND scheduled_at <= NOW()"),
        schema='trading'
    )
    
    # Company processing optimization
    op.create_index(
        'idx_companies_processing_optimization', 'edgar.companies',
        ['processing_enabled', 'is_active', 'last_filing_check'],
        postgresql_where=sa.text('processing_enabled = true AND is_active = true'),
        schema='edgar'
    )
    
    # =============================================================================
    # AUDIT SCHEMA INDEXES
    # =============================================================================
    
    # Error logs indexes
    op.create_index(
        'idx_error_logs_severity_time', 'audit.error_logs', 
        ['severity', 'created_at'],
        postgresql_using='btree',
        schema='audit'
    )
    op.create_index(
        'idx_error_logs_company', 'audit.error_logs', 
        ['company_id', 'created_at'],
        postgresql_using='btree',
        schema='audit'
    )
    op.create_index(
        'idx_error_logs_unresolved', 'audit.error_logs', 
        ['is_resolved', 'severity'],
        postgresql_where=sa.text('NOT is_resolved'),
        schema='audit'
    )
    op.create_index(
        'idx_error_logs_category', 'audit.error_logs', 
        ['category', 'created_at'],
        postgresql_using='btree',
        schema='audit'
    )
    
    # Active error monitoring index
    op.create_index(
        'idx_error_logs_active_monitoring', 'audit.error_logs',
        ['created_at', 'severity', 'category'],
        postgresql_where=sa.text("NOT is_resolved AND severity IN ('ERROR', 'FATAL')"),
        schema='audit'
    )
    
    # Performance metrics indexes
    op.create_index(
        'idx_performance_metrics_time_name', 'audit.performance_metrics', 
        ['timestamp', 'metric_name'],
        postgresql_using='btree',
        schema='audit'
    )
    op.create_index(
        'idx_performance_metrics_company_time', 'audit.performance_metrics', 
        ['company_id', 'timestamp'],
        postgresql_using='btree',
        schema='audit'
    )
    
    # Time-series index for recent metrics
    op.create_index(
        'idx_performance_metrics_time_series', 'audit.performance_metrics',
        ['timestamp', 'metric_name', 'company_id'],
        postgresql_where=sa.text("timestamp >= NOW() - INTERVAL '90 days'"),
        schema='audit'
    )


def downgrade() -> None:
    """Drop the indexes created in this migration."""
    
    # Drop indexes in reverse order
    
    # Audit schema indexes
    op.drop_index('idx_performance_metrics_time_series', schema='audit')
    op.drop_index('idx_performance_metrics_company_time', schema='audit')
    op.drop_index('idx_performance_metrics_time_name', schema='audit')
    op.drop_index('idx_error_logs_active_monitoring', schema='audit')
    op.drop_index('idx_error_logs_category', schema='audit')
    op.drop_index('idx_error_logs_unresolved', schema='audit')
    op.drop_index('idx_error_logs_company', schema='audit')
    op.drop_index('idx_error_logs_severity_time', schema='audit')
    
    # Trading schema indexes
    op.drop_index('idx_companies_processing_optimization', schema='edgar')
    op.drop_index('idx_jobs_queue_optimization', schema='trading')
    op.drop_index('idx_jobs_performance', schema='trading')
    op.drop_index('idx_jobs_active', schema='trading')
    op.drop_index('idx_jobs_retry', schema='trading')
    op.drop_index('idx_jobs_company_type', schema='trading')
    op.drop_index('idx_jobs_status_priority', schema='trading')
    
    # Edgar schema indexes
    op.drop_index('idx_documents_size', schema='edgar')
    op.drop_index('idx_documents_type_status', schema='edgar')
    op.drop_index('idx_documents_filing', schema='edgar')
    op.drop_index('idx_filings_sla_monitoring', schema='edgar')
    op.drop_index('idx_filings_daily_processing', schema='edgar')
    op.drop_index('idx_filings_unprocessed', schema='edgar')
    op.drop_index('idx_filings_processing_active', schema='edgar')
    op.drop_index('idx_filings_error_retry', schema='edgar')
    op.drop_index('idx_filings_date_range', schema='edgar')
    op.drop_index('idx_filings_status_priority', schema='edgar')
    op.drop_index('idx_filings_company_form_date', schema='edgar')
    op.drop_index('idx_filings_accession', schema='edgar')
    op.drop_index('idx_companies_last_check', schema='edgar')
    op.drop_index('idx_companies_sector', schema='edgar')
    op.drop_index('idx_companies_active', schema='edgar')
    op.drop_index('idx_companies_ticker', schema='edgar')
    op.drop_index('idx_companies_cik', schema='edgar')