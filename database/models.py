"""
SQLAlchemy models for GPT Trader database schema.

This module defines the ORM models for the PostgreSQL database schema,
optimized for financial data processing with proper relationships, 
constraints, and performance considerations.
"""

import uuid
from datetime import datetime, date
from decimal import Decimal
from typing import Optional, List, Dict, Any

from sqlalchemy import (
    Boolean, Column, Date, DateTime, ForeignKey, Integer, String, Text, 
    DECIMAL, BigInteger, JSON, ARRAY, Index, CheckConstraint,
    UniqueConstraint, func, text
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, validates
from sqlalchemy.sql import expression

# Base class for all models
Base = declarative_base()


class TimestampMixin:
    """Mixin for timestamp fields."""
    created_at = Column(DateTime(timezone=True), nullable=False, 
                       server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, 
                       server_default=func.now(), onupdate=func.now())


class AuditMixin(TimestampMixin):
    """Mixin for audit fields including user tracking."""
    created_by = Column(String(100), default='system')
    updated_by = Column(String(100), default='system')


# =============================================================================
# EDGAR SCHEMA MODELS
# =============================================================================

class Company(Base, AuditMixin):
    """Master company registry with SEC and market data."""
    
    __tablename__ = 'companies'
    __table_args__ = (
        {'schema': 'edgar'},
        Index('idx_companies_cik', 'cik', unique=True),
        Index('idx_companies_ticker', 'ticker', 
              postgresql_where=text('ticker IS NOT NULL')),
        Index('idx_companies_active', 'is_active', 'processing_enabled'),
        Index('idx_companies_sector', 'sector', 
              postgresql_where=text('sector IS NOT NULL')),
        Index('idx_companies_last_check', 'last_filing_check'),
    )
    
    id = Column(UUID(as_uuid=True), primary_key=True, 
                default=uuid.uuid4, nullable=False)
    cik = Column(String(10), nullable=False, unique=True)
    ticker = Column(String(10), nullable=True)
    company_name = Column(Text, nullable=False)
    sic = Column(String(4), nullable=True)
    sector = Column(String(100), nullable=True)
    industry = Column(String(200), nullable=True)
    exchange = Column(String(10), nullable=True)
    state_of_incorporation = Column(String(2), nullable=True)
    fiscal_year_end = Column(String(4), nullable=True)
    
    # Financial metadata
    market_cap = Column(BigInteger, nullable=True)
    employees = Column(Integer, nullable=True)
    
    # Status and processing flags
    is_active = Column(Boolean, nullable=False, default=True)
    processing_enabled = Column(Boolean, nullable=False, default=True)
    last_filing_check = Column(DateTime(timezone=True), nullable=True)
    
    # Relationships
    filings = relationship("Filing", back_populates="company", 
                          cascade="all, delete-orphan",
                          order_by="Filing.filing_date.desc()")
    jobs = relationship("Job", back_populates="company")
    error_logs = relationship("ErrorLog", back_populates="company")
    
    @validates('cik')
    def validate_cik(self, key, cik):
        """Validate CIK format."""
        if cik and len(cik) != 10:
            raise ValueError(f"CIK must be exactly 10 characters, got {len(cik)}")
        return cik
    
    @validates('ticker')
    def validate_ticker(self, key, ticker):
        """Validate ticker format."""
        if ticker and len(ticker) > 10:
            raise ValueError(f"Ticker too long: {ticker}")
        return ticker.upper() if ticker else None
    
    def __repr__(self):
        return f"<Company(cik='{self.cik}', ticker='{self.ticker}', name='{self.company_name}')>"


class FilingType(Base, TimestampMixin):
    """Lookup table for filing types with processing priorities."""
    
    __tablename__ = 'filing_types'
    __table_args__ = {'schema': 'edgar'}
    
    id = Column(Integer, primary_key=True)
    form_type = Column(String(20), nullable=False, unique=True)
    description = Column(Text, nullable=True)
    priority = Column(Integer, nullable=False, default=10)
    retention_days = Column(Integer, default=2555)  # ~7 years
    is_active = Column(Boolean, nullable=False, default=True)
    
    # Relationships
    filings = relationship("Filing", back_populates="filing_type")
    
    def __repr__(self):
        return f"<FilingType(form_type='{self.form_type}', priority={self.priority})>"


class Filing(Base, AuditMixin):
    """SEC filing metadata and processing state."""
    
    __tablename__ = 'filings'
    __table_args__ = (
        {'schema': 'edgar'},
        Index('idx_filings_accession', 'accession_number', unique=True),
        Index('idx_filings_company_form_date', 'company_id', 'form_type', 
              'filing_date', postgresql_using='btree'),
        Index('idx_filings_status_priority', 'processing_status', 'processing_priority'),
        Index('idx_filings_date_range', 'filing_date', 
              postgresql_where=text("processing_status != 'completed'")),
        Index('idx_filings_error_retry', 'retry_after', 
              postgresql_where=text('retry_after IS NOT NULL')),
        Index('idx_filings_processing_active', 'processing_started_at',
              postgresql_where=text('processing_completed_at IS NULL')),
        Index('idx_filings_unprocessed', 'company_id', 'filing_date',
              postgresql_where=text("processing_status IN ('discovered', 'pending', 'processing', 'failed')")),
        CheckConstraint("processing_status IN ('discovered', 'pending', 'processing', 'completed', 'failed', 'archived')",
                       name='check_filing_processing_status'),
        CheckConstraint('processing_priority BETWEEN 1 AND 10',
                       name='check_filing_priority_range'),
    )
    
    id = Column(UUID(as_uuid=True), primary_key=True, 
                default=uuid.uuid4, nullable=False)
    company_id = Column(UUID(as_uuid=True), ForeignKey('edgar.companies.id', ondelete='CASCADE'),
                       nullable=False)
    
    # EDGAR metadata
    accession_number = Column(String(20), nullable=False, unique=True)
    form_type = Column(String(20), nullable=False)
    filing_date = Column(Date, nullable=False)
    report_date = Column(Date, nullable=True)
    acceptance_datetime = Column(DateTime(timezone=True), nullable=True)
    
    # Document information
    primary_document = Column(String(255), nullable=True)
    document_count = Column(Integer, default=0)
    size_bytes = Column(BigInteger, nullable=True)
    
    # URLs and locations
    edgar_url = Column(Text, nullable=True)
    s3_bucket = Column(String(100), nullable=True)
    s3_key_prefix = Column(String(500), nullable=True)
    
    # Processing state
    processing_status = Column(String(20), nullable=False, default='discovered')
    processing_priority = Column(Integer, nullable=False, default=10)
    processing_started_at = Column(DateTime(timezone=True), nullable=True)
    processing_completed_at = Column(DateTime(timezone=True), nullable=True)
    processing_duration_ms = Column(Integer, nullable=True)
    
    # Error handling
    error_count = Column(Integer, nullable=False, default=0)
    last_error_message = Column(Text, nullable=True)
    last_error_at = Column(DateTime(timezone=True), nullable=True)
    retry_after = Column(DateTime(timezone=True), nullable=True)
    
    # Content metadata
    has_parsed_content = Column(Boolean, nullable=False, default=False)
    has_embeddings = Column(Boolean, nullable=False, default=False)
    text_extraction_method = Column(String(50), nullable=True)
    
    # Additional audit field
    processed_at = Column(DateTime(timezone=True), nullable=True)
    
    # Relationships
    company = relationship("Company", back_populates="filings")
    filing_type = relationship("FilingType", back_populates="filings")
    documents = relationship("Document", back_populates="filing", 
                           cascade="all, delete-orphan")
    jobs = relationship("Job", back_populates="filing")
    error_logs = relationship("ErrorLog", back_populates="filing")
    
    @validates('accession_number')
    def validate_accession_number(self, key, accession_number):
        """Validate accession number format."""
        if accession_number and len(accession_number) != 20:
            raise ValueError(f"Accession number must be 20 characters, got {len(accession_number)}")
        return accession_number
    
    @validates('processing_priority')
    def validate_priority(self, key, priority):
        """Validate priority range."""
        if priority < 1 or priority > 10:
            raise ValueError(f"Priority must be between 1 and 10, got {priority}")
        return priority
    
    @property
    def is_processed(self) -> bool:
        """Check if filing is fully processed."""
        return self.processing_status == 'completed'
    
    @property
    def processing_duration_seconds(self) -> Optional[float]:
        """Get processing duration in seconds."""
        if self.processing_duration_ms:
            return self.processing_duration_ms / 1000.0
        return None
    
    def __repr__(self):
        return f"<Filing(accession='{self.accession_number}', form='{self.form_type}', status='{self.processing_status}')>"


class Document(Base, TimestampMixin):
    """Individual documents within filings."""
    
    __tablename__ = 'documents'
    __table_args__ = (
        {'schema': 'edgar'},
        Index('idx_documents_filing', 'filing_id'),
        Index('idx_documents_type_status', 'document_type', 'processing_status'),
        Index('idx_documents_size', 'size_bytes', postgresql_using='btree'),
        CheckConstraint("processing_status IN ('pending', 'processing', 'completed', 'failed')",
                       name='check_document_processing_status'),
    )
    
    id = Column(UUID(as_uuid=True), primary_key=True, 
                default=uuid.uuid4, nullable=False)
    filing_id = Column(UUID(as_uuid=True), ForeignKey('edgar.filings.id', ondelete='CASCADE'),
                      nullable=False)
    
    # Document metadata
    document_name = Column(String(255), nullable=False)
    document_type = Column(String(50), nullable=True)
    description = Column(Text, nullable=True)
    sequence = Column(Integer, nullable=True)
    size_bytes = Column(BigInteger, nullable=True)
    
    # Storage locations
    s3_bucket = Column(String(100), nullable=True)
    s3_key = Column(String(500), nullable=True)
    local_path = Column(Text, nullable=True)
    
    # Content processing
    content_type = Column(String(100), nullable=True)
    extracted_text_length = Column(Integer, nullable=True)
    has_tables = Column(Boolean, default=False)
    has_images = Column(Boolean, default=False)
    
    # Processing status
    processing_status = Column(String(20), nullable=False, default='pending')
    extraction_method = Column(String(50), nullable=True)
    processed_at = Column(DateTime(timezone=True), nullable=True)
    
    # Relationships
    filing = relationship("Filing", back_populates="documents")
    
    def __repr__(self):
        return f"<Document(name='{self.document_name}', type='{self.document_type}')>"


# =============================================================================
# TRADING SCHEMA MODELS
# =============================================================================

class JobType(Base, TimestampMixin):
    """Job types for different processing tasks."""
    
    __tablename__ = 'job_types'
    __table_args__ = {'schema': 'trading'}
    
    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False, unique=True)
    description = Column(Text, nullable=True)
    default_timeout_minutes = Column(Integer, nullable=False, default=30)
    max_retries = Column(Integer, nullable=False, default=3)
    is_active = Column(Boolean, nullable=False, default=True)
    
    # Relationships
    jobs = relationship("Job", back_populates="job_type")
    
    def __repr__(self):
        return f"<JobType(name='{self.name}', timeout={self.default_timeout_minutes}min)>"


class Job(Base, TimestampMixin):
    """Job execution tracking for processing pipeline."""
    
    __tablename__ = 'jobs'
    __table_args__ = (
        {'schema': 'trading'},
        Index('idx_jobs_status_priority', 'status', 'priority', 'scheduled_at'),
        Index('idx_jobs_company_type', 'company_id', 'job_type_id', 'status'),
        Index('idx_jobs_retry', 'next_retry_at', 
              postgresql_where=text('next_retry_at IS NOT NULL')),
        Index('idx_jobs_active', 'started_at', 
              postgresql_where=text('completed_at IS NULL')),
        Index('idx_jobs_performance', 'job_type_id', 'completed_at',
              postgresql_where=text('completed_at IS NOT NULL')),
        CheckConstraint("status IN ('pending', 'running', 'completed', 'failed', 'cancelled')",
                       name='check_job_status'),
        CheckConstraint('priority BETWEEN 1 AND 10',
                       name='check_job_priority_range'),
    )
    
    id = Column(UUID(as_uuid=True), primary_key=True, 
                default=uuid.uuid4, nullable=False)
    job_type_id = Column(Integer, ForeignKey('trading.job_types.id'), nullable=False)
    
    # Job parameters
    company_id = Column(UUID(as_uuid=True), ForeignKey('edgar.companies.id'), nullable=True)
    filing_id = Column(UUID(as_uuid=True), ForeignKey('edgar.filings.id'), nullable=True)
    parameters = Column(JSONB, nullable=True)
    
    # Execution state
    status = Column(String(20), nullable=False, default='pending')
    priority = Column(Integer, nullable=False, default=10)
    scheduled_at = Column(DateTime(timezone=True), nullable=False, 
                         server_default=func.now())
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    
    # Progress tracking
    progress_current = Column(Integer, default=0)
    progress_total = Column(Integer, default=1)
    progress_message = Column(Text, nullable=True)
    
    # Resource usage
    worker_id = Column(String(100), nullable=True)
    execution_node = Column(String(100), nullable=True)
    memory_usage_mb = Column(Integer, nullable=True)
    cpu_time_ms = Column(Integer, nullable=True)
    
    # Error handling
    error_message = Column(Text, nullable=True)
    retry_count = Column(Integer, nullable=False, default=0)
    max_retries = Column(Integer, nullable=False, default=3)
    next_retry_at = Column(DateTime(timezone=True), nullable=True)
    
    # Results
    result_data = Column(JSONB, nullable=True)
    output_files = Column(ARRAY(Text), nullable=True)
    
    # Audit field
    created_by = Column(String(100), default='system')
    
    # Relationships
    job_type = relationship("JobType", back_populates="jobs")
    company = relationship("Company", back_populates="jobs")
    filing = relationship("Filing", back_populates="jobs")
    
    @property
    def progress_percentage(self) -> float:
        """Calculate progress percentage."""
        if self.progress_total and self.progress_total > 0:
            return round((self.progress_current / self.progress_total) * 100, 2)
        return 0.0
    
    @property
    def duration_seconds(self) -> Optional[float]:
        """Get job duration in seconds."""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None
    
    def __repr__(self):
        return f"<Job(id='{self.id}', type='{self.job_type.name if self.job_type else 'Unknown'}', status='{self.status}')>"


class ProcessingState(Base, TimestampMixin):
    """Global processing state and configuration."""
    
    __tablename__ = 'processing_state'
    __table_args__ = (
        {'schema': 'trading'},
        CheckConstraint("value_type IN ('string', 'integer', 'boolean', 'json', 'timestamp')",
                       name='check_processing_state_value_type'),
    )
    
    id = Column(Integer, primary_key=True)
    key = Column(String(100), nullable=False, unique=True)
    value = Column(Text, nullable=True)
    value_type = Column(String(20), nullable=False, default='string')
    description = Column(Text, nullable=True)
    is_system = Column(Boolean, nullable=False, default=False)
    updated_by = Column(String(100), default='system')
    
    @property
    def typed_value(self) -> Any:
        """Get value converted to appropriate type."""
        if self.value is None:
            return None
        
        if self.value_type == 'integer':
            return int(self.value)
        elif self.value_type == 'boolean':
            return self.value.lower() in ('true', '1', 'yes', 'on')
        elif self.value_type == 'json':
            import json
            return json.loads(self.value)
        elif self.value_type == 'timestamp':
            from dateutil.parser import parse
            return parse(self.value)
        else:
            return self.value
    
    def __repr__(self):
        return f"<ProcessingState(key='{self.key}', type='{self.value_type}')>"


# =============================================================================
# AUDIT SCHEMA MODELS
# =============================================================================

class ErrorLog(Base, TimestampMixin):
    """Comprehensive error logging and tracking."""
    
    __tablename__ = 'error_logs'
    __table_args__ = (
        {'schema': 'audit'},
        Index('idx_error_logs_severity_time', 'severity', 'created_at', postgresql_using='btree'),
        Index('idx_error_logs_company', 'company_id', 'created_at', postgresql_using='btree'),
        Index('idx_error_logs_unresolved', 'is_resolved', 'severity',
              postgresql_where=text('NOT is_resolved')),
        Index('idx_error_logs_category', 'category', 'created_at', postgresql_using='btree'),
        CheckConstraint("severity IN ('DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL')",
                       name='check_error_log_severity'),
    )
    
    id = Column(UUID(as_uuid=True), primary_key=True, 
                default=uuid.uuid4, nullable=False)
    
    # Context
    job_id = Column(UUID(as_uuid=True), ForeignKey('trading.jobs.id'), nullable=True)
    filing_id = Column(UUID(as_uuid=True), ForeignKey('edgar.filings.id'), nullable=True)
    company_id = Column(UUID(as_uuid=True), ForeignKey('edgar.companies.id'), nullable=True)
    
    # Error details
    error_type = Column(String(50), nullable=False)
    error_code = Column(String(20), nullable=True)
    error_message = Column(Text, nullable=False)
    stack_trace = Column(Text, nullable=True)
    
    # Severity and categorization
    severity = Column(String(10), nullable=False, default='ERROR')
    category = Column(String(50), nullable=True)
    is_retryable = Column(Boolean, nullable=False, default=True)
    
    # Context data
    request_data = Column(JSONB, nullable=True)
    response_data = Column(JSONB, nullable=True)
    environment_data = Column(JSONB, nullable=True)
    
    # Resolution tracking
    is_resolved = Column(Boolean, nullable=False, default=False)
    resolved_at = Column(DateTime(timezone=True), nullable=True)
    resolution_notes = Column(Text, nullable=True)
    
    # Source tracking
    source_module = Column(String(100), nullable=True)
    source_function = Column(String(100), nullable=True)
    
    # Relationships
    job = relationship("Job")
    filing = relationship("Filing", back_populates="error_logs")
    company = relationship("Company", back_populates="error_logs")
    
    def __repr__(self):
        return f"<ErrorLog(type='{self.error_type}', severity='{self.severity}', resolved={self.is_resolved})>"


class PerformanceMetric(Base):
    """Performance metrics tracking for monitoring and optimization."""
    
    __tablename__ = 'performance_metrics'
    __table_args__ = (
        {'schema': 'audit'},
        Index('idx_performance_metrics_time_name', 'timestamp', 'metric_name', 
              postgresql_using='btree'),
        Index('idx_performance_metrics_company_time', 'company_id', 'timestamp',
              postgresql_using='btree'),
        CheckConstraint("metric_type IN ('counter', 'gauge', 'histogram', 'timing')",
                       name='check_performance_metric_type'),
    )
    
    id = Column(UUID(as_uuid=True), primary_key=True, 
                default=uuid.uuid4, nullable=False)
    
    # Metric identification
    metric_name = Column(String(100), nullable=False)
    metric_type = Column(String(20), nullable=False)
    
    # Values
    value_numeric = Column(DECIMAL(15, 6), nullable=True)
    value_text = Column(Text, nullable=True)
    
    # Dimensions
    company_id = Column(UUID(as_uuid=True), ForeignKey('edgar.companies.id'), nullable=True)
    job_type = Column(String(50), nullable=True)
    source_module = Column(String(100), nullable=True)
    
    # Tags for grouping
    tags = Column(JSONB, nullable=True)
    
    # Timing
    timestamp = Column(DateTime(timezone=True), nullable=False, 
                      server_default=func.now())
    
    # Metadata
    metadata = Column(JSONB, nullable=True)
    
    # Relationships
    company = relationship("Company")
    
    def __repr__(self):
        return f"<PerformanceMetric(name='{self.metric_name}', type='{self.metric_type}', value={self.value_numeric})>"


class DataQualityCheck(Base, TimestampMixin):
    """Data quality checks and validation rules."""
    
    __tablename__ = 'data_quality_checks'
    __table_args__ = (
        {'schema': 'audit'},
        CheckConstraint("check_type IN ('completeness', 'accuracy', 'consistency', 'timeliness')",
                       name='check_data_quality_check_type'),
        CheckConstraint("threshold_operator IN ('>', '<', '=', '>=', '<=')",
                       name='check_threshold_operator'),
        CheckConstraint("severity IN ('DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL')",
                       name='check_data_quality_severity'),
        CheckConstraint("last_status IN ('passed', 'failed', 'error')",
                       name='check_last_status'),
    )
    
    id = Column(UUID(as_uuid=True), primary_key=True, 
                default=uuid.uuid4, nullable=False)
    
    # Target
    table_name = Column(String(100), nullable=False)
    column_name = Column(String(100), nullable=True)
    check_type = Column(String(50), nullable=False)
    
    # Check definition
    check_name = Column(String(200), nullable=False)
    check_query = Column(Text, nullable=False)
    threshold_value = Column(DECIMAL(10, 4), nullable=True)
    threshold_operator = Column(String(10), nullable=True)
    
    # Status
    is_active = Column(Boolean, nullable=False, default=True)
    severity = Column(String(10), nullable=False, default='WARN')
    
    # Execution
    last_run_at = Column(DateTime(timezone=True), nullable=True)
    last_result = Column(DECIMAL(10, 4), nullable=True)
    last_status = Column(String(20), nullable=True)
    
    def __repr__(self):
        return f"<DataQualityCheck(name='{self.check_name}', table='{self.table_name}', status='{self.last_status}')>"


# =============================================================================
# MODEL REGISTRY
# =============================================================================

# Export all models for easy importing
__all__ = [
    'Base',
    'TimestampMixin', 
    'AuditMixin',
    'Company',
    'FilingType', 
    'Filing',
    'Document',
    'JobType',
    'Job',
    'ProcessingState',
    'ErrorLog',
    'PerformanceMetric',
    'DataQualityCheck',
]