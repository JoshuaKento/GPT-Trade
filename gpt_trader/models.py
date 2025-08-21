"""
Database models for GPT Trader platform.

This module defines SQLAlchemy models for storing corporate filing metadata,
company information, and processing job states.
"""

from datetime import datetime
from typing import List, Optional
from enum import Enum

from sqlalchemy import (
    Column, Integer, String, DateTime, Text, Boolean, Float, 
    ForeignKey, Index, JSON, BigInteger
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, Session
from sqlalchemy.dialects.postgresql import UUID
import uuid

Base = declarative_base()


class ProcessingStatus(str, Enum):
    """Processing status enumeration."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"


class FilingType(str, Enum):
    """SEC filing form types."""
    FORM_10K = "10-K"
    FORM_10Q = "10-Q" 
    FORM_8K = "8-K"
    FORM_DEF14A = "DEF 14A"
    FORM_S1 = "S-1"
    OTHER = "OTHER"


class Company(Base):
    """Company information and metadata."""
    
    __tablename__ = "companies"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    cik = Column(String(10), unique=True, nullable=False, index=True)
    ticker = Column(String(10), nullable=True, index=True)
    name = Column(String(255), nullable=False)
    sector = Column(String(100), nullable=True)
    industry = Column(String(100), nullable=True)
    
    # Metadata
    is_active = Column(Boolean, default=True, nullable=False)
    last_processed = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    # Additional SEC metadata
    sic_code = Column(String(4), nullable=True)
    state_of_incorporation = Column(String(2), nullable=True)
    fiscal_year_end = Column(String(4), nullable=True)
    
    # Relationships
    filings = relationship("Filing", back_populates="company", cascade="all, delete-orphan")
    processing_jobs = relationship("ProcessingJob", back_populates="company")
    
    # Indexes
    __table_args__ = (
        Index('ix_companies_ticker_active', 'ticker', 'is_active'),
        Index('ix_companies_last_processed', 'last_processed'),
    )
    
    def __repr__(self):
        return f"<Company(cik='{self.cik}', ticker='{self.ticker}', name='{self.name}')>"
    
    @classmethod
    def get_by_cik(cls, session: Session, cik: str) -> Optional['Company']:
        """Get company by CIK."""
        return session.query(cls).filter(cls.cik == cik.zfill(10)).first()
    
    @classmethod
    def get_active_companies(cls, session: Session) -> List['Company']:
        """Get all active companies."""
        return session.query(cls).filter(cls.is_active == True).all()


class Filing(Base):
    """SEC filing metadata and information."""
    
    __tablename__ = "filings"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey('companies.id'), nullable=False)
    
    # SEC filing identifiers
    accession_number = Column(String(20), unique=True, nullable=False, index=True)
    form_type = Column(String(20), nullable=False, index=True)
    filing_date = Column(DateTime, nullable=False, index=True)
    period_of_report = Column(DateTime, nullable=True)
    
    # URLs and file information
    primary_document = Column(String(255), nullable=True)
    primary_doc_url = Column(Text, nullable=True)
    filing_url = Column(Text, nullable=True)
    interactive_data_url = Column(Text, nullable=True)
    
    # Processing metadata
    is_processed = Column(Boolean, default=False, nullable=False)
    processing_status = Column(String(20), default=ProcessingStatus.PENDING, nullable=False)
    error_message = Column(Text, nullable=True)
    processing_attempts = Column(Integer, default=0, nullable=False)
    last_processing_attempt = Column(DateTime, nullable=True)
    
    # File metadata
    file_size = Column(BigInteger, nullable=True)
    file_count = Column(Integer, nullable=True)
    
    # Audit fields
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    processed_at = Column(DateTime, nullable=True)
    
    # Relationships
    company = relationship("Company", back_populates="filings")
    documents = relationship("Document", back_populates="filing", cascade="all, delete-orphan")
    
    # Indexes
    __table_args__ = (
        Index('ix_filings_company_date', 'company_id', 'filing_date'),
        Index('ix_filings_form_date', 'form_type', 'filing_date'),
        Index('ix_filings_status', 'processing_status'),
        Index('ix_filings_pending', 'is_processed', 'processing_status'),
    )
    
    def __repr__(self):
        return f"<Filing(accession='{self.accession_number}', form='{self.form_type}', date='{self.filing_date}')>"
    
    @classmethod
    def get_by_accession(cls, session: Session, accession_number: str) -> Optional['Filing']:
        """Get filing by accession number."""
        return session.query(cls).filter(cls.accession_number == accession_number).first()
    
    @classmethod
    def get_pending_filings(cls, session: Session, limit: Optional[int] = None) -> List['Filing']:
        """Get filings pending processing."""
        query = session.query(cls).filter(
            cls.is_processed == False,
            cls.processing_status.in_([ProcessingStatus.PENDING, ProcessingStatus.FAILED])
        ).order_by(cls.filing_date.desc())
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    def mark_processing_started(self, session: Session) -> None:
        """Mark filing as processing started."""
        self.processing_status = ProcessingStatus.IN_PROGRESS
        self.last_processing_attempt = datetime.utcnow()
        self.processing_attempts += 1
        session.commit()
    
    def mark_processing_completed(self, session: Session) -> None:
        """Mark filing as successfully processed."""
        self.is_processed = True
        self.processing_status = ProcessingStatus.COMPLETED
        self.processed_at = datetime.utcnow()
        self.error_message = None
        session.commit()
    
    def mark_processing_failed(self, session: Session, error_message: str) -> None:
        """Mark filing as processing failed."""
        self.processing_status = ProcessingStatus.FAILED
        self.error_message = error_message
        session.commit()


class Document(Base):
    """Individual documents within SEC filings."""
    
    __tablename__ = "documents"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    filing_id = Column(Integer, ForeignKey('filings.id'), nullable=False)
    
    # Document metadata
    sequence = Column(String(10), nullable=True)
    description = Column(Text, nullable=True)
    document_name = Column(String(255), nullable=False)
    document_type = Column(String(50), nullable=True)
    file_size = Column(BigInteger, nullable=True)
    
    # Storage information
    s3_bucket = Column(String(63), nullable=True)
    s3_key = Column(String(1024), nullable=True)
    local_path = Column(Text, nullable=True)
    content_hash = Column(String(64), nullable=True)  # SHA-256 hash
    
    # Processing metadata
    is_downloaded = Column(Boolean, default=False, nullable=False)
    is_processed = Column(Boolean, default=False, nullable=False)
    download_attempts = Column(Integer, default=0, nullable=False)
    last_download_attempt = Column(DateTime, nullable=True)
    
    # Audit fields
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    downloaded_at = Column(DateTime, nullable=True)
    
    # Relationships
    filing = relationship("Filing", back_populates="documents")
    
    # Indexes
    __table_args__ = (
        Index('ix_documents_filing_name', 'filing_id', 'document_name'),
        Index('ix_documents_download_status', 'is_downloaded', 'download_attempts'),
        Index('ix_documents_s3_location', 's3_bucket', 's3_key'),
    )
    
    def __repr__(self):
        return f"<Document(name='{self.document_name}', type='{self.document_type}')>"
    
    def mark_downloaded(self, session: Session, s3_bucket: str = None, s3_key: str = None, 
                       content_hash: str = None) -> None:
        """Mark document as successfully downloaded."""
        self.is_downloaded = True
        self.downloaded_at = datetime.utcnow()
        if s3_bucket:
            self.s3_bucket = s3_bucket
        if s3_key:
            self.s3_key = s3_key
        if content_hash:
            self.content_hash = content_hash
        session.commit()


class ProcessingJob(Base):
    """ETL processing job tracking and metadata."""
    
    __tablename__ = "processing_jobs"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    job_uuid = Column(String(36), unique=True, nullable=False, default=lambda: str(uuid.uuid4()))
    
    # Job metadata
    job_type = Column(String(50), nullable=False)  # e.g., "daily_etl", "backfill", "single_company"
    job_name = Column(String(255), nullable=False)
    company_id = Column(Integer, ForeignKey('companies.id'), nullable=True)  # nullable for multi-company jobs
    
    # Job parameters
    parameters = Column(JSON, nullable=True)  # Job-specific parameters
    ticker_list = Column(JSON, nullable=True)  # List of tickers for batch jobs
    
    # Job status and timing
    status = Column(String(20), default=ProcessingStatus.PENDING, nullable=False)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    estimated_duration = Column(Integer, nullable=True)  # seconds
    
    # Progress tracking
    total_items = Column(Integer, nullable=True)
    processed_items = Column(Integer, default=0, nullable=False)
    failed_items = Column(Integer, default=0, nullable=False)
    success_rate = Column(Float, nullable=True)
    
    # Error handling
    error_message = Column(Text, nullable=True)
    retry_count = Column(Integer, default=0, nullable=False)
    max_retries = Column(Integer, default=3, nullable=False)
    
    # Performance metrics
    items_per_second = Column(Float, nullable=True)
    peak_memory_mb = Column(Float, nullable=True)
    
    # Audit fields
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    # Relationships
    company = relationship("Company", back_populates="processing_jobs")
    
    # Indexes
    __table_args__ = (
        Index('ix_jobs_status_created', 'status', 'created_at'),
        Index('ix_jobs_type_status', 'job_type', 'status'),
        Index('ix_jobs_company_status', 'company_id', 'status'),
    )
    
    def __repr__(self):
        return f"<ProcessingJob(uuid='{self.job_uuid}', type='{self.job_type}', status='{self.status}')>"
    
    @classmethod
    def create_job(cls, session: Session, job_type: str, job_name: str, 
                   company_id: int = None, parameters: dict = None, 
                   ticker_list: List[str] = None) -> 'ProcessingJob':
        """Create a new processing job."""
        job = cls(
            job_type=job_type,
            job_name=job_name,
            company_id=company_id,
            parameters=parameters,
            ticker_list=ticker_list
        )
        session.add(job)
        session.commit()
        return job
    
    def start_job(self, session: Session, total_items: int = None) -> None:
        """Mark job as started."""
        self.status = ProcessingStatus.IN_PROGRESS
        self.started_at = datetime.utcnow()
        if total_items:
            self.total_items = total_items
        session.commit()
    
    def complete_job(self, session: Session) -> None:
        """Mark job as completed."""
        self.status = ProcessingStatus.COMPLETED
        self.completed_at = datetime.utcnow()
        if self.total_items and self.total_items > 0:
            self.success_rate = (self.processed_items - self.failed_items) / self.total_items
        session.commit()
    
    def fail_job(self, session: Session, error_message: str) -> None:
        """Mark job as failed."""
        self.status = ProcessingStatus.FAILED
        self.error_message = error_message
        self.completed_at = datetime.utcnow()
        session.commit()
    
    def update_progress(self, session: Session, processed_items: int = None, 
                       failed_items: int = None) -> None:
        """Update job progress."""
        if processed_items is not None:
            self.processed_items = processed_items
        if failed_items is not None:
            self.failed_items = failed_items
        
        # Calculate performance metrics
        if self.started_at:
            duration = (datetime.utcnow() - self.started_at).total_seconds()
            if duration > 0 and self.processed_items > 0:
                self.items_per_second = self.processed_items / duration
        
        session.commit()
    
    @property
    def duration(self) -> Optional[float]:
        """Get job duration in seconds."""
        if not self.started_at:
            return None
        end_time = self.completed_at or datetime.utcnow()
        return (end_time - self.started_at).total_seconds()
    
    @property
    def progress_percentage(self) -> Optional[float]:
        """Get job progress as percentage."""
        if not self.total_items or self.total_items == 0:
            return None
        return (self.processed_items / self.total_items) * 100