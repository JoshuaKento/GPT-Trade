"""
Test cases for GPT Trader database models.
"""

import pytest
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from unittest.mock import patch

from gpt_trader.models import (
    Base, Company, Filing, Document, ProcessingJob,
    ProcessingStatus, FilingType
)
from gpt_trader.database import DatabaseManager, DatabaseConfig


@pytest.fixture
def test_engine():
    """Create in-memory SQLite engine for testing."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(engine)
    return engine


@pytest.fixture
def test_session(test_engine):
    """Create test database session."""
    SessionLocal = sessionmaker(bind=test_engine)
    session = SessionLocal()
    yield session
    session.close()


@pytest.fixture
def sample_company(test_session):
    """Create a sample company for testing."""
    company = Company(
        cik="0000320193",
        ticker="AAPL",
        name="Apple Inc.",
        sector="Technology",
        industry="Consumer Electronics"
    )
    test_session.add(company)
    test_session.commit()
    return company


@pytest.fixture
def sample_filing(test_session, sample_company):
    """Create a sample filing for testing."""
    filing = Filing(
        company_id=sample_company.id,
        accession_number="0000320193-23-000006",
        form_type="10-K",
        filing_date=datetime(2023, 10, 2),
        period_of_report=datetime(2023, 9, 30),
        primary_document="aapl-20230930.htm"
    )
    test_session.add(filing)
    test_session.commit()
    return filing


class TestCompanyModel:
    """Test cases for Company model."""
    
    def test_company_creation(self, test_session):
        """Test creating a company record."""
        company = Company(
            cik="0000789019",
            ticker="MSFT",
            name="Microsoft Corporation",
            sector="Technology"
        )
        test_session.add(company)
        test_session.commit()
        
        assert company.id is not None
        assert company.cik == "0000789019"
        assert company.ticker == "MSFT"
        assert company.is_active is True
        assert company.created_at is not None
    
    def test_get_by_cik(self, test_session, sample_company):
        """Test retrieving company by CIK."""
        found_company = Company.get_by_cik(test_session, "0000320193")
        assert found_company is not None
        assert found_company.id == sample_company.id
        assert found_company.ticker == "AAPL"
        
        # Test with zero-padded CIK
        found_company2 = Company.get_by_cik(test_session, "320193")
        assert found_company2 is not None
        assert found_company2.id == sample_company.id
    
    def test_get_active_companies(self, test_session):
        """Test retrieving active companies."""
        # Create active company
        active_company = Company(cik="1234567890", ticker="TEST1", name="Test Company 1", is_active=True)
        test_session.add(active_company)
        
        # Create inactive company
        inactive_company = Company(cik="0987654321", ticker="TEST2", name="Test Company 2", is_active=False)
        test_session.add(inactive_company)
        test_session.commit()
        
        active_companies = Company.get_active_companies(test_session)
        active_tickers = [c.ticker for c in active_companies]
        
        assert "TEST1" in active_tickers
        assert "TEST2" not in active_tickers
    
    def test_company_repr(self, sample_company):
        """Test company string representation."""
        repr_str = repr(sample_company)
        assert "0000320193" in repr_str
        assert "AAPL" in repr_str
        assert "Apple Inc." in repr_str


class TestFilingModel:
    """Test cases for Filing model."""
    
    def test_filing_creation(self, test_session, sample_company):
        """Test creating a filing record."""
        filing = Filing(
            company_id=sample_company.id,
            accession_number="0000320193-23-000007",
            form_type="10-Q",
            filing_date=datetime(2023, 8, 3),
            primary_document="aapl-20230630.htm"
        )
        test_session.add(filing)
        test_session.commit()
        
        assert filing.id is not None
        assert filing.accession_number == "0000320193-23-000007"
        assert filing.form_type == "10-Q"
        assert filing.is_processed is False
        assert filing.processing_status == ProcessingStatus.PENDING
        assert filing.processing_attempts == 0
    
    def test_get_by_accession(self, test_session, sample_filing):
        """Test retrieving filing by accession number."""
        found_filing = Filing.get_by_accession(test_session, "0000320193-23-000006")
        assert found_filing is not None
        assert found_filing.id == sample_filing.id
        assert found_filing.form_type == "10-K"
    
    def test_get_pending_filings(self, test_session, sample_company):
        """Test retrieving pending filings."""
        # Create pending filing
        pending_filing = Filing(
            company_id=sample_company.id,
            accession_number="0000320193-23-000008",
            form_type="8-K",
            filing_date=datetime(2023, 7, 1),
            is_processed=False,
            processing_status=ProcessingStatus.PENDING
        )
        test_session.add(pending_filing)
        
        # Create processed filing
        processed_filing = Filing(
            company_id=sample_company.id,
            accession_number="0000320193-23-000009",
            form_type="8-K",
            filing_date=datetime(2023, 6, 1),
            is_processed=True,
            processing_status=ProcessingStatus.COMPLETED
        )
        test_session.add(processed_filing)
        test_session.commit()
        
        pending_filings = Filing.get_pending_filings(test_session)
        pending_accessions = [f.accession_number for f in pending_filings]
        
        assert "0000320193-23-000008" in pending_accessions
        assert "0000320193-23-000009" not in pending_accessions
    
    def test_filing_status_transitions(self, test_session, sample_filing):
        """Test filing status transition methods."""
        # Test processing started
        sample_filing.mark_processing_started(test_session)
        assert sample_filing.processing_status == ProcessingStatus.IN_PROGRESS
        assert sample_filing.processing_attempts == 1
        assert sample_filing.last_processing_attempt is not None
        
        # Test processing completed
        sample_filing.mark_processing_completed(test_session)
        assert sample_filing.is_processed is True
        assert sample_filing.processing_status == ProcessingStatus.COMPLETED
        assert sample_filing.processed_at is not None
        assert sample_filing.error_message is None
        
        # Test processing failed
        sample_filing.mark_processing_failed(test_session, "Test error")
        assert sample_filing.processing_status == ProcessingStatus.FAILED
        assert sample_filing.error_message == "Test error"


class TestDocumentModel:
    """Test cases for Document model."""
    
    def test_document_creation(self, test_session, sample_filing):
        """Test creating a document record."""
        document = Document(
            filing_id=sample_filing.id,
            sequence="1",
            description="Primary Document",
            document_name="aapl-20230930.htm",
            document_type="EX-99.1",
            file_size=1024000
        )
        test_session.add(document)
        test_session.commit()
        
        assert document.id is not None
        assert document.document_name == "aapl-20230930.htm"
        assert document.file_size == 1024000
        assert document.is_downloaded is False
        assert document.download_attempts == 0
    
    def test_mark_downloaded(self, test_session, sample_filing):
        """Test marking document as downloaded."""
        document = Document(
            filing_id=sample_filing.id,
            document_name="test-document.htm"
        )
        test_session.add(document)
        test_session.commit()
        
        document.mark_downloaded(
            test_session,
            s3_bucket="test-bucket",
            s3_key="test/key/document.htm",
            content_hash="abc123"
        )
        
        assert document.is_downloaded is True
        assert document.downloaded_at is not None
        assert document.s3_bucket == "test-bucket"
        assert document.s3_key == "test/key/document.htm"
        assert document.content_hash == "abc123"


class TestProcessingJobModel:
    """Test cases for ProcessingJob model."""
    
    def test_job_creation(self, test_session, sample_company):
        """Test creating a processing job."""
        job = ProcessingJob.create_job(
            session=test_session,
            job_type="daily_etl",
            job_name="Test ETL Job",
            company_id=sample_company.id,
            parameters={"batch_size": 50},
            ticker_list=["AAPL", "MSFT"]
        )
        
        assert job.id is not None
        assert job.job_uuid is not None
        assert job.job_type == "daily_etl"
        assert job.job_name == "Test ETL Job"
        assert job.status == ProcessingStatus.PENDING
        assert job.parameters == {"batch_size": 50}
        assert job.ticker_list == ["AAPL", "MSFT"]
    
    def test_job_lifecycle(self, test_session):
        """Test complete job lifecycle."""
        job = ProcessingJob.create_job(
            session=test_session,
            job_type="test_job",
            job_name="Test Job Lifecycle"
        )
        
        # Start job
        job.start_job(test_session, total_items=100)
        assert job.status == ProcessingStatus.IN_PROGRESS
        assert job.started_at is not None
        assert job.total_items == 100
        
        # Update progress
        job.update_progress(test_session, processed_items=50, failed_items=5)
        assert job.processed_items == 50
        assert job.failed_items == 5
        assert job.items_per_second is not None
        
        # Complete job
        job.complete_job(test_session)
        assert job.status == ProcessingStatus.COMPLETED
        assert job.completed_at is not None
        assert job.success_rate is not None
    
    def test_job_failure(self, test_session):
        """Test job failure handling."""
        job = ProcessingJob.create_job(
            session=test_session,
            job_type="test_job",
            job_name="Test Job Failure"
        )
        
        job.start_job(test_session)
        job.fail_job(test_session, "Test failure message")
        
        assert job.status == ProcessingStatus.FAILED
        assert job.error_message == "Test failure message"
        assert job.completed_at is not None
    
    def test_job_progress_percentage(self, test_session):
        """Test job progress percentage calculation."""
        job = ProcessingJob.create_job(
            session=test_session,
            job_type="test_job",
            job_name="Test Progress"
        )
        
        # No total items
        assert job.progress_percentage is None
        
        # With total items
        job.total_items = 100
        job.processed_items = 25
        test_session.commit()
        
        assert job.progress_percentage == 25.0
    
    def test_job_duration(self, test_session):
        """Test job duration calculation."""
        job = ProcessingJob.create_job(
            session=test_session,
            job_type="test_job",
            job_name="Test Duration"
        )
        
        # Job not started
        assert job.duration is None
        
        # Job started
        job.start_job(test_session)
        duration = job.duration
        assert duration is not None
        assert duration >= 0


class TestDatabaseManager:
    """Test cases for DatabaseManager."""
    
    def test_database_config_defaults(self):
        """Test database configuration defaults."""
        config = DatabaseConfig()
        assert config.pool_size == 20
        assert config.max_overflow == 30
        assert config.echo_sql is False
    
    def test_database_manager_creation(self):
        """Test creating database manager."""
        config = DatabaseConfig()
        config.url = "sqlite:///:memory:"
        
        manager = DatabaseManager(config)
        assert manager.config == config
        assert manager._engine is None
        
        # Test engine creation
        engine = manager.engine
        assert engine is not None
        assert manager._engine is engine
    
    def test_session_creation(self):
        """Test session creation."""
        config = DatabaseConfig()
        config.url = "sqlite:///:memory:"
        
        manager = DatabaseManager(config)
        session = manager.get_session()
        
        assert session is not None
        session.close()
    
    def test_session_scope(self):
        """Test session scope context manager."""
        config = DatabaseConfig()
        config.url = "sqlite:///:memory:"
        
        manager = DatabaseManager(config)
        manager.create_all_tables()
        
        with manager.session_scope() as session:
            company = Company(cik="1234567890", ticker="TEST", name="Test Company")
            session.add(company)
            # Commit happens automatically
        
        # Verify data was saved
        with manager.session_scope() as session:
            found_company = session.query(Company).filter_by(ticker="TEST").first()
            assert found_company is not None
            assert found_company.name == "Test Company"
    
    def test_health_check(self):
        """Test database health check."""
        config = DatabaseConfig()
        config.url = "sqlite:///:memory:"
        
        manager = DatabaseManager(config)
        manager.create_all_tables()
        
        assert manager.health_check() is True
    
    def test_connection_info(self):
        """Test getting connection information."""
        config = DatabaseConfig()
        config.url = "sqlite:///:memory:"
        
        manager = DatabaseManager(config)
        info = manager.get_connection_info()
        
        assert "database_url" in info
        assert "is_sqlite" in info
        assert "is_postgresql" in info
        assert info["is_sqlite"] is True
        assert info["is_postgresql"] is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])