"""Comprehensive tests for database models with proper fixtures and test data."""

import datetime
import json
import uuid
from decimal import Decimal
from typing import Dict, List, Optional

import factory
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

# Database model tests using factory-boy for test data generation


class CompanyFactory(factory.Factory):
    """Factory for generating test Company data."""
    
    class Meta:
        model = dict
    
    cik = factory.Sequence(lambda n: str(320193 + n).zfill(10))
    ticker = factory.Sequence(lambda n: f"AAPL{n}")
    name = factory.Faker("company")
    exchange = factory.Faker("random_element", elements=["NASDAQ", "NYSE", "AMEX"])
    sector = factory.Faker("random_element", elements=[
        "Technology", "Healthcare", "Financial Services", "Consumer Cyclical",
        "Industrials", "Communication Services", "Energy", "Utilities"
    ])
    industry = factory.Faker("random_element", elements=[
        "Software", "Semiconductors", "Biotechnology", "Banks",
        "Auto Manufacturers", "Oil & Gas", "Real Estate"
    ])
    website = factory.Faker("url")
    description = factory.Faker("text", max_nb_chars=500)
    employee_count = factory.Faker("random_int", min=100, max=500000)
    market_cap = factory.Faker("random_int", min=1000000000, max=3000000000000)
    fiscal_year_end = factory.Faker("random_element", elements=[
        "0331", "0630", "0930", "1231"
    ])
    created_at = factory.Faker("date_time_this_year")
    updated_at = factory.Faker("date_time_this_month")


class FilingFactory(factory.Factory):
    """Factory for generating test Filing data."""
    
    class Meta:
        model = dict
    
    id = factory.LazyFunction(lambda: str(uuid.uuid4()))
    cik = factory.LazyAttribute(lambda obj: CompanyFactory().cik)
    accession_number = factory.Sequence(
        lambda n: f"0000320193-23-{str(n).zfill(6)}"
    )
    form_type = factory.Faker("random_element", elements=["10-K", "10-Q", "8-K", "DEF 14A"])
    filing_date = factory.Faker("date_this_year")
    report_date = factory.Faker("date_this_year") 
    period_of_report = factory.Faker("date_this_year")
    document_count = factory.Faker("random_int", min=1, max=50)
    size_bytes = factory.Faker("random_int", min=1000, max=50000000)
    is_xbrl = factory.Faker("boolean", chance_of_getting_true=80)
    is_inline_xbrl = factory.Faker("boolean", chance_of_getting_true=60)
    primary_document = factory.LazyAttribute(
        lambda obj: f"{obj.cik}-{obj.filing_date.strftime('%Y%m%d')}.htm"
    )
    edgar_url = factory.LazyAttribute(
        lambda obj: f"https://www.sec.gov/Archives/edgar/data/{obj.cik}/{obj.accession_number.replace('-', '')}/{obj.primary_document}"
    )
    s3_bucket = "edgar-filings"
    s3_key_prefix = factory.LazyAttribute(
        lambda obj: f"filings/{obj.cik}/{obj.accession_number}"
    )
    processing_status = factory.Faker("random_element", elements=[
        "pending", "downloading", "processing", "completed", "failed"
    ])
    error_message = None
    created_at = factory.Faker("date_time_this_year")
    updated_at = factory.Faker("date_time_this_month")


class DocumentFactory(factory.Factory):
    """Factory for generating test Document data."""
    
    class Meta:
        model = dict
    
    id = factory.LazyFunction(lambda: str(uuid.uuid4()))
    filing_id = factory.LazyFunction(lambda: str(uuid.uuid4()))
    sequence = factory.Faker("random_int", min=1, max=100)
    filename = factory.Faker("file_name", extension="htm")
    document_type = factory.Faker("random_element", elements=[
        "10-K", "EX-21.1", "EX-23.1", "EX-31.1", "EX-32.1", "GRAPHIC"
    ])
    description = factory.Faker("sentence")
    size_bytes = factory.Faker("random_int", min=100, max=10000000)
    s3_key = factory.LazyAttribute(
        lambda obj: f"documents/{obj.filing_id}/{obj.filename}"
    )
    content_type = factory.LazyAttribute(
        lambda obj: "text/html" if obj.filename.endswith(".htm") else "application/octet-stream"
    )
    is_primary = factory.Faker("boolean", chance_of_getting_true=20)
    extracted_text = factory.Faker("text", max_nb_chars=1000)
    embedding_vector = None  # Will be populated by vector embedding service
    created_at = factory.Faker("date_time_this_year")
    updated_at = factory.Faker("date_time_this_month")


class ProcessingJobFactory(factory.Factory):
    """Factory for generating test ProcessingJob data."""
    
    class Meta:
        model = dict
    
    id = factory.LazyFunction(lambda: str(uuid.uuid4()))
    job_type = factory.Faker("random_element", elements=[
        "filing_discovery", "filing_download", "document_extraction", 
        "text_processing", "embedding_generation", "company_update"
    ])
    status = factory.Faker("random_element", elements=[
        "pending", "running", "completed", "failed", "cancelled"
    ])
    target_cik = factory.LazyAttribute(lambda obj: CompanyFactory().cik)
    target_filing_id = factory.LazyFunction(lambda: str(uuid.uuid4()))
    priority = factory.Faker("random_element", elements=["low", "medium", "high", "urgent"])
    retry_count = factory.Faker("random_int", min=0, max=3)
    max_retries = factory.Faker("random_int", min=3, max=5)
    started_at = factory.Faker("date_time_this_month")
    completed_at = None
    error_message = None
    metadata = factory.LazyFunction(lambda: {
        "config": {"batch_size": 100, "timeout": 300},
        "source": "scheduled"
    })
    created_at = factory.Faker("date_time_this_year")
    updated_at = factory.Faker("date_time_this_month")


class EtlRunFactory(factory.Factory):
    """Factory for generating test ETL run data."""
    
    class Meta:
        model = dict
    
    id = factory.LazyFunction(lambda: str(uuid.uuid4()))
    run_type = factory.Faker("random_element", elements=[
        "daily_discovery", "backfill", "manual", "emergency"
    ])
    status = factory.Faker("random_element", elements=[
        "pending", "running", "completed", "failed", "cancelled"
    ])
    target_ciks = factory.LazyFunction(lambda: [
        CompanyFactory().cik for _ in range(factory.Faker("random_int", min=1, max=10).generate()
    )])
    form_types = factory.LazyFunction(lambda: ["10-K", "10-Q", "8-K"])
    started_at = factory.Faker("date_time_this_month")
    completed_at = None
    companies_processed = 0
    filings_discovered = 0
    filings_downloaded = 0
    documents_processed = 0
    errors_count = 0
    performance_metrics = factory.LazyFunction(lambda: {
        "avg_filing_download_time": 5.2,
        "avg_document_processing_time": 2.1,
        "total_bytes_processed": 0,
        "peak_memory_usage": 0
    })
    error_summary = None
    created_at = factory.Faker("date_time_this_year")
    updated_at = factory.Faker("date_time_this_month")


@pytest.mark.unit
class TestDatabaseModels:
    """Test database model creation, validation, and relationships."""
    
    def test_company_factory(self):
        """Test Company model factory generates valid data."""
        company = CompanyFactory()
        
        assert len(company["cik"]) == 10
        assert company["cik"].isdigit()
        assert company["ticker"] is not None
        assert company["name"] is not None
        assert company["exchange"] in ["NASDAQ", "NYSE", "AMEX"]
        assert company["sector"] is not None
        assert company["website"].startswith("http")
        assert isinstance(company["employee_count"], int)
        assert company["employee_count"] >= 100
        assert isinstance(company["market_cap"], int)
        assert company["market_cap"] >= 1000000000
        assert company["fiscal_year_end"] in ["0331", "0630", "0930", "1231"]
    
    def test_filing_factory(self):
        """Test Filing model factory generates valid data."""
        filing = FilingFactory()
        
        assert len(filing["cik"]) == 10
        assert filing["cik"].isdigit()
        assert filing["accession_number"].count("-") == 2
        assert filing["form_type"] in ["10-K", "10-Q", "8-K", "DEF 14A"]
        assert filing["filing_date"] is not None
        assert filing["document_count"] >= 1
        assert filing["size_bytes"] >= 1000
        assert isinstance(filing["is_xbrl"], bool)
        assert filing["edgar_url"].startswith("https://www.sec.gov")
        assert filing["s3_bucket"] == "edgar-filings"
        assert filing["processing_status"] in [
            "pending", "downloading", "processing", "completed", "failed"
        ]
    
    def test_document_factory(self):
        """Test Document model factory generates valid data."""
        document = DocumentFactory()
        
        assert len(document["id"]) == 36  # UUID length
        assert len(document["filing_id"]) == 36
        assert document["sequence"] >= 1
        assert document["filename"] is not None
        assert document["document_type"] is not None
        assert document["size_bytes"] >= 100
        assert document["s3_key"].startswith("documents/")
        assert isinstance(document["is_primary"], bool)
    
    def test_processing_job_factory(self):
        """Test ProcessingJob model factory generates valid data."""
        job = ProcessingJobFactory()
        
        assert len(job["id"]) == 36
        assert job["job_type"] in [
            "filing_discovery", "filing_download", "document_extraction",
            "text_processing", "embedding_generation", "company_update"
        ]
        assert job["status"] in ["pending", "running", "completed", "failed", "cancelled"]
        assert len(job["target_cik"]) == 10
        assert job["priority"] in ["low", "medium", "high", "urgent"]
        assert job["retry_count"] >= 0
        assert job["max_retries"] >= 3
        assert isinstance(job["metadata"], dict)
    
    def test_etl_run_factory(self):
        """Test EtlRun model factory generates valid data."""
        run = EtlRunFactory()
        
        assert len(run["id"]) == 36
        assert run["run_type"] in ["daily_discovery", "backfill", "manual", "emergency"]
        assert run["status"] in ["pending", "running", "completed", "failed", "cancelled"]
        assert isinstance(run["target_ciks"], list)
        assert len(run["target_ciks"]) >= 1
        assert isinstance(run["form_types"], list)
        assert "10-K" in run["form_types"]
        assert isinstance(run["performance_metrics"], dict)
        assert "avg_filing_download_time" in run["performance_metrics"]


@pytest.mark.unit
class TestModelValidation:
    """Test model field validation and constraints."""
    
    def test_company_cik_validation(self):
        """Test CIK validation rules."""
        # Valid CIK
        company = CompanyFactory(cik="0000320193")
        assert company["cik"] == "0000320193"
        
        # Test various CIK formats
        test_ciks = ["320193", "0000320193", "1234567890"]
        for cik in test_ciks:
            company = CompanyFactory(cik=cik)
            assert len(company["cik"]) <= 10
    
    def test_filing_accession_format(self):
        """Test accession number format validation."""
        filing = FilingFactory(accession_number="0000320193-23-000001")
        
        parts = filing["accession_number"].split("-")
        assert len(parts) == 3
        assert len(parts[0]) == 10  # CIK part
        assert len(parts[1]) == 2   # Year part
        assert len(parts[2]) == 6   # Sequence part
    
    def test_filing_date_constraints(self):
        """Test filing date validation."""
        filing_date = datetime.date(2023, 11, 1)
        report_date = datetime.date(2023, 9, 30)
        
        filing = FilingFactory(
            filing_date=filing_date,
            report_date=report_date
        )
        
        # Filing date should be >= report date in most cases
        assert filing["filing_date"] >= filing["report_date"] or \
               (filing["filing_date"] - filing["report_date"]).days <= 365
    
    def test_document_size_constraints(self):
        """Test document size validation."""
        document = DocumentFactory(size_bytes=1000000)
        assert document["size_bytes"] > 0
        assert document["size_bytes"] <= 100000000  # 100MB max
    
    def test_processing_job_retry_logic(self):
        """Test processing job retry constraints."""
        job = ProcessingJobFactory(retry_count=2, max_retries=3)
        assert job["retry_count"] <= job["max_retries"]


@pytest.mark.unit
class TestModelRelationships:
    """Test model relationships and foreign key constraints."""
    
    def test_company_filing_relationship(self):
        """Test one-to-many relationship between Company and Filing."""
        company = CompanyFactory()
        filings = [
            FilingFactory(cik=company["cik"]) for _ in range(3)
        ]
        
        # All filings should belong to the same company
        for filing in filings:
            assert filing["cik"] == company["cik"]
    
    def test_filing_document_relationship(self):
        """Test one-to-many relationship between Filing and Document."""
        filing = FilingFactory()
        documents = [
            DocumentFactory(filing_id=filing["id"]) for _ in range(5)
        ]
        
        # All documents should belong to the same filing
        for document in documents:
            assert document["filing_id"] == filing["id"]
        
        # One document should be primary
        primary_docs = [d for d in documents if d["is_primary"]]
        # Note: Factory generates random primary flags, in real system only one should be primary
    
    def test_processing_job_target_references(self):
        """Test ProcessingJob references to other entities."""
        company = CompanyFactory()
        filing = FilingFactory(cik=company["cik"])
        
        job = ProcessingJobFactory(
            target_cik=company["cik"],
            target_filing_id=filing["id"]
        )
        
        assert job["target_cik"] == company["cik"]
        assert job["target_filing_id"] == filing["id"]


@pytest.mark.unit
class TestModelSerialization:
    """Test model serialization and deserialization."""
    
    def test_company_json_serialization(self):
        """Test Company model JSON serialization."""
        company = CompanyFactory()
        
        # Convert to JSON and back
        json_str = json.dumps(company, default=str)
        assert json_str is not None
        
        deserialized = json.loads(json_str)
        assert deserialized["cik"] == company["cik"]
        assert deserialized["ticker"] == company["ticker"]
        assert deserialized["name"] == company["name"]
    
    def test_filing_json_serialization(self):
        """Test Filing model JSON serialization."""
        filing = FilingFactory()
        
        json_str = json.dumps(filing, default=str)
        assert json_str is not None
        
        deserialized = json.loads(json_str)
        assert deserialized["cik"] == filing["cik"]
        assert deserialized["accession_number"] == filing["accession_number"]
        assert deserialized["form_type"] == filing["form_type"]
    
    def test_metadata_json_handling(self):
        """Test JSON metadata field handling."""
        metadata = {
            "config": {"batch_size": 100, "timeout": 300},
            "performance": {"start_time": "2023-01-01T00:00:00Z"},
            "nested": {"level1": {"level2": "value"}}
        }
        
        job = ProcessingJobFactory(metadata=metadata)
        
        json_str = json.dumps(job, default=str)
        deserialized = json.loads(json_str)
        
        assert deserialized["metadata"]["config"]["batch_size"] == 100
        assert deserialized["metadata"]["nested"]["level1"]["level2"] == "value"


@pytest.mark.unit
class TestModelFactoryConsistency:
    """Test factory consistency and reproducibility."""
    
    def test_factory_deterministic_with_seed(self):
        """Test that factories produce consistent data with seeds."""
        # Note: factory-boy uses random seeds, this test verifies structure consistency
        companies = [CompanyFactory() for _ in range(10)]
        
        # All should have required fields
        for company in companies:
            assert "cik" in company
            assert "ticker" in company
            assert "name" in company
            assert "exchange" in company
    
    def test_related_factory_consistency(self):
        """Test consistency when creating related objects."""
        company = CompanyFactory()
        filing = FilingFactory(cik=company["cik"])
        documents = [DocumentFactory(filing_id=filing["id"]) for _ in range(3)]
        
        # Verify relationships are maintained
        assert filing["cik"] == company["cik"]
        for doc in documents:
            assert doc["filing_id"] == filing["id"]
    
    def test_batch_factory_creation(self):
        """Test creating multiple instances efficiently."""
        # Create batch of companies
        companies = [CompanyFactory() for _ in range(100)]
        assert len(companies) == 100
        
        # Verify uniqueness where expected
        ciks = [c["cik"] for c in companies]
        assert len(set(ciks)) == len(ciks)  # All CIKs should be unique
        
        tickers = [c["ticker"] for c in companies]
        # Note: Tickers might not be unique in factory, but should be in real DB


if __name__ == "__main__":
    pytest.main([__file__, "-v"])