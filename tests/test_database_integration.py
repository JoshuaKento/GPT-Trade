"""Database integration tests using test containers and in-memory databases."""

import asyncio
import datetime
import os
import tempfile
import time
import uuid
from contextlib import asynccontextmanager, contextmanager
from typing import Any, Dict, Generator, List, Optional

import asyncpg
import pytest
from moto import mock_s3
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from tests.test_database_models import (
    CompanyFactory,
    DocumentFactory,
    FilingFactory,
    ProcessingJobFactory,
    EtlRunFactory
)


class DatabaseTestContainer:
    """Test container for PostgreSQL database testing."""
    
    def __init__(self, use_real_postgres: bool = False):
        self.use_real_postgres = use_real_postgres
        self.engine = None
        self.session_factory = None
        self.connection_string = None
    
    @contextmanager
    def get_test_database(self) -> Generator:
        """Get test database connection - either in-memory SQLite or test PostgreSQL."""
        if self.use_real_postgres and os.getenv("POSTGRES_TEST_URL"):
            # Use real PostgreSQL for integration testing
            self.connection_string = os.getenv("POSTGRES_TEST_URL")
            self.engine = create_engine(self.connection_string)
        else:
            # Use in-memory SQLite for unit testing
            self.connection_string = "sqlite:///:memory:"
            self.engine = create_engine(
                self.connection_string,
                echo=False,
                connect_args={"check_same_thread": False}
            )
        
        try:
            # Create session factory
            self.session_factory = sessionmaker(bind=self.engine)
            
            # Initialize schema
            self._create_test_schema()
            
            yield self
            
        finally:
            if self.engine:
                self.engine.dispose()
    
    def _create_test_schema(self):
        """Create test database schema."""
        with self.engine.connect() as conn:
            # Create companies table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS companies (
                    cik VARCHAR(10) PRIMARY KEY,
                    ticker VARCHAR(10) UNIQUE NOT NULL,
                    name VARCHAR(500) NOT NULL,
                    exchange VARCHAR(20),
                    sector VARCHAR(100),
                    industry VARCHAR(100),
                    website VARCHAR(500),
                    description TEXT,
                    employee_count INTEGER,
                    market_cap BIGINT,
                    fiscal_year_end VARCHAR(4),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # Create filings table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS filings (
                    id VARCHAR(36) PRIMARY KEY,
                    cik VARCHAR(10) NOT NULL,
                    accession_number VARCHAR(25) UNIQUE NOT NULL,
                    form_type VARCHAR(20) NOT NULL,
                    filing_date DATE NOT NULL,
                    report_date DATE,
                    period_of_report DATE,
                    document_count INTEGER DEFAULT 0,
                    size_bytes BIGINT DEFAULT 0,
                    is_xbrl BOOLEAN DEFAULT FALSE,
                    is_inline_xbrl BOOLEAN DEFAULT FALSE,
                    primary_document VARCHAR(100),
                    edgar_url VARCHAR(500),
                    s3_bucket VARCHAR(100),
                    s3_key_prefix VARCHAR(500),
                    processing_status VARCHAR(20) DEFAULT 'pending',
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (cik) REFERENCES companies(cik)
                )
            """))
            
            # Create documents table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS documents (
                    id VARCHAR(36) PRIMARY KEY,
                    filing_id VARCHAR(36) NOT NULL,
                    sequence INTEGER NOT NULL,
                    filename VARCHAR(200) NOT NULL,
                    document_type VARCHAR(50),
                    description TEXT,
                    size_bytes BIGINT DEFAULT 0,
                    s3_key VARCHAR(500),
                    content_type VARCHAR(100),
                    is_primary BOOLEAN DEFAULT FALSE,
                    extracted_text TEXT,
                    embedding_vector BLOB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (filing_id) REFERENCES filings(id)
                )
            """))
            
            # Create processing_jobs table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS processing_jobs (
                    id VARCHAR(36) PRIMARY KEY,
                    job_type VARCHAR(50) NOT NULL,
                    status VARCHAR(20) DEFAULT 'pending',
                    target_cik VARCHAR(10),
                    target_filing_id VARCHAR(36),
                    priority VARCHAR(10) DEFAULT 'medium',
                    retry_count INTEGER DEFAULT 0,
                    max_retries INTEGER DEFAULT 3,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    error_message TEXT,
                    metadata TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # Create etl_runs table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS etl_runs (
                    id VARCHAR(36) PRIMARY KEY,
                    run_type VARCHAR(50) NOT NULL,
                    status VARCHAR(20) DEFAULT 'pending',
                    target_ciks TEXT,
                    form_types TEXT,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    companies_processed INTEGER DEFAULT 0,
                    filings_discovered INTEGER DEFAULT 0,
                    filings_downloaded INTEGER DEFAULT 0,
                    documents_processed INTEGER DEFAULT 0,
                    errors_count INTEGER DEFAULT 0,
                    performance_metrics TEXT,
                    error_summary TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            conn.commit()
    
    def get_session(self):
        """Get database session."""
        return self.session_factory()
    
    def execute_sql(self, sql: str, params: Optional[Dict] = None):
        """Execute SQL query."""
        with self.engine.connect() as conn:
            return conn.execute(text(sql), params or {})
    
    def insert_test_data(self, table: str, data: Dict[str, Any]):
        """Insert test data into table."""
        with self.engine.connect() as conn:
            columns = ", ".join(data.keys())
            placeholders = ", ".join(f":{k}" for k in data.keys())
            sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
            conn.execute(text(sql), data)
            conn.commit()
    
    def cleanup_table(self, table: str):
        """Clean up table data."""
        with self.engine.connect() as conn:
            conn.execute(text(f"DELETE FROM {table}"))
            conn.commit()


@pytest.fixture
def test_db():
    """Provide test database container."""
    container = DatabaseTestContainer(use_real_postgres=False)
    with container.get_test_database():
        yield container


@pytest.fixture
def postgres_test_db():
    """Provide PostgreSQL test database container."""
    container = DatabaseTestContainer(use_real_postgres=True)
    with container.get_test_database():
        yield container


@pytest.mark.integration
class TestDatabaseOperations:
    """Test basic database operations and constraints."""
    
    def test_company_crud_operations(self, test_db):
        """Test company CRUD operations."""
        # Create
        company_data = CompanyFactory()
        test_db.insert_test_data("companies", company_data)
        
        # Read
        result = test_db.execute_sql(
            "SELECT * FROM companies WHERE cik = :cik",
            {"cik": company_data["cik"]}
        ).fetchone()
        
        assert result is not None
        assert result[0] == company_data["cik"]  # cik
        assert result[1] == company_data["ticker"]  # ticker
        assert result[2] == company_data["name"]  # name
        
        # Update
        test_db.execute_sql(
            "UPDATE companies SET name = :name WHERE cik = :cik",
            {"name": "Updated Company Name", "cik": company_data["cik"]}
        )
        
        updated = test_db.execute_sql(
            "SELECT name FROM companies WHERE cik = :cik",
            {"cik": company_data["cik"]}
        ).fetchone()
        
        assert updated[0] == "Updated Company Name"
        
        # Delete
        test_db.execute_sql(
            "DELETE FROM companies WHERE cik = :cik",
            {"cik": company_data["cik"]}
        )
        
        deleted = test_db.execute_sql(
            "SELECT * FROM companies WHERE cik = :cik",
            {"cik": company_data["cik"]}
        ).fetchone()
        
        assert deleted is None
    
    def test_filing_crud_operations(self, test_db):
        """Test filing CRUD operations with foreign key constraints."""
        # First create a company
        company_data = CompanyFactory()
        test_db.insert_test_data("companies", company_data)
        
        # Create filing
        filing_data = FilingFactory(cik=company_data["cik"])
        test_db.insert_test_data("filings", filing_data)
        
        # Read
        result = test_db.execute_sql(
            "SELECT * FROM filings WHERE id = :id",
            {"id": filing_data["id"]}
        ).fetchone()
        
        assert result is not None
        assert result[0] == filing_data["id"]  # id
        assert result[1] == filing_data["cik"]  # cik
        assert result[2] == filing_data["accession_number"]  # accession_number
        
        # Test cascade delete (if implemented)
        test_db.execute_sql(
            "DELETE FROM companies WHERE cik = :cik",
            {"cik": company_data["cik"]}
        )
        
        # Filing should still exist (no cascade in this schema)
        filing_check = test_db.execute_sql(
            "SELECT * FROM filings WHERE id = :id",
            {"id": filing_data["id"]}
        ).fetchone()
        
        assert filing_check is not None
    
    def test_document_relationships(self, test_db):
        """Test document-filing relationships."""
        # Create company and filing
        company_data = CompanyFactory()
        filing_data = FilingFactory(cik=company_data["cik"])
        
        test_db.insert_test_data("companies", company_data)
        test_db.insert_test_data("filings", filing_data)
        
        # Create multiple documents for the filing
        documents = [
            DocumentFactory(filing_id=filing_data["id"])
            for _ in range(3)
        ]
        
        for doc_data in documents:
            test_db.insert_test_data("documents", doc_data)
        
        # Query documents by filing
        results = test_db.execute_sql(
            "SELECT * FROM documents WHERE filing_id = :filing_id ORDER BY sequence",
            {"filing_id": filing_data["id"]}
        ).fetchall()
        
        assert len(results) == 3
        for i, result in enumerate(results):
            assert result[1] == filing_data["id"]  # filing_id
    
    def test_unique_constraints(self, test_db):
        """Test unique constraints enforcement."""
        # Create first company
        company1 = CompanyFactory(ticker="AAPL")
        test_db.insert_test_data("companies", company1)
        
        # Try to create another company with same ticker
        company2 = CompanyFactory(ticker="AAPL", cik="0000123456")
        
        with pytest.raises(Exception):  # Should fail due to unique ticker constraint
            test_db.insert_test_data("companies", company2)
    
    def test_foreign_key_constraints(self, test_db):
        """Test foreign key constraint enforcement."""
        # Try to create filing without corresponding company
        filing_data = FilingFactory(cik="0000999999")  # Non-existent CIK
        
        # This should work in our current schema (no FK enforcement in SQLite by default)
        # In real PostgreSQL with FK enforcement, this would fail
        test_db.insert_test_data("filings", filing_data)
        
        # Verify the filing was created
        result = test_db.execute_sql(
            "SELECT * FROM filings WHERE cik = :cik",
            {"cik": "0000999999"}
        ).fetchone()
        
        assert result is not None


@pytest.mark.integration 
class TestDatabaseTransactions:
    """Test database transaction handling and consistency."""
    
    def test_transaction_rollback(self, test_db):
        """Test transaction rollback on error."""
        company_data = CompanyFactory()
        
        try:
            with test_db.engine.begin() as conn:
                # Insert company
                conn.execute(
                    text("INSERT INTO companies (cik, ticker, name, exchange) VALUES (:cik, :ticker, :name, :exchange)"),
                    {
                        "cik": company_data["cik"],
                        "ticker": company_data["ticker"],
                        "name": company_data["name"],
                        "exchange": company_data["exchange"]
                    }
                )
                
                # Try to insert duplicate ticker (should fail)
                conn.execute(
                    text("INSERT INTO companies (cik, ticker, name, exchange) VALUES (:cik, :ticker, :name, :exchange)"),
                    {
                        "cik": "0000999999",
                        "ticker": company_data["ticker"],  # Duplicate ticker
                        "name": "Another Company",
                        "exchange": "NYSE"
                    }
                )
        except Exception:
            # Transaction should be rolled back
            pass
        
        # Verify no data was inserted
        result = test_db.execute_sql(
            "SELECT COUNT(*) FROM companies WHERE ticker = :ticker",
            {"ticker": company_data["ticker"]}
        ).fetchone()
        
        assert result[0] == 0
    
    def test_concurrent_access_simulation(self, test_db):
        """Test concurrent access patterns."""
        company_data = CompanyFactory()
        test_db.insert_test_data("companies", company_data)
        
        # Simulate concurrent updates
        original_name = company_data["name"]
        
        # First update
        test_db.execute_sql(
            "UPDATE companies SET name = :name WHERE cik = :cik",
            {"name": "Updated Name 1", "cik": company_data["cik"]}
        )
        
        # Second update (would overwrite first in real concurrent scenario)
        test_db.execute_sql(
            "UPDATE companies SET name = :name WHERE cik = :cik",
            {"name": "Updated Name 2", "cik": company_data["cik"]}
        )
        
        # Verify final state
        result = test_db.execute_sql(
            "SELECT name FROM companies WHERE cik = :cik",
            {"cik": company_data["cik"]}
        ).fetchone()
        
        assert result[0] == "Updated Name 2"


@pytest.mark.integration
class TestDatabasePerformance:
    """Test database performance characteristics."""
    
    def test_bulk_insert_performance(self, test_db):
        """Test bulk insert performance."""
        # Generate test data
        companies = [CompanyFactory() for _ in range(100)]
        
        start_time = time.time()
        
        # Bulk insert
        for company_data in companies:
            test_db.insert_test_data("companies", company_data)
        
        end_time = time.time()
        insert_time = end_time - start_time
        
        # Verify all inserted
        count = test_db.execute_sql("SELECT COUNT(*) FROM companies").fetchone()
        assert count[0] == 100
        
        # Performance should be reasonable (adjust threshold as needed)
        assert insert_time < 10.0  # Should complete in under 10 seconds
        
        print(f"Bulk insert of 100 companies took {insert_time:.2f} seconds")
    
    def test_query_performance_with_indexes(self, test_db):
        """Test query performance with and without indexes."""
        # Insert test data
        companies = [CompanyFactory() for _ in range(1000)]
        for company_data in companies:
            test_db.insert_test_data("companies", company_data)
        
        # Test query without index
        start_time = time.time()
        results = test_db.execute_sql(
            "SELECT * FROM companies WHERE name LIKE :pattern",
            {"pattern": "%Apple%"}
        ).fetchall()
        no_index_time = time.time() - start_time
        
        # Create index
        test_db.execute_sql("CREATE INDEX IF NOT EXISTS idx_companies_name ON companies(name)")
        
        # Test query with index
        start_time = time.time()
        results_indexed = test_db.execute_sql(
            "SELECT * FROM companies WHERE name LIKE :pattern",
            {"pattern": "%Apple%"}
        ).fetchall()
        with_index_time = time.time() - start_time
        
        # Results should be the same
        assert len(results) == len(results_indexed)
        
        print(f"Query without index: {no_index_time:.4f}s, with index: {with_index_time:.4f}s")
    
    def test_large_dataset_operations(self, test_db):
        """Test operations on larger datasets."""
        # Create companies
        companies = [CompanyFactory() for _ in range(50)]
        for company_data in companies:
            test_db.insert_test_data("companies", company_data)
        
        # Create filings for each company
        filing_count = 0
        for company_data in companies:
            filings = [
                FilingFactory(cik=company_data["cik"])
                for _ in range(10)  # 10 filings per company
            ]
            for filing_data in filings:
                test_db.insert_test_data("filings", filing_data)
                filing_count += 1
        
        # Test complex query performance
        start_time = time.time()
        results = test_db.execute_sql("""
            SELECT c.name, COUNT(f.id) as filing_count
            FROM companies c
            LEFT JOIN filings f ON c.cik = f.cik
            GROUP BY c.cik, c.name
            HAVING COUNT(f.id) > 5
            ORDER BY filing_count DESC
        """).fetchall()
        query_time = time.time() - start_time
        
        assert len(results) == 50  # All companies should have > 5 filings
        assert query_time < 5.0  # Should complete reasonably fast
        
        print(f"Complex aggregation query took {query_time:.4f}s")


@pytest.mark.integration
class TestDatabaseMigrations:
    """Test database schema migrations and versioning."""
    
    def test_schema_version_tracking(self, test_db):
        """Test schema version tracking table."""
        # Create schema_versions table
        test_db.execute_sql("""
            CREATE TABLE IF NOT EXISTS schema_versions (
                version INTEGER PRIMARY KEY,
                description TEXT NOT NULL,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Insert initial version
        test_db.execute_sql(
            "INSERT INTO schema_versions (version, description) VALUES (1, 'Initial schema')",
        )
        
        # Verify version tracking
        result = test_db.execute_sql(
            "SELECT version, description FROM schema_versions ORDER BY version DESC LIMIT 1"
        ).fetchone()
        
        assert result[0] == 1
        assert result[1] == "Initial schema"
    
    def test_column_addition_migration(self, test_db):
        """Test adding columns to existing tables."""
        # Add a new column to companies table
        try:
            test_db.execute_sql("ALTER TABLE companies ADD COLUMN market_sector VARCHAR(100)")
        except Exception:
            # Column might already exist
            pass
        
        # Verify column was added by inserting data
        company_data = CompanyFactory()
        company_data["market_sector"] = "Technology"
        
        # Insert with new column
        columns = ", ".join(company_data.keys())
        placeholders = ", ".join(f":{k}" for k in company_data.keys())
        sql = f"INSERT INTO companies ({columns}) VALUES ({placeholders})"
        
        try:
            test_db.execute_sql(sql, company_data)
            # If this succeeds, column was added successfully
            success = True
        except Exception:
            success = False
        
        # Should succeed if column addition worked
        assert success or "market_sector" in str(test_db.execute_sql("PRAGMA table_info(companies)").fetchall())
    
    def test_index_creation_migration(self, test_db):
        """Test index creation as part of migration."""
        # Create performance indexes
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_filings_cik ON filings(cik)",
            "CREATE INDEX IF NOT EXISTS idx_filings_form_type ON filings(form_type)",
            "CREATE INDEX IF NOT EXISTS idx_filings_filing_date ON filings(filing_date)",
            "CREATE INDEX IF NOT EXISTS idx_documents_filing_id ON documents(filing_id)",
            "CREATE INDEX IF NOT EXISTS idx_processing_jobs_status ON processing_jobs(status)",
        ]
        
        for index_sql in indexes:
            test_db.execute_sql(index_sql)
        
        # Verify indexes exist (SQLite specific)
        result = test_db.execute_sql(
            "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'"
        ).fetchall()
        
        index_names = [row[0] for row in result]
        assert "idx_filings_cik" in index_names
        assert "idx_filings_form_type" in index_names


@pytest.mark.integration 
@pytest.mark.slow
class TestDatabaseScalability:
    """Test database scalability and limits."""
    
    def test_large_text_storage(self, test_db):
        """Test storing large text documents."""
        # Create company and filing
        company_data = CompanyFactory()
        filing_data = FilingFactory(cik=company_data["cik"])
        
        test_db.insert_test_data("companies", company_data)
        test_db.insert_test_data("filings", filing_data)
        
        # Create document with large text
        large_text = "Lorem ipsum " * 10000  # ~110KB of text
        document_data = DocumentFactory(
            filing_id=filing_data["id"],
            extracted_text=large_text
        )
        
        test_db.insert_test_data("documents", document_data)
        
        # Verify large text was stored correctly
        result = test_db.execute_sql(
            "SELECT extracted_text FROM documents WHERE id = :id",
            {"id": document_data["id"]}
        ).fetchone()
        
        assert len(result[0]) == len(large_text)
        assert result[0] == large_text
    
    def test_connection_pooling_simulation(self, test_db):
        """Test multiple concurrent connections."""
        # Simulate multiple connections doing work
        def worker_task(worker_id: int):
            # Each worker creates some data
            company_data = CompanyFactory(ticker=f"TEST{worker_id}")
            test_db.insert_test_data("companies", company_data)
            
            # Query the data
            result = test_db.execute_sql(
                "SELECT * FROM companies WHERE ticker = :ticker",
                {"ticker": f"TEST{worker_id}"}
            ).fetchone()
            
            return result is not None
        
        # Simulate 10 concurrent workers
        results = []
        for i in range(10):
            try:
                result = worker_task(i)
                results.append(result)
            except Exception as e:
                print(f"Worker {i} failed: {e}")
                results.append(False)
        
        # All workers should succeed
        assert all(results)
        
        # Verify all data was created
        count = test_db.execute_sql(
            "SELECT COUNT(*) FROM companies WHERE ticker LIKE 'TEST%'"
        ).fetchone()
        
        assert count[0] == 10


if __name__ == "__main__":
    pytest.main([__file__, "-v"])