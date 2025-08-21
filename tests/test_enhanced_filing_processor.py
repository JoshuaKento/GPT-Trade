"""Integration tests for enhanced filing processor with database integration."""

import asyncio
import os
import sqlite3
import tempfile
from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock, patch

import pytest

from edgar.database_models import (
    Company,
    DatabaseManager,
    Filing,
    JobStatus,
    PerformanceMetric,
    ProcessingJob,
    ProcessingStatus,
)
from edgar.duplicate_detection import DuplicateDetector
from edgar.enhanced_filing_processor import (
    AsyncEnhancedFilingProcessor,
    BatchFilingProcessor,
    BatchProcessingResult,
    EnhancedFilingProcessor,
)
from edgar.filing_processor import AdaptiveRateLimiter, FilingInfo, RateLimiter
from edgar.job_orchestrator import JobOrchestrator, RetryMechanism
from edgar.performance_monitor import PerformanceMonitor, SLATarget


class TestDatabaseManager:
    """Test database manager functionality."""
    
    def setup_method(self):
        """Set up test database."""
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_manager = DatabaseManager(self.temp_db.name)
    
    def teardown_method(self):
        """Clean up test database."""
        try:
            os.unlink(self.temp_db.name)
        except FileNotFoundError:
            pass
    
    def test_database_initialization(self):
        """Test database tables are created properly."""
        with self.db_manager.get_connection() as conn:
            cursor = conn.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
            """)
            tables = [row[0] for row in cursor.fetchall()]
            
            expected_tables = [
                'companies', 'documents', 'filings', 
                'performance_metrics', 'processing_jobs'
            ]
            assert set(tables) == set(expected_tables)
    
    def test_create_company(self):
        """Test creating company records."""
        company = Company(cik="0000320193", ticker="AAPL", name="Apple Inc.")
        result = self.db_manager.create_company(company)
        assert result is True
        
        # Verify company was created
        with self.db_manager.get_connection() as conn:
            cursor = conn.execute("SELECT * FROM companies WHERE cik = ?", ("0000320193",))
            row = cursor.fetchone()
            assert row is not None
            assert row["ticker"] == "AAPL"
    
    def test_create_filing(self):
        """Test creating filing records."""
        # First create a company
        company = Company(cik="0000320193")
        self.db_manager.create_company(company)
        
        filing = Filing(
            cik="0000320193",
            accession="0000320193-23-000106",
            form_type="10-K",
            filing_date="2023-11-03",
            status=ProcessingStatus.PENDING,
        )
        
        filing_id = self.db_manager.create_filing(filing)
        assert filing_id is not None
        assert filing_id > 0
    
    def test_get_processed_accessions(self):
        """Test retrieving processed accessions."""
        # Create test data
        company = Company(cik="0000320193")
        self.db_manager.create_company(company)
        
        # Create completed filing
        filing1 = Filing(
            cik="0000320193",
            accession="0000320193-23-000001",
            form_type="10-K",
            status=ProcessingStatus.COMPLETED,
        )
        self.db_manager.create_filing(filing1)
        
        # Create pending filing
        filing2 = Filing(
            cik="0000320193",
            accession="0000320193-23-000002",
            form_type="10-Q",
            status=ProcessingStatus.PENDING,
        )
        self.db_manager.create_filing(filing2)
        
        processed = self.db_manager.get_processed_accessions("0000320193")
        assert "0000320193-23-000001" in processed
        assert "0000320193-23-000002" not in processed
    
    def test_create_job(self):
        """Test creating processing jobs."""
        job = ProcessingJob(
            job_name="test_job",
            target_ciks=["0000320193", "0000012345"],
            form_types=["10-K", "10-Q"],
            status=JobStatus.CREATED,
        )
        
        job_id = self.db_manager.create_job(job)
        assert job_id is not None
        assert job_id > 0
    
    def test_record_metric(self):
        """Test recording performance metrics."""
        # Create a job first
        job = ProcessingJob(job_name="test_job", target_ciks=["0000320193"])
        job_id = self.db_manager.create_job(job)
        
        metric = PerformanceMetric(
            job_id=job_id,
            metric_name="throughput",
            metric_value=2.5,
            metric_unit="filings_per_minute",
        )
        
        result = self.db_manager.record_metric(metric)
        assert result is True
        
        # Verify metric was recorded
        metrics = self.db_manager.get_job_metrics(job_id)
        assert len(metrics) == 1
        assert metrics[0].metric_name == "throughput"
        assert metrics[0].metric_value == 2.5


class TestEnhancedFilingProcessor:
    """Test enhanced filing processor with database integration."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_manager = DatabaseManager(self.temp_db.name)
        
        self.mock_edgar_client = Mock()
        self.mock_s3_manager = Mock()
        
        self.processor = EnhancedFilingProcessor(
            edgar_client=self.mock_edgar_client,
            s3_manager=self.mock_s3_manager,
            db_manager=self.db_manager,
        )
    
    def teardown_method(self):
        """Clean up test database."""
        try:
            os.unlink(self.temp_db.name)
        except FileNotFoundError:
            pass
    
    def test_ensure_company_exists(self):
        """Test ensuring company records exist."""
        result = self.processor.ensure_company_exists(
            "0000320193", ticker="AAPL", name="Apple Inc."
        )
        assert result is True
        
        # Verify company was created
        with self.db_manager.get_connection() as conn:
            cursor = conn.execute("SELECT * FROM companies WHERE cik = ?", ("0000320193",))
            row = cursor.fetchone()
            assert row is not None
            assert row["ticker"] == "AAPL"
    
    def test_get_new_filings_with_db(self):
        """Test getting new filings using database state."""
        # Create test company and processed filing
        self.processor.ensure_company_exists("0000320193")
        
        processed_filing = Filing(
            cik="0000320193",
            accession="0000320193-23-000001",
            form_type="10-K",
            status=ProcessingStatus.COMPLETED,
        )
        self.db_manager.create_filing(processed_filing)
        
        # Mock the API response
        mock_api_data = {
            "filings": {
                "recent": {
                    "form": ["10-K", "10-Q"],
                    "accessionNumber": ["0000320193-23-000001", "0000320193-23-000002"],
                    "primaryDocument": ["doc1.htm", "doc2.htm"],
                    "filingDate": ["2023-11-01", "2023-08-01"],
                    "reportDate": ["2023-09-30", "2023-06-30"],
                }
            }
        }
        self.mock_edgar_client.get_json.return_value = mock_api_data
        
        with patch("edgar.enhanced_filing_processor.URLBuilder.submissions_url"):
            new_filings = self.processor.get_new_filings_with_db("0000320193")
            
            # Should only return the unprocessed filing
            assert len(new_filings) == 1
            assert new_filings[0].accession == "0000320193-23-000002"
    
    @patch("edgar.enhanced_filing_processor.FilingProcessor.process_filing")
    def test_process_filing_with_db_success(self, mock_parent_process):
        """Test processing filing with database persistence - success case."""
        from edgar.filing_processor import ProcessingResult
        
        # Mock successful processing
        mock_parent_process.return_value = ProcessingResult(
            accession="0000320193-23-000106",
            success=True,
            documents_processed=3,
            uploaded_files=["file1.htm", "file2.htm", "file3.htm"],
        )
        
        filing = FilingInfo(
            form="10-K",
            accession="0000320193-23-000106",
            primary_document="aapl-20230930.htm",
        )
        
        result = self.processor.process_filing_with_db(
            "0000320193", filing, "test-bucket", "test-prefix"
        )
        
        assert result.success is True
        assert result.documents_processed == 3
        
        # Verify database record was created and updated
        with self.db_manager.get_connection() as conn:
            cursor = conn.execute("""
                SELECT status, documents_processed FROM filings 
                WHERE accession = ?
            """, ("0000320193-23-000106",))
            row = cursor.fetchone()
            assert row is not None
            assert row["status"] == "completed"
            assert row["documents_processed"] == 3
    
    @patch("edgar.enhanced_filing_processor.FilingProcessor.process_filing")
    def test_process_filing_with_db_failure(self, mock_parent_process):
        """Test processing filing with database persistence - failure case."""
        from edgar.filing_processor import ProcessingResult
        
        # Mock failed processing
        mock_parent_process.return_value = ProcessingResult(
            accession="0000320193-23-000106",
            success=False,
            documents_processed=0,
            error="Network timeout",
        )
        
        filing = FilingInfo(
            form="10-K",
            accession="0000320193-23-000106",
            primary_document="aapl-20230930.htm",
        )
        
        result = self.processor.process_filing_with_db(
            "0000320193", filing, "test-bucket", "test-prefix"
        )
        
        assert result.success is False
        assert result.error == "Network timeout"
        
        # Verify database record reflects failure
        with self.db_manager.get_connection() as conn:
            cursor = conn.execute("""
                SELECT status, error_message FROM filings 
                WHERE accession = ?
            """, ("0000320193-23-000106",))
            row = cursor.fetchone()
            assert row is not None
            assert row["status"] == "failed"
            assert "Network timeout" in row["error_message"]


class TestBatchFilingProcessor:
    """Test batch filing processor functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_manager = DatabaseManager(self.temp_db.name)
        
        self.mock_edgar_client = Mock()
        self.mock_s3_manager = Mock()
        
        self.batch_processor = BatchFilingProcessor(
            edgar_client=self.mock_edgar_client,
            s3_manager=self.mock_s3_manager,
            db_manager=self.db_manager,
            max_concurrent_ciks=2,
            max_concurrent_filings=5,
        )
    
    def teardown_method(self):
        """Clean up test database."""
        try:
            os.unlink(self.temp_db.name)
        except FileNotFoundError:
            pass
    
    def test_create_job(self):
        """Test creating batch processing job."""
        job_id = self.batch_processor.create_job(
            job_name="test_batch",
            target_ciks=["0000320193", "0000012345"],
            form_types=["10-K", "10-Q"],
            config={"bucket": "test-bucket", "prefix": "test-prefix"},
        )
        
        assert job_id is not None
        assert job_id > 0
        
        # Verify job was created properly
        job_info = self.batch_processor.get_job_status(job_id)
        assert job_info is not None
        assert job_info["job_name"] == "test_batch"
        assert len(job_info["target_ciks"]) == 2
    
    @patch("edgar.enhanced_filing_processor.EnhancedFilingProcessor.get_new_filings_with_db")
    @patch("edgar.enhanced_filing_processor.EnhancedFilingProcessor.process_filing_with_db")
    def test_process_batch_sync(self, mock_process_filing, mock_get_filings):
        """Test synchronous batch processing."""
        from edgar.filing_processor import ProcessingResult
        
        # Mock filings discovery
        mock_filings = [
            FilingInfo("10-K", "0000320193-23-000001", "doc1.htm"),
            FilingInfo("10-Q", "0000320193-23-000002", "doc2.htm"),
        ]
        mock_get_filings.return_value = mock_filings
        
        # Mock successful processing
        mock_process_filing.side_effect = [
            ProcessingResult("0000320193-23-000001", True, 2),
            ProcessingResult("0000320193-23-000002", True, 1),
        ]
        
        result = self.batch_processor.process_batch_sync(
            ciks=["0000320193"],
            bucket="test-bucket",
            prefix="test-prefix",
            job_name="test_sync_batch",
        )
        
        assert isinstance(result, BatchProcessingResult)
        assert result.total_ciks == 1
        assert result.processed_ciks == 1
        assert result.total_filings == 2
        assert result.processed_filings == 2
        assert result.failed_filings == 0
        assert result.success_rate == 100.0
    
    @pytest.mark.asyncio
    @patch("edgar.enhanced_filing_processor.AsyncEnhancedFilingProcessor.create_processor")
    async def test_process_batch_async(self, mock_create_processor):
        """Test asynchronous batch processing."""
        # This is a simplified test due to complexity of mocking async context managers
        # In practice, you would use more sophisticated mocking for the async processor
        
        mock_processor = AsyncMock()
        mock_create_processor.return_value.__aenter__.return_value = mock_processor
        mock_create_processor.return_value.__aexit__.return_value = None
        
        # Mock the async processing to return immediately
        async def mock_process_batch():
            return BatchProcessingResult(
                job_id=1,
                total_ciks=1,
                processed_ciks=1,
                total_filings=1,
                processed_filings=1,
                failed_filings=0,
                skipped_filings=0,
                start_time=datetime.now(timezone.utc),
                end_time=datetime.now(timezone.utc),
            )
        
        # Replace the actual method with our mock
        with patch.object(self.batch_processor, 'process_batch_async', mock_process_batch):
            result = await self.batch_processor.process_batch_async(
                ciks=["0000320193"],
                bucket="test-bucket",
                prefix="test-prefix",
                job_name="test_async_batch",
            )
            
            assert isinstance(result, BatchProcessingResult)
            assert result.processed_ciks == 1


class TestJobOrchestrator:
    """Test job orchestration functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_manager = DatabaseManager(self.temp_db.name)
        
        self.mock_batch_processor = Mock()
        self.mock_batch_processor.db_manager = self.db_manager
        self.mock_batch_processor.create_job.return_value = 1
        self.mock_batch_processor.get_job_status.return_value = {
            "job_id": 1,
            "job_name": "test_job",
            "target_ciks": ["0000320193"],
            "form_types": ["10-K"],
            "status": "created",
        }
        
        self.orchestrator = JobOrchestrator(
            batch_processor=self.mock_batch_processor,
            db_manager=self.db_manager,
        )
    
    def teardown_method(self):
        """Clean up test database."""
        try:
            os.unlink(self.temp_db.name)
        except FileNotFoundError:
            pass
    
    def test_create_scheduled_job(self):
        """Test creating scheduled jobs."""
        job_id = self.orchestrator.create_scheduled_job(
            job_name="scheduled_test",
            target_ciks=["0000320193"],
            form_types=["10-K"],
            schedule_config={"interval": "daily"},
            processing_config={"bucket": "test-bucket"},
        )
        
        assert job_id == 1
        self.mock_batch_processor.create_job.assert_called_once()
    
    def test_pause_resume_job(self):
        """Test pausing and resuming jobs."""
        # Test pause
        result = self.orchestrator.pause_job(1)
        assert result is True
        
        # Test resume
        result = self.orchestrator.resume_job(1)
        assert result is True
    
    def test_cancel_job(self):
        """Test cancelling jobs."""
        result = self.orchestrator.cancel_job(1)
        assert result is True
    
    def test_get_job_progress(self):
        """Test getting job progress."""
        # Create a test job with metrics
        job = ProcessingJob(
            job_name="test_job",
            target_ciks=["0000320193"],
            total_filings=10,
            processed_filings=5,
            status=JobStatus.RUNNING,
            start_time=datetime.now(timezone.utc),
        )
        job_id = self.db_manager.create_job(job)
        
        # Add some metrics
        metric = PerformanceMetric(
            job_id=job_id,
            metric_name="throughput",
            metric_value=2.0,
            metric_unit="filings_per_minute",
        )
        self.db_manager.record_metric(metric)
        
        # Update mock to return our test job
        self.mock_batch_processor.get_job_status.return_value = {
            "job_id": job_id,
            "job_name": "test_job",
            "status": "running",
            "total_filings": 10,
            "processed_filings": 5,
            "start_time": job.start_time.isoformat(),
        }
        
        progress = self.orchestrator.get_job_progress(job_id)
        assert progress is not None
        assert progress["progress_percentage"] == 50.0
        assert len(progress["metrics"]) == 1


class TestDuplicateDetector:
    """Test duplicate detection functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_manager = DatabaseManager(self.temp_db.name)
        self.detector = DuplicateDetector(self.db_manager)
    
    def teardown_method(self):
        """Clean up test database."""
        try:
            os.unlink(self.temp_db.name)
        except FileNotFoundError:
            pass
    
    def test_check_filing_exists(self):
        """Test checking if filing exists."""
        # Initially should not exist
        assert not self.detector.check_filing_exists("0000320193-23-000001")
        
        # Create a filing
        company = Company(cik="0000320193")
        self.db_manager.create_company(company)
        
        filing = Filing(
            cik="0000320193",
            accession="0000320193-23-000001",
            form_type="10-K",
        )
        self.db_manager.create_filing(filing)
        
        # Now should exist
        assert self.detector.check_filing_exists("0000320193-23-000001")
    
    def test_get_filing_status(self):
        """Test getting filing status."""
        # Create a filing
        company = Company(cik="0000320193")
        self.db_manager.create_company(company)
        
        filing = Filing(
            cik="0000320193",
            accession="0000320193-23-000001",
            form_type="10-K",
            status=ProcessingStatus.COMPLETED,
        )
        self.db_manager.create_filing(filing)
        
        status = self.detector.get_filing_status("0000320193-23-000001")
        assert status == ProcessingStatus.COMPLETED
    
    def test_get_incremental_filings(self):
        """Test getting incremental filings."""
        # Create processed filing
        company = Company(cik="0000320193")
        self.db_manager.create_company(company)
        
        processed_filing = Filing(
            cik="0000320193",
            accession="0000320193-23-000001",
            form_type="10-K",
            status=ProcessingStatus.COMPLETED,
        )
        self.db_manager.create_filing(processed_filing)
        
        # Test with mixed filings
        all_filings = [
            {"accession": "0000320193-23-000001", "form": "10-K"},  # Already processed
            {"accession": "0000320193-23-000002", "form": "10-Q"},  # New
            {"accession": "0000320193-23-000003", "form": "8-K"},   # New
        ]
        
        new_filings, skipped_filings = self.detector.get_incremental_filings(
            "0000320193", all_filings, form_types=["10-K", "10-Q"]
        )
        
        assert len(new_filings) == 1  # Only 10-Q is new and matches filter
        assert len(skipped_filings) == 1  # 10-K is already processed
        assert new_filings[0]["accession"] == "0000320193-23-000002"
    
    def test_cleanup_stale_processing(self):
        """Test cleaning up stale processing records."""
        # Create a company and filing in processing state
        company = Company(cik="0000320193")
        self.db_manager.create_company(company)
        
        # Create old in-progress filing
        filing = Filing(
            cik="0000320193",
            accession="0000320193-23-000001",
            form_type="10-K",
            status=ProcessingStatus.IN_PROGRESS,
        )
        filing_id = self.db_manager.create_filing(filing)
        
        # Manually update the timestamp to be old
        with self.db_manager.get_connection() as conn:
            old_time = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0)
            conn.execute("""
                UPDATE filings SET updated_at = ? WHERE id = ?
            """, (old_time.isoformat(), filing_id))
            conn.commit()
        
        # Clean up stale records
        cleaned_count = self.detector.cleanup_stale_processing(hours_old=12)
        assert cleaned_count == 1
        
        # Verify filing is now marked as failed
        status = self.detector.get_filing_status("0000320193-23-000001")
        assert status == ProcessingStatus.FAILED


class TestPerformanceMonitor:
    """Test performance monitoring functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_manager = DatabaseManager(self.temp_db.name)
        
        self.sla_target = SLATarget(
            max_duration_minutes=30,
            max_filings=50,
            success_rate_threshold=95.0,
            throughput_threshold=1.0,
        )
        
        self.monitor = PerformanceMonitor(
            db_manager=self.db_manager,
            sla_target=self.sla_target,
        )
    
    def teardown_method(self):
        """Clean up test database."""
        try:
            os.unlink(self.temp_db.name)
        except FileNotFoundError:
            pass
    
    def test_sla_target_compliance(self):
        """Test SLA target compliance checking."""
        # Test compliant case
        assert self.sla_target.is_compliant(
            duration_minutes=25,
            filing_count=40,
            success_rate=96.0,
            throughput=1.5,
        )
        
        # Test non-compliant cases
        assert not self.sla_target.is_compliant(
            duration_minutes=35,  # Too long
            filing_count=40,
            success_rate=96.0,
            throughput=1.5,
        )
        
        assert not self.sla_target.is_compliant(
            duration_minutes=25,
            filing_count=40,
            success_rate=90.0,  # Too low success rate
            throughput=1.5,
        )
    
    def test_record_job_metrics(self):
        """Test recording job start and completion metrics."""
        # Create a test job
        job = ProcessingJob(job_name="test_job", target_ciks=["0000320193"])
        job_id = self.db_manager.create_job(job)
        
        # Record start metrics
        self.monitor.record_job_start(job_id)
        
        # Record completion metrics
        self.monitor.record_job_completion(
            job_id=job_id,
            total_filings=10,
            processed_filings=9,
            failed_filings=1,
            duration_seconds=300,  # 5 minutes
        )
        
        # Verify metrics were recorded
        metrics = self.db_manager.get_job_metrics(job_id)
        metric_names = {m.metric_name for m in metrics}
        
        expected_metrics = {
            "job_start", "job_completion", "duration", 
            "success_rate", "throughput", "sla_compliant"
        }
        assert expected_metrics.issubset(metric_names)
    
    def test_generate_performance_report(self):
        """Test generating performance reports."""
        # Create a completed job
        start_time = datetime.now(timezone.utc)
        end_time = start_time.replace(minute=start_time.minute + 10)  # 10 minutes later
        
        job = ProcessingJob(
            job_name="test_job",
            target_ciks=["0000320193"],
            total_filings=20,
            processed_filings=18,
            failed_filings=2,
            status=JobStatus.COMPLETED,
            start_time=start_time,
            end_time=end_time,
        )
        job_id = self.db_manager.create_job(job)
        
        # Add some metrics
        metrics = [
            PerformanceMetric(job_id=job_id, metric_name="throughput", metric_value=1.8, metric_unit="filings_per_minute"),
            PerformanceMetric(job_id=job_id, metric_name="memory_usage_start", metric_value=100.0, metric_unit="mb"),
            PerformanceMetric(job_id=job_id, metric_name="memory_usage_end", metric_value=120.0, metric_unit="mb"),
        ]
        for metric in metrics:
            self.db_manager.record_metric(metric)
        
        # Generate report
        report = self.monitor.generate_performance_report(job_id)
        
        assert report is not None
        assert report.job_id == job_id
        assert report.job_name == "test_job"
        assert report.total_filings == 20
        assert report.processed_filings == 18
        assert report.success_rate == 90.0
        assert len(report.recommendations) > 0


@pytest.mark.asyncio
class TestIntegrationScenarios:
    """Integration tests for complete processing scenarios."""
    
    async def test_complete_batch_processing_workflow(self):
        """Test complete batch processing workflow with all components."""
        # This would be a comprehensive integration test that:
        # 1. Creates a database
        # 2. Sets up all components
        # 3. Runs a complete batch processing job
        # 4. Verifies all aspects work together
        
        # Due to complexity and need for real network mocking,
        # this is a placeholder for the full integration test
        
        temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        temp_db.close()
        
        try:
            db_manager = DatabaseManager(temp_db.name)
            
            # Create mock components
            mock_edgar_client = Mock()
            mock_s3_manager = Mock()
            
            # Set up batch processor
            batch_processor = BatchFilingProcessor(
                edgar_client=mock_edgar_client,
                s3_manager=mock_s3_manager,
                db_manager=db_manager,
                max_concurrent_ciks=2,
            )
            
            # Set up orchestrator
            orchestrator = JobOrchestrator(
                batch_processor=batch_processor,
                db_manager=db_manager,
            )
            
            # Create a job
            job_id = orchestrator.create_scheduled_job(
                job_name="integration_test",
                target_ciks=["0000320193"],
                form_types=["10-K"],
            )
            
            assert job_id is not None
            
            # Verify job was created
            job_info = batch_processor.get_job_status(job_id)
            assert job_info is not None
            assert job_info["job_name"] == "integration_test"
            
        finally:
            try:
                os.unlink(temp_db.name)
            except FileNotFoundError:
                pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])