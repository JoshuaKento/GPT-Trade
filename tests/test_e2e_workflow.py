"""End-to-end workflow tests for complete system validation."""

import asyncio
import json
import tempfile
import time
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Set
from unittest.mock import Mock, patch, MagicMock

import pytest
import boto3
from moto import mock_s3

from tests.test_database_models import (
    CompanyFactory,
    FilingFactory,
    DocumentFactory,
    ProcessingJobFactory,
    EtlRunFactory
)
from tests.test_database_integration import DatabaseTestContainer
from tests.test_etl_pipeline import MockEdgarAPI, MockETLPipeline


class E2EWorkflowOrchestrator:
    """Orchestrates end-to-end workflow testing."""
    
    def __init__(self, db_container, s3_client, bucket_name):
        self.db_container = db_container
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.state_file = None
        self.workflow_state = {}
    
    def initialize_companies(self, companies_data: List[Dict]) -> None:
        """Initialize companies in database."""
        for company_data in companies_data:
            self.db_container.insert_test_data("companies", company_data)
        
        self.workflow_state["companies_count"] = len(companies_data)
    
    def create_etl_run(self, run_config: Dict) -> str:
        """Create new ETL run record."""
        etl_run = EtlRunFactory()
        etl_run.update(run_config)
        
        self.db_container.insert_test_data("etl_runs", etl_run)
        self.workflow_state["current_etl_run"] = etl_run["id"]
        
        return etl_run["id"]
    
    def update_etl_run_status(self, run_id: str, status: str, metrics: Dict = None):
        """Update ETL run status and metrics."""
        update_data = {"status": status, "updated_at": datetime.now()}
        
        if metrics:
            update_data.update(metrics)
        
        # Build update query
        set_clauses = ", ".join(f"{k} = :{k}" for k in update_data.keys())
        sql = f"UPDATE etl_runs SET {set_clauses} WHERE id = :id"
        
        params = update_data.copy()
        params["id"] = run_id
        
        self.db_container.execute_sql(sql, params)
        
        self.workflow_state["etl_run_status"] = status
    
    def record_filing_discovery(self, cik: str, filings: List[Dict]) -> List[str]:
        """Record discovered filings in database."""
        filing_ids = []
        
        for filing_data in filings:
            filing = FilingFactory(
                cik=cik,
                accession_number=filing_data["accession"],
                form_type=filing_data["form"],
                filing_date=filing_data.get("filing_date"),
                processing_status="discovered"
            )
            
            self.db_container.insert_test_data("filings", filing)
            filing_ids.append(filing["id"])
        
        self.workflow_state.setdefault("discovered_filings", []).extend(filing_ids)
        return filing_ids
    
    def record_filing_processing(self, filing_id: str, documents: List[Dict], success: bool) -> List[str]:
        """Record filing processing results."""
        # Update filing status
        status = "completed" if success else "failed"
        self.db_container.execute_sql(
            "UPDATE filings SET processing_status = :status, updated_at = :updated_at WHERE id = :id",
            {"status": status, "updated_at": datetime.now(), "id": filing_id}
        )
        
        document_ids = []
        if success:
            # Record documents
            for i, doc_data in enumerate(documents):
                document = DocumentFactory(
                    filing_id=filing_id,
                    sequence=i + 1,
                    filename=doc_data["filename"],
                    size_bytes=doc_data.get("size", 0),
                    s3_key=doc_data.get("s3_key"),
                    is_primary=(i == 0)  # First document is primary
                )
                
                self.db_container.insert_test_data("documents", document)
                document_ids.append(document["id"])
        
        self.workflow_state.setdefault("processed_documents", []).extend(document_ids)
        return document_ids
    
    def create_processing_job(self, job_type: str, target_cik: str = None, target_filing_id: str = None) -> str:
        """Create processing job record."""
        job = ProcessingJobFactory(
            job_type=job_type,
            status="pending",
            target_cik=target_cik,
            target_filing_id=target_filing_id
        )
        
        self.db_container.insert_test_data("processing_jobs", job)
        self.workflow_state.setdefault("processing_jobs", []).append(job["id"])
        
        return job["id"]
    
    def update_processing_job(self, job_id: str, status: str, error_message: str = None):
        """Update processing job status."""
        update_data = {
            "status": status,
            "updated_at": datetime.now()
        }
        
        if status == "running":
            update_data["started_at"] = datetime.now()
        elif status in ["completed", "failed"]:
            update_data["completed_at"] = datetime.now()
        
        if error_message:
            update_data["error_message"] = error_message
        
        set_clauses = ", ".join(f"{k} = :{k}" for k in update_data.keys())
        sql = f"UPDATE processing_jobs SET {set_clauses} WHERE id = :id"
        
        params = update_data.copy()
        params["id"] = job_id
        
        self.db_container.execute_sql(sql, params)
    
    def get_workflow_summary(self) -> Dict:
        """Get summary of workflow execution."""
        summary = {
            "companies_processed": self.workflow_state.get("companies_count", 0),
            "filings_discovered": len(self.workflow_state.get("discovered_filings", [])),
            "documents_processed": len(self.workflow_state.get("processed_documents", [])),
            "processing_jobs_created": len(self.workflow_state.get("processing_jobs", [])),
            "etl_run_id": self.workflow_state.get("current_etl_run"),
            "etl_run_status": self.workflow_state.get("etl_run_status", "unknown")
        }
        
        # Get database counts for validation
        companies_count = self.db_container.execute_sql("SELECT COUNT(*) FROM companies").fetchone()[0]
        filings_count = self.db_container.execute_sql("SELECT COUNT(*) FROM filings").fetchone()[0]
        documents_count = self.db_container.execute_sql("SELECT COUNT(*) FROM documents").fetchone()[0]
        jobs_count = self.db_container.execute_sql("SELECT COUNT(*) FROM processing_jobs").fetchone()[0]
        
        summary["database_counts"] = {
            "companies": companies_count,
            "filings": filings_count,
            "documents": documents_count,
            "processing_jobs": jobs_count
        }
        
        return summary


class FullStackETLPipeline:
    """Full-stack ETL pipeline for end-to-end testing."""
    
    def __init__(self, orchestrator: E2EWorkflowOrchestrator, edgar_api: MockEdgarAPI):
        self.orchestrator = orchestrator
        self.edgar_api = edgar_api
        self.processed_state = set()
    
    async def run_complete_workflow(self, companies: List[Dict], form_types: List[str]) -> Dict:
        """Run complete ETL workflow from start to finish."""
        workflow_start = time.time()
        
        # Phase 1: Initialize
        run_config = {
            "run_type": "e2e_test",
            "status": "running",
            "target_ciks": [c["cik"] for c in companies],
            "form_types": form_types,
            "started_at": datetime.now()
        }
        
        etl_run_id = self.orchestrator.create_etl_run(run_config)
        
        try:
            # Phase 2: Company Discovery and Setup
            self.orchestrator.initialize_companies(companies)
            
            discovery_job_id = self.orchestrator.create_processing_job(
                "filing_discovery", target_cik=",".join(c["cik"] for c in companies)
            )
            self.orchestrator.update_processing_job(discovery_job_id, "running")
            
            # Phase 3: Filing Discovery
            all_discovered_filings = []
            for company in companies:
                cik = company["cik"]
                
                # Simulate discovery
                if cik in self.edgar_api.submissions_data:
                    submissions = self.edgar_api.submissions_data[cik]
                    recent = submissions.get("filings", {}).get("recent", {})
                    
                    forms = recent.get("form", [])
                    accessions = recent.get("accessionNumber", [])
                    filing_dates = recent.get("filingDate", [])
                    
                    discovered = []
                    for i, (form, accession) in enumerate(zip(forms, accessions)):
                        if form in form_types:
                            filing_data = {
                                "form": form,
                                "accession": accession,
                                "filing_date": filing_dates[i] if i < len(filing_dates) else None
                            }
                            discovered.append(filing_data)
                    
                    filing_ids = self.orchestrator.record_filing_discovery(cik, discovered)
                    all_discovered_filings.extend(discovered)
            
            self.orchestrator.update_processing_job(discovery_job_id, "completed")
            
            # Phase 4: Filing Download and Processing
            processed_filings = 0
            processed_documents = 0
            
            for company in companies:
                cik = company["cik"]
                company_filings = [f for f in all_discovered_filings 
                                 if any(acc.startswith(cik) for acc in [f["accession"]])]
                
                for filing in company_filings:
                    accession = filing["accession"]
                    
                    # Create processing job for this filing
                    processing_job_id = self.orchestrator.create_processing_job(
                        "filing_download", target_cik=cik
                    )
                    self.orchestrator.update_processing_job(processing_job_id, "running")
                    
                    try:
                        # Simulate downloading filing
                        await asyncio.sleep(0.01)  # Simulate processing time
                        
                        # Get filing ID from database
                        filing_result = self.orchestrator.db_container.execute_sql(
                            "SELECT id FROM filings WHERE accession_number = :accession",
                            {"accession": accession}
                        ).fetchone()
                        
                        if filing_result:
                            filing_id = filing_result[0]
                            
                            # Simulate document processing
                            index_key = f"{cik}_{accession}"
                            if index_key in self.edgar_api.filing_index_data:
                                # Mock documents for this filing
                                documents = [
                                    {
                                        "filename": f"doc_{i}.htm",
                                        "size": 1000 + i * 500,
                                        "s3_key": f"filings/{cik}/{accession}/doc_{i}.htm"
                                    }
                                    for i in range(3)
                                ]
                                
                                # Upload to S3 (simulate)
                                for doc in documents:
                                    content = f"Document content for {doc['filename']}".encode()
                                    self.orchestrator.s3_client.put_object(
                                        Bucket=self.orchestrator.bucket_name,
                                        Key=doc["s3_key"],
                                        Body=content
                                    )
                                
                                self.orchestrator.record_filing_processing(filing_id, documents, True)
                                processed_documents += len(documents)
                                processed_filings += 1
                                
                                self.orchestrator.update_processing_job(processing_job_id, "completed")
                            else:
                                self.orchestrator.record_filing_processing(filing_id, [], False)
                                self.orchestrator.update_processing_job(
                                    processing_job_id, "failed", "Filing index not found"
                                )
                        else:
                            self.orchestrator.update_processing_job(
                                processing_job_id, "failed", "Filing record not found in database"
                            )
                    
                    except Exception as e:
                        self.orchestrator.update_processing_job(
                            processing_job_id, "failed", str(e)
                        )
            
            # Phase 5: Finalization
            workflow_end = time.time()
            
            final_metrics = {
                "completed_at": datetime.now(),
                "companies_processed": len(companies),
                "filings_discovered": len(all_discovered_filings),
                "filings_downloaded": processed_filings,
                "documents_processed": processed_documents,
                "performance_metrics": json.dumps({
                    "total_duration_seconds": workflow_end - workflow_start,
                    "avg_filing_processing_time": (workflow_end - workflow_start) / max(processed_filings, 1)
                })
            }
            
            self.orchestrator.update_etl_run_status(etl_run_id, "completed", final_metrics)
            
            return {
                "success": True,
                "etl_run_id": etl_run_id,
                "duration": workflow_end - workflow_start,
                **final_metrics
            }
            
        except Exception as e:
            # Handle workflow failure
            self.orchestrator.update_etl_run_status(
                etl_run_id, "failed", {"error_summary": str(e)}
            )
            
            return {
                "success": False,
                "etl_run_id": etl_run_id,
                "error": str(e)
            }


@pytest.fixture
def e2e_test_database():
    """Provide database for E2E testing."""
    container = DatabaseTestContainer(use_real_postgres=False)
    with container.get_test_database():
        yield container


@pytest.fixture
def e2e_s3_environment():
    """Provide S3 environment for E2E testing."""
    with mock_s3():
        s3_client = boto3.client("s3", region_name="us-east-1")
        bucket_name = "test-edgar-e2e"
        s3_client.create_bucket(Bucket=bucket_name)
        
        yield s3_client, bucket_name


@pytest.fixture
def e2e_orchestrator(e2e_test_database, e2e_s3_environment):
    """Provide E2E workflow orchestrator."""
    s3_client, bucket_name = e2e_s3_environment
    return E2EWorkflowOrchestrator(e2e_test_database, s3_client, bucket_name)


@pytest.fixture
def e2e_edgar_api():
    """Provide EDGAR API with E2E test data."""
    api = MockEdgarAPI()
    
    # Create comprehensive test dataset
    companies = [
        {"cik": "0000320193", "name": "Apple Inc."},
        {"cik": "0000789019", "name": "Microsoft Corp"},
        {"cik": "0001652044", "name": "Alphabet Inc."},
    ]
    
    for company in companies:
        cik = company["cik"]
        
        # Add submissions data
        submissions = {
            "cik": int(cik),
            "name": company["name"],
            "filings": {
                "recent": {
                    "form": ["10-K", "10-Q", "8-K", "DEF 14A"],
                    "accessionNumber": [
                        f"{cik}-23-000001",
                        f"{cik}-23-000002", 
                        f"{cik}-23-000003",
                        f"{cik}-23-000004"
                    ],
                    "filingDate": ["2023-11-01", "2023-08-01", "2023-07-15", "2023-04-15"],
                    "primaryDocument": [
                        f"{cik.lower()}-10k.htm",
                        f"{cik.lower()}-10q.htm",
                        f"{cik.lower()}-8k.htm",
                        f"{cik.lower()}-def14a.htm"
                    ]
                }
            }
        }
        api.add_company_submissions(cik, submissions)
        
        # Add filing indexes and documents
        for i, accession in enumerate(submissions["filings"]["recent"]["accessionNumber"]):
            form_type = submissions["filings"]["recent"]["form"][i]
            
            filing_index = f"""
            <html>
            <table class="tableFile">
            <tr><td>1</td><td>primary-{i}.htm</td><td>{form_type}</td><td>Primary Document</td></tr>
            <tr><td>2</td><td>exhibit-{i}-1.htm</td><td>EX-21.1</td><td>Exhibit 21.1</td></tr>
            <tr><td>3</td><td>exhibit-{i}-2.htm</td><td>EX-23.1</td><td>Exhibit 23.1</td></tr>
            </table>
            </html>
            """
            api.add_filing_index(cik, accession, filing_index)
            
            # Add document content
            for doc_idx in range(3):
                filename = f"primary-{i}.htm" if doc_idx == 0 else f"exhibit-{i}-{doc_idx}.htm"
                content = f"Document content for {company['name']} {form_type} {filename}".encode()
                api.add_document(cik, accession, filename, content)
    
    return api


@pytest.mark.e2e
class TestCompleteWorkflow:
    """Test complete end-to-end workflows."""
    
    @pytest.mark.asyncio
    async def test_single_company_complete_workflow(self, e2e_orchestrator, e2e_edgar_api):
        """Test complete workflow for single company."""
        pipeline = FullStackETLPipeline(e2e_orchestrator, e2e_edgar_api)
        
        companies = [CompanyFactory(cik="0000320193", ticker="AAPL", name="Apple Inc.")]
        form_types = ["10-K", "10-Q"]
        
        result = await pipeline.run_complete_workflow(companies, form_types)
        
        assert result["success"] is True
        assert result["companies_processed"] == 1
        assert result["filings_discovered"] >= 2  # Should find 10-K and 10-Q
        assert result["documents_processed"] > 0
        
        # Verify database state
        summary = e2e_orchestrator.get_workflow_summary()
        assert summary["database_counts"]["companies"] == 1
        assert summary["database_counts"]["filings"] >= 2
        assert summary["database_counts"]["documents"] > 0
        assert summary["database_counts"]["processing_jobs"] >= 2
        
        print(f"Single company workflow: {summary}")
    
    @pytest.mark.asyncio
    async def test_multi_company_workflow(self, e2e_orchestrator, e2e_edgar_api):
        """Test workflow with multiple companies."""
        pipeline = FullStackETLPipeline(e2e_orchestrator, e2e_edgar_api)
        
        companies = [
            CompanyFactory(cik="0000320193", ticker="AAPL", name="Apple Inc."),
            CompanyFactory(cik="0000789019", ticker="MSFT", name="Microsoft Corp"),
            CompanyFactory(cik="0001652044", ticker="GOOGL", name="Alphabet Inc.")
        ]
        form_types = ["10-K", "10-Q", "8-K"]
        
        result = await pipeline.run_complete_workflow(companies, form_types)
        
        assert result["success"] is True
        assert result["companies_processed"] == 3
        assert result["filings_discovered"] >= 6  # 2+ filings per company
        assert result["documents_processed"] > 0
        
        # Verify all companies processed
        summary = e2e_orchestrator.get_workflow_summary()
        assert summary["database_counts"]["companies"] == 3
        
        # Check that filings exist for each company
        for company in companies:
            filings_for_company = e2e_orchestrator.db_container.execute_sql(
                "SELECT COUNT(*) FROM filings WHERE cik = :cik",
                {"cik": company["cik"]}
            ).fetchone()[0]
            assert filings_for_company > 0, f"No filings found for company {company['cik']}"
        
        print(f"Multi-company workflow: {summary}")
    
    @pytest.mark.asyncio
    async def test_workflow_with_form_filtering(self, e2e_orchestrator, e2e_edgar_api):
        """Test workflow with specific form type filtering."""
        pipeline = FullStackETLPipeline(e2e_orchestrator, e2e_edgar_api)
        
        companies = [CompanyFactory(cik="0000320193", ticker="AAPL", name="Apple Inc.")]
        form_types = ["10-K"]  # Only 10-K forms
        
        result = await pipeline.run_complete_workflow(companies, form_types)
        
        assert result["success"] is True
        
        # Verify only 10-K forms were processed
        processed_forms = e2e_orchestrator.db_container.execute_sql(
            "SELECT DISTINCT form_type FROM filings"
        ).fetchall()
        
        form_types_found = [row[0] for row in processed_forms]
        assert "10-K" in form_types_found
        # Should not have other forms if filtering worked correctly
        for form_type in form_types_found:
            assert form_type in form_types, f"Unexpected form type {form_type} found"
    
    @pytest.mark.asyncio
    async def test_workflow_error_recovery(self, e2e_orchestrator, e2e_edgar_api):
        """Test workflow error handling and recovery."""
        pipeline = FullStackETLPipeline(e2e_orchestrator, e2e_edgar_api)
        
        # Include a company that will cause errors
        companies = [
            CompanyFactory(cik="0000320193", ticker="AAPL", name="Apple Inc."),  # Good
            CompanyFactory(cik="0000999999", ticker="FAIL", name="Failing Corp")  # Bad - no data
        ]
        form_types = ["10-K", "10-Q"]
        
        result = await pipeline.run_complete_workflow(companies, form_types)
        
        # Workflow should partially succeed
        assert result["companies_processed"] == 2  # Both companies attempted
        
        # Check that some processing succeeded despite errors
        summary = e2e_orchestrator.get_workflow_summary()
        assert summary["database_counts"]["companies"] == 2
        assert summary["database_counts"]["filings"] > 0  # Some filings should succeed
        
        # Check for failed processing jobs
        failed_jobs = e2e_orchestrator.db_container.execute_sql(
            "SELECT COUNT(*) FROM processing_jobs WHERE status = 'failed'"
        ).fetchone()[0]
        
        assert failed_jobs > 0, "Expected some processing jobs to fail"
        
        print(f"Error recovery workflow - Failed jobs: {failed_jobs}")


@pytest.mark.e2e
class TestDataIntegrity:
    """Test data integrity throughout the workflow."""
    
    @pytest.mark.asyncio
    async def test_referential_integrity(self, e2e_orchestrator, e2e_edgar_api):
        """Test referential integrity between entities."""
        pipeline = FullStackETLPipeline(e2e_orchestrator, e2e_edgar_api)
        
        companies = [CompanyFactory(cik="0000320193", ticker="AAPL", name="Apple Inc.")]
        form_types = ["10-K", "10-Q"]
        
        await pipeline.run_complete_workflow(companies, form_types)
        
        # Verify referential integrity
        
        # 1. All filings should reference existing companies
        orphaned_filings = e2e_orchestrator.db_container.execute_sql("""
            SELECT COUNT(*) FROM filings f
            LEFT JOIN companies c ON f.cik = c.cik
            WHERE c.cik IS NULL
        """).fetchone()[0]
        assert orphaned_filings == 0, "Found filings without corresponding companies"
        
        # 2. All documents should reference existing filings
        orphaned_documents = e2e_orchestrator.db_container.execute_sql("""
            SELECT COUNT(*) FROM documents d
            LEFT JOIN filings f ON d.filing_id = f.id
            WHERE f.id IS NULL
        """).fetchone()[0]
        assert orphaned_documents == 0, "Found documents without corresponding filings"
        
        # 3. Processing jobs should have valid targets
        invalid_job_targets = e2e_orchestrator.db_container.execute_sql("""
            SELECT COUNT(*) FROM processing_jobs pj
            WHERE pj.target_cik IS NOT NULL 
            AND pj.target_cik NOT IN (SELECT cik FROM companies)
        """).fetchone()[0]
        assert invalid_job_targets == 0, "Found processing jobs with invalid CIK targets"
    
    @pytest.mark.asyncio
    async def test_data_consistency(self, e2e_orchestrator, e2e_edgar_api):
        """Test data consistency across the workflow."""
        pipeline = FullStackETLPipeline(e2e_orchestrator, e2e_edgar_api)
        
        companies = [
            CompanyFactory(cik="0000320193", ticker="AAPL", name="Apple Inc."),
            CompanyFactory(cik="0000789019", ticker="MSFT", name="Microsoft Corp")
        ]
        form_types = ["10-K"]
        
        await pipeline.run_complete_workflow(companies, form_types)
        
        # Check data consistency
        
        # 1. Filing accession numbers should be unique
        duplicate_accessions = e2e_orchestrator.db_container.execute_sql("""
            SELECT accession_number, COUNT(*) 
            FROM filings 
            GROUP BY accession_number 
            HAVING COUNT(*) > 1
        """).fetchall()
        assert len(duplicate_accessions) == 0, f"Found duplicate accession numbers: {duplicate_accessions}"
        
        # 2. Each filing should have at least one document
        filings_without_docs = e2e_orchestrator.db_container.execute_sql("""
            SELECT f.id, f.accession_number
            FROM filings f
            LEFT JOIN documents d ON f.id = d.filing_id
            WHERE f.processing_status = 'completed'
            AND d.id IS NULL
        """).fetchall()
        assert len(filings_without_docs) == 0, f"Found completed filings without documents: {filings_without_docs}"
        
        # 3. Document sequences should be consistent
        invalid_sequences = e2e_orchestrator.db_container.execute_sql("""
            SELECT filing_id, COUNT(*) as doc_count, MAX(sequence) as max_seq
            FROM documents
            GROUP BY filing_id
            HAVING COUNT(*) != MAX(sequence)
        """).fetchall()
        assert len(invalid_sequences) == 0, f"Found inconsistent document sequences: {invalid_sequences}"
    
    @pytest.mark.asyncio
    async def test_s3_data_consistency(self, e2e_orchestrator, e2e_edgar_api):
        """Test consistency between database and S3 storage."""
        pipeline = FullStackETLPipeline(e2e_orchestrator, e2e_edgar_api)
        
        companies = [CompanyFactory(cik="0000320193", ticker="AAPL", name="Apple Inc.")]
        form_types = ["10-K"]
        
        await pipeline.run_complete_workflow(companies, form_types)
        
        # Get all documents from database
        documents = e2e_orchestrator.db_container.execute_sql("""
            SELECT id, s3_key, filename, size_bytes
            FROM documents
            WHERE s3_key IS NOT NULL
        """).fetchall()
        
        # Verify each document exists in S3
        for doc_id, s3_key, filename, size_bytes in documents:
            try:
                s3_object = e2e_orchestrator.s3_client.head_object(
                    Bucket=e2e_orchestrator.bucket_name,
                    Key=s3_key
                )
                
                # Verify S3 object exists
                assert s3_object["ContentLength"] > 0, f"S3 object {s3_key} is empty"
                
                # Note: Size comparison might differ due to encoding, but should be reasonable
                size_ratio = s3_object["ContentLength"] / max(size_bytes, 1)
                assert 0.5 <= size_ratio <= 2.0, f"S3 size mismatch for {s3_key}: DB={size_bytes}, S3={s3_object['ContentLength']}"
                
            except Exception as e:
                pytest.fail(f"S3 object {s3_key} not found or inaccessible: {e}")


@pytest.mark.e2e
class TestWorkflowMonitoring:
    """Test workflow monitoring and observability."""
    
    @pytest.mark.asyncio
    async def test_etl_run_tracking(self, e2e_orchestrator, e2e_edgar_api):
        """Test ETL run creation and tracking."""
        pipeline = FullStackETLPipeline(e2e_orchestrator, e2e_edgar_api)
        
        companies = [CompanyFactory(cik="0000320193", ticker="AAPL", name="Apple Inc.")]
        form_types = ["10-K"]
        
        result = await pipeline.run_complete_workflow(companies, form_types)
        
        # Verify ETL run was created and tracked
        etl_run_id = result["etl_run_id"]
        assert etl_run_id is not None
        
        # Check ETL run record
        etl_run = e2e_orchestrator.db_container.execute_sql(
            "SELECT * FROM etl_runs WHERE id = :id",
            {"id": etl_run_id}
        ).fetchone()
        
        assert etl_run is not None
        assert etl_run[2] == "completed"  # status
        assert etl_run[6] is not None  # started_at
        assert etl_run[7] is not None  # completed_at
        
        # Check performance metrics were recorded
        performance_metrics = json.loads(etl_run[12]) if etl_run[12] else {}
        assert "total_duration_seconds" in performance_metrics
        assert performance_metrics["total_duration_seconds"] > 0
    
    @pytest.mark.asyncio
    async def test_processing_job_lifecycle(self, e2e_orchestrator, e2e_edgar_api):
        """Test processing job lifecycle tracking."""
        pipeline = FullStackETLPipeline(e2e_orchestrator, e2e_edgar_api)
        
        companies = [CompanyFactory(cik="0000320193", ticker="AAPL", name="Apple Inc.")]
        form_types = ["10-K"]
        
        await pipeline.run_complete_workflow(companies, form_types)
        
        # Check processing jobs were created and tracked
        jobs = e2e_orchestrator.db_container.execute_sql("""
            SELECT job_type, status, started_at, completed_at, error_message
            FROM processing_jobs
            ORDER BY created_at
        """).fetchall()
        
        assert len(jobs) > 0, "No processing jobs were created"
        
        # Verify job lifecycle
        for job_type, status, started_at, completed_at, error_message in jobs:
            assert status in ["pending", "running", "completed", "failed"], f"Invalid job status: {status}"
            
            if status in ["completed", "failed"]:
                assert started_at is not None, f"Job {job_type} completed but no start time recorded"
                assert completed_at is not None, f"Job {job_type} completed but no completion time recorded"
                assert completed_at >= started_at, f"Job {job_type} completion before start time"
    
    @pytest.mark.asyncio
    async def test_error_tracking(self, e2e_orchestrator, e2e_edgar_api):
        """Test error tracking throughout workflow."""
        pipeline = FullStackETLPipeline(e2e_orchestrator, e2e_edgar_api)
        
        # Include problematic company to trigger errors
        companies = [
            CompanyFactory(cik="0000320193", ticker="AAPL", name="Apple Inc."),
            CompanyFactory(cik="0000999999", ticker="ERROR", name="Error Corp")
        ]
        form_types = ["10-K"]
        
        await pipeline.run_complete_workflow(companies, form_types)
        
        # Check for error recording
        failed_jobs = e2e_orchestrator.db_container.execute_sql("""
            SELECT job_type, error_message
            FROM processing_jobs
            WHERE status = 'failed'
            AND error_message IS NOT NULL
        """).fetchall()
        
        if len(failed_jobs) > 0:
            # Verify error messages are meaningful
            for job_type, error_message in failed_jobs:
                assert error_message.strip() != "", f"Empty error message for failed job {job_type}"
                assert len(error_message) > 10, f"Error message too short for job {job_type}: {error_message}"


@pytest.mark.e2e
@pytest.mark.slow
class TestFullScaleWorkflow:
    """Test full-scale workflow scenarios."""
    
    @pytest.mark.asyncio
    async def test_large_batch_processing(self, e2e_orchestrator, e2e_edgar_api):
        """Test processing larger batches of companies."""
        # Extend EDGAR API with more companies
        for i in range(10, 20):  # Add 10 more companies
            cik = f"000032{i:04d}"
            submissions = {
                "cik": int(cik),
                "name": f"Test Company {i}",
                "filings": {
                    "recent": {
                        "form": ["10-K", "10-Q"],
                        "accessionNumber": [f"{cik}-23-000001", f"{cik}-23-000002"],
                        "filingDate": ["2023-11-01", "2023-08-01"],
                        "primaryDocument": [f"test-{i}-10k.htm", f"test-{i}-10q.htm"]
                    }
                }
            }
            e2e_edgar_api.add_company_submissions(cik, submissions)
            
            # Add minimal filing data
            for j, accession in enumerate(submissions["filings"]["recent"]["accessionNumber"]):
                filing_index = f"<html><table class='tableFile'><tr><td>test-{i}-{j}.htm</td></tr></table></html>"
                e2e_edgar_api.add_filing_index(cik, accession, filing_index)
                e2e_edgar_api.add_document(cik, accession, f"test-{i}-{j}.htm", b"Test content")
        
        pipeline = FullStackETLPipeline(e2e_orchestrator, e2e_edgar_api)
        
        # Create 10 companies
        companies = [
            CompanyFactory(cik=f"000032{i:04d}", ticker=f"TEST{i}", name=f"Test Company {i}")
            for i in range(10, 20)
        ]
        form_types = ["10-K", "10-Q"]
        
        start_time = time.time()
        result = await pipeline.run_complete_workflow(companies, form_types)
        end_time = time.time()
        
        assert result["success"] is True
        assert result["companies_processed"] == 10
        
        # Performance should be reasonable even for larger batches
        total_time = end_time - start_time
        assert total_time < 30, f"Large batch took too long: {total_time:.2f}s"
        
        # Verify data integrity at scale
        summary = e2e_orchestrator.get_workflow_summary()
        assert summary["database_counts"]["companies"] == 10
        assert summary["database_counts"]["filings"] >= 10  # At least one filing per company
        
        print(f"Large batch processing: {summary}")
        print(f"Processing time: {total_time:.2f}s")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "e2e"])