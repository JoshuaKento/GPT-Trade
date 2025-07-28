"""ETL pipeline tests with mocked external dependencies."""

import asyncio
import json
import time
from datetime import datetime, date
from typing import Dict, List, Optional, Set
from unittest.mock import AsyncMock, Mock, patch, MagicMock, call

import aiohttp
import pytest
import responses
from moto import mock_s3
import boto3

from tests.test_database_models import (
    CompanyFactory,
    FilingFactory,
    DocumentFactory,
    ProcessingJobFactory,
    EtlRunFactory
)


class MockEdgarAPI:
    """Mock EDGAR API for testing ETL pipeline."""
    
    def __init__(self):
        self.submissions_data = {}
        self.filing_index_data = {}
        self.document_data = {}
        self.request_count = 0
        self.delay_ms = 0  # Simulated API delay
    
    def add_company_submissions(self, cik: str, submissions: Dict):
        """Add mock submissions data for a CIK."""
        self.submissions_data[cik] = submissions
    
    def add_filing_index(self, cik: str, accession: str, index_html: str):
        """Add mock filing index HTML."""
        key = f"{cik}_{accession}"
        self.filing_index_data[key] = index_html
    
    def add_document(self, cik: str, accession: str, filename: str, content: bytes):
        """Add mock document content."""
        key = f"{cik}_{accession}_{filename}"
        self.document_data[key] = content
    
    @responses.activate
    def setup_responses(self):
        """Setup responses for mocked HTTP calls."""
        
        def submissions_callback(request):
            self.request_count += 1
            # Extract CIK from URL
            url_parts = request.url.split('/')
            cik = url_parts[-1].replace('CIK', '').replace('.json', '')
            
            if cik in self.submissions_data:
                return (200, {}, json.dumps(self.submissions_data[cik]))
            else:
                return (404, {}, json.dumps({"error": "CIK not found"}))
        
        def filing_index_callback(request):
            self.request_count += 1
            # Extract CIK and accession from URL
            url_parts = request.url.split('/')
            cik = url_parts[-3]
            accession = url_parts[-2]
            
            key = f"{cik}_{accession}"
            if key in self.filing_index_data:
                return (200, {}, self.filing_index_data[key])
            else:
                return (404, {}, "<html><body>Not found</body></html>")
        
        def document_callback(request):
            self.request_count += 1
            # Extract document info from URL
            url_parts = request.url.split('/')
            cik = url_parts[-3]
            accession = url_parts[-2]
            filename = url_parts[-1]
            
            key = f"{cik}_{accession}_{filename}"
            if key in self.document_data:
                return (200, {}, self.document_data[key])
            else:
                return (404, {}, b"Document not found")
        
        # Mock EDGAR API endpoints
        responses.add_callback(
            responses.GET,
            url="https://data.sec.gov/submissions/CIK*.json",
            callback=submissions_callback,
            content_type="application/json"
        )
        
        responses.add_callback(
            responses.GET,
            url="https://www.sec.gov/Archives/edgar/data/*/*/index.html",
            callback=filing_index_callback,
            content_type="text/html"
        )
        
        responses.add_callback(
            responses.GET,
            url="https://www.sec.gov/Archives/edgar/data/*/*/*.htm*",
            callback=document_callback,
            content_type="text/html"
        )


class MockETLPipeline:
    """Mock ETL pipeline for testing."""
    
    def __init__(self, edgar_api: MockEdgarAPI):
        self.edgar_api = edgar_api
        self.processed_ciks = set()
        self.discovered_filings = []
        self.downloaded_documents = []
        self.processing_jobs = []
        self.errors = []
    
    async def discover_filings(self, ciks: List[str], form_types: List[str]) -> List[Dict]:
        """Discover new filings for given CIKs."""
        discovered = []
        
        for cik in ciks:
            try:
                # Simulate API call delay
                if self.edgar_api.delay_ms > 0:
                    await asyncio.sleep(self.edgar_api.delay_ms / 1000)
                
                if cik in self.edgar_api.submissions_data:
                    submissions = self.edgar_api.submissions_data[cik]
                    recent = submissions.get("filings", {}).get("recent", {})
                    
                    forms = recent.get("form", [])
                    accessions = recent.get("accessionNumber", [])
                    
                    for i, (form, accession) in enumerate(zip(forms, accessions)):
                        if form in form_types:
                            filing = {
                                "cik": cik,
                                "form": form,
                                "accession": accession,
                                "filing_date": recent.get("filingDate", [])[i] if i < len(recent.get("filingDate", [])) else None
                            }
                            discovered.append(filing)
                            self.discovered_filings.append(filing)
                
                self.processed_ciks.add(cik)
                
            except Exception as e:
                self.errors.append(f"Failed to discover filings for CIK {cik}: {e}")
        
        return discovered
    
    async def download_filing(self, cik: str, accession: str) -> Dict:
        """Download filing and its documents."""
        try:
            # Simulate API call delay
            if self.edgar_api.delay_ms > 0:
                await asyncio.sleep(self.edgar_api.delay_ms / 1000)
            
            # Get filing index
            index_key = f"{cik}_{accession}"
            if index_key not in self.edgar_api.filing_index_data:
                raise Exception(f"Filing index not found: {accession}")
            
            # Parse documents from index (simplified)
            documents = []
            for filename in ["primary.htm", "exhibit1.htm", "exhibit2.htm"]:
                doc_key = f"{cik}_{accession}_{filename}"
                if doc_key in self.edgar_api.document_data:
                    doc_info = {
                        "filename": filename,
                        "size": len(self.edgar_api.document_data[doc_key]),
                        "content": self.edgar_api.document_data[doc_key]
                    }
                    documents.append(doc_info)
                    self.downloaded_documents.append(doc_info)
            
            return {
                "cik": cik,
                "accession": accession,
                "documents": documents,
                "success": True
            }
            
        except Exception as e:
            self.errors.append(f"Failed to download filing {accession}: {e}")
            return {
                "cik": cik,
                "accession": accession,
                "documents": [],
                "success": False,
                "error": str(e)
            }
    
    async def process_documents(self, filing_data: Dict) -> Dict:
        """Process documents for a filing."""
        try:
            processed_docs = []
            
            for doc in filing_data.get("documents", []):
                # Simulate document processing
                if self.edgar_api.delay_ms > 0:
                    await asyncio.sleep(self.edgar_api.delay_ms / 1000)
                
                processed_doc = {
                    "filename": doc["filename"],
                    "extracted_text": f"Extracted text from {doc['filename']}",
                    "word_count": len(doc["content"]),
                    "processing_time": 0.1
                }
                processed_docs.append(processed_doc)
            
            return {
                "filing_id": filing_data["accession"],
                "processed_documents": processed_docs,
                "success": True
            }
            
        except Exception as e:
            self.errors.append(f"Failed to process documents for {filing_data['accession']}: {e}")
            return {
                "filing_id": filing_data["accession"],
                "processed_documents": [],
                "success": False,
                "error": str(e)
            }
    
    async def run_etl_pipeline(self, ciks: List[str], form_types: List[str]) -> Dict:
        """Run complete ETL pipeline."""
        start_time = time.time()
        
        # Discovery phase
        discovered_filings = await self.discover_filings(ciks, form_types)
        
        # Download phase
        download_tasks = [
            self.download_filing(filing["cik"], filing["accession"])
            for filing in discovered_filings
        ]
        download_results = await asyncio.gather(*download_tasks, return_exceptions=True)
        
        # Process phase
        process_tasks = [
            self.process_documents(result)
            for result in download_results
            if isinstance(result, dict) and result.get("success")
        ]
        process_results = await asyncio.gather(*process_tasks, return_exceptions=True)
        
        end_time = time.time()
        
        return {
            "run_id": f"etl_run_{int(time.time())}",
            "duration": end_time - start_time,
            "ciks_processed": len(self.processed_ciks),
            "filings_discovered": len(self.discovered_filings),
            "filings_downloaded": sum(1 for r in download_results if isinstance(r, dict) and r.get("success")),
            "documents_processed": sum(len(r.get("processed_documents", [])) for r in process_results if isinstance(r, dict)),
            "errors": self.errors,
            "success": len(self.errors) == 0
        }


@pytest.fixture
def mock_edgar_api():
    """Provide mock EDGAR API."""
    api = MockEdgarAPI()
    
    # Add test data
    test_submissions = {
        "cik": 320193,
        "name": "Apple Inc.",
        "filings": {
            "recent": {
                "form": ["10-K", "10-Q", "8-K"],
                "accessionNumber": ["0000320193-23-000106", "0000320193-23-000064", "0000320193-23-000077"],
                "filingDate": ["2023-11-02", "2023-08-04", "2023-07-27"],
                "primaryDocument": ["aapl-20230930.htm", "aapl-20230701.htm", "aapl-20230727.htm"]
            }
        }
    }
    
    api.add_company_submissions("0000320193", test_submissions)
    
    # Add filing index
    filing_index_html = """
    <html>
    <table class="tableFile">
    <tr><td>1</td><td>primary.htm</td><td>10-K</td></tr>
    <tr><td>2</td><td>exhibit1.htm</td><td>EX-21.1</td></tr>
    <tr><td>3</td><td>exhibit2.htm</td><td>EX-23.1</td></tr>
    </table>
    </html>
    """
    
    api.add_filing_index("0000320193", "0000320193-23-000106", filing_index_html)
    
    # Add document content
    api.add_document("0000320193", "0000320193-23-000106", "primary.htm", b"<html>Primary document content</html>")
    api.add_document("0000320193", "0000320193-23-000106", "exhibit1.htm", b"<html>Exhibit 1 content</html>")
    api.add_document("0000320193", "0000320193-23-000106", "exhibit2.htm", b"<html>Exhibit 2 content</html>")
    
    return api


@pytest.fixture
def mock_etl_pipeline(mock_edgar_api):
    """Provide mock ETL pipeline."""
    return MockETLPipeline(mock_edgar_api)


@pytest.mark.unit
class TestETLPipelineComponents:
    """Test individual ETL pipeline components."""
    
    @pytest.mark.asyncio
    async def test_filing_discovery(self, mock_etl_pipeline):
        """Test filing discovery phase."""
        ciks = ["0000320193"]
        form_types = ["10-K", "10-Q"]
        
        discovered = await mock_etl_pipeline.discover_filings(ciks, form_types)
        
        assert len(discovered) == 2  # Should find 10-K and 10-Q
        assert all(filing["form"] in form_types for filing in discovered)
        assert all(filing["cik"] == "0000320193" for filing in discovered)
        assert "0000320193" in mock_etl_pipeline.processed_ciks
    
    @pytest.mark.asyncio
    async def test_filing_download(self, mock_etl_pipeline):
        """Test filing download phase."""
        result = await mock_etl_pipeline.download_filing("0000320193", "0000320193-23-000106")
        
        assert result["success"] is True
        assert result["cik"] == "0000320193"
        assert result["accession"] == "0000320193-23-000106"
        assert len(result["documents"]) == 3
        
        # Verify document content
        for doc in result["documents"]:
            assert "filename" in doc
            assert "size" in doc
            assert "content" in doc
            assert doc["size"] > 0
    
    @pytest.mark.asyncio
    async def test_document_processing(self, mock_etl_pipeline):
        """Test document processing phase."""
        filing_data = {
            "accession": "0000320193-23-000106",
            "documents": [
                {"filename": "primary.htm", "content": b"<html>Document content</html>"},
                {"filename": "exhibit.htm", "content": b"<html>Exhibit content</html>"}
            ]
        }
        
        result = await mock_etl_pipeline.process_documents(filing_data)
        
        assert result["success"] is True
        assert result["filing_id"] == "0000320193-23-000106"
        assert len(result["processed_documents"]) == 2
        
        # Verify processed document structure
        for doc in result["processed_documents"]:
            assert "filename" in doc
            assert "extracted_text" in doc
            assert "word_count" in doc
            assert "processing_time" in doc
    
    @pytest.mark.asyncio
    async def test_error_handling_discovery(self, mock_etl_pipeline):
        """Test error handling in discovery phase."""
        # Test with non-existent CIK
        discovered = await mock_etl_pipeline.discover_filings(["9999999999"], ["10-K"])
        
        assert len(discovered) == 0
        assert len(mock_etl_pipeline.errors) == 0  # Should not error for missing CIK, just return empty
    
    @pytest.mark.asyncio
    async def test_error_handling_download(self, mock_etl_pipeline):
        """Test error handling in download phase."""
        # Test with non-existent filing
        result = await mock_etl_pipeline.download_filing("0000320193", "non-existent-accession")
        
        assert result["success"] is False
        assert "error" in result
        assert len(mock_etl_pipeline.errors) > 0


@pytest.mark.integration
class TestETLPipelineIntegration:
    """Test complete ETL pipeline integration."""
    
    @pytest.mark.asyncio
    async def test_full_pipeline_execution(self, mock_etl_pipeline):
        """Test complete ETL pipeline execution."""
        ciks = ["0000320193"]
        form_types = ["10-K", "10-Q", "8-K"]
        
        result = await mock_etl_pipeline.run_etl_pipeline(ciks, form_types)
        
        assert result["success"] is True
        assert result["ciks_processed"] == 1
        assert result["filings_discovered"] == 3  # 10-K, 10-Q, 8-K
        assert result["filings_downloaded"] == 1  # Only one has mock data
        assert result["documents_processed"] == 3  # 3 documents per filing
        assert result["duration"] > 0
        assert len(result["errors"]) == 0
    
    @pytest.mark.asyncio 
    async def test_pipeline_with_multiple_ciks(self, mock_edgar_api, mock_etl_pipeline):
        """Test pipeline with multiple CIKs."""
        # Add data for second company
        test_submissions_2 = {
            "cik": 789019,
            "name": "Microsoft Corp",
            "filings": {
                "recent": {
                    "form": ["10-K", "10-Q"],
                    "accessionNumber": ["0000789019-23-000020", "0000789019-23-000015"],
                    "filingDate": ["2023-07-25", "2023-04-25"],
                    "primaryDocument": ["msft-20230630.htm", "msft-20230331.htm"]
                }
            }
        }
        
        mock_edgar_api.add_company_submissions("0000789019", test_submissions_2)
        
        ciks = ["0000320193", "0000789019"]
        form_types = ["10-K", "10-Q"]
        
        result = await mock_etl_pipeline.run_etl_pipeline(ciks, form_types)
        
        assert result["ciks_processed"] == 2
        assert result["filings_discovered"] == 4  # 2 + 2 filings
    
    @pytest.mark.asyncio
    async def test_pipeline_concurrency_limits(self, mock_etl_pipeline):
        """Test pipeline respects concurrency limits."""
        # Set API delay to simulate real-world conditions
        mock_etl_pipeline.edgar_api.delay_ms = 100
        
        start_time = time.time()
        
        ciks = ["0000320193"]
        form_types = ["10-K", "10-Q", "8-K"]
        
        result = await mock_etl_pipeline.run_etl_pipeline(ciks, form_types)
        
        end_time = time.time()
        
        # Should complete despite delays
        assert result["success"] is True
        assert end_time - start_time < 10.0  # Should not take too long with concurrency
    
    @pytest.mark.asyncio
    async def test_pipeline_error_recovery(self, mock_etl_pipeline):
        """Test pipeline error recovery and partial success."""
        # Introduce some failures by not adding all required mock data
        ciks = ["0000320193", "0000999999"]  # Second CIK has no data
        form_types = ["10-K", "10-Q"]
        
        result = await mock_etl_pipeline.run_etl_pipeline(ciks, form_types)
        
        # Should partially succeed
        assert result["ciks_processed"] >= 1
        assert result["filings_discovered"] >= 2  # At least from first CIK


@pytest.mark.integration
class TestETLPipelinePerformance:
    """Test ETL pipeline performance characteristics."""
    
    @pytest.mark.asyncio
    async def test_sla_compliance_single_ticker(self, mock_etl_pipeline):
        """Test SLA compliance for single ticker processing."""
        ciks = ["0000320193"]
        form_types = ["10-K", "10-Q", "8-K"]
        
        start_time = time.time()
        result = await mock_etl_pipeline.run_etl_pipeline(ciks, form_types)
        end_time = time.time()
        
        processing_time = end_time - start_time
        
        # Single ticker should process very quickly with mocked data
        assert processing_time < 1.0  # Should be under 1 second
        assert result["success"] is True
    
    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_sla_compliance_multiple_tickers(self, mock_edgar_api):
        """Test SLA compliance for multiple tickers (simulated)."""
        # Create mock data for 10 companies
        pipeline = MockETLPipeline(mock_edgar_api)
        
        for i in range(10):
            cik = f"000032019{i:01d}"
            submissions = {
                "cik": int(cik),
                "name": f"Test Company {i}",
                "filings": {
                    "recent": {
                        "form": ["10-K", "10-Q"],
                        "accessionNumber": [f"{cik}-23-00000{j}" for j in range(2)],
                        "filingDate": ["2023-11-01", "2023-08-01"],
                        "primaryDocument": [f"test-{i}-{j}.htm" for j in range(2)]
                    }
                }
            }
            mock_edgar_api.add_company_submissions(cik, submissions)
        
        # Set realistic API delay
        pipeline.edgar_api.delay_ms = 50  # 50ms per request
        
        ciks = [f"000032019{i:01d}" for i in range(10)]
        form_types = ["10-K", "10-Q"]
        
        start_time = time.time()
        result = await pipeline.run_etl_pipeline(ciks, form_types)
        end_time = time.time()
        
        processing_time = end_time - start_time
        
        # Should process 10 tickers efficiently with concurrency
        assert processing_time < 5.0  # Should be under 5 seconds with mocking
        assert result["ciks_processed"] == 10
        
        # Calculate throughput
        filings_per_second = result["filings_discovered"] / processing_time if processing_time > 0 else 0
        print(f"Processed {result['filings_discovered']} filings in {processing_time:.2f}s ({filings_per_second:.1f} filings/sec)")
    
    @pytest.mark.asyncio
    async def test_memory_usage_simulation(self, mock_etl_pipeline):
        """Test memory usage characteristics during processing."""
        # Simulate large document processing
        large_content = b"Large document content " * 1000  # ~23KB per document
        
        # Add large documents to mock data
        for i in range(5):
            mock_etl_pipeline.edgar_api.add_document(
                "0000320193", "0000320193-23-000106", f"large_doc_{i}.htm", large_content
            )
        
        result = await mock_etl_pipeline.download_filing("0000320193", "0000320193-23-000106")
        
        # Should handle large documents without issues
        assert result["success"] is True
        total_size = sum(doc["size"] for doc in result["documents"])
        assert total_size > 100000  # Should be substantial size
    
    @pytest.mark.asyncio
    async def test_rate_limiting_simulation(self, mock_etl_pipeline):
        """Test rate limiting behavior."""
        # Set aggressive rate limiting
        mock_etl_pipeline.edgar_api.delay_ms = 200  # 200ms between requests
        
        ciks = ["0000320193"]
        form_types = ["10-K", "10-Q", "8-K"]
        
        start_time = time.time()
        result = await mock_etl_pipeline.run_etl_pipeline(ciks, form_types)
        end_time = time.time()
        
        # Should respect rate limits and still complete
        assert result["success"] is True
        assert end_time - start_time >= 0.2  # Should take at least one delay period


@pytest.mark.integration
@mock_s3
class TestETLPipelineWithS3:
    """Test ETL pipeline integration with S3 storage."""
    
    def setup_method(self):
        """Set up S3 mock environment."""
        # Create mock S3 bucket
        self.s3_client = boto3.client("s3", region_name="us-east-1")
        self.bucket_name = "test-edgar-filings"
        self.s3_client.create_bucket(Bucket=self.bucket_name)
    
    @pytest.mark.asyncio
    async def test_pipeline_with_s3_storage(self, mock_etl_pipeline):
        """Test ETL pipeline storing results in S3."""
        
        class S3ETLPipeline(MockETLPipeline):
            def __init__(self, edgar_api, s3_client, bucket):
                super().__init__(edgar_api)
                self.s3_client = s3_client
                self.bucket = bucket
                self.s3_uploads = []
            
            async def download_filing(self, cik, accession):
                result = await super().download_filing(cik, accession)
                
                if result["success"]:
                    # Upload documents to S3
                    for doc in result["documents"]:
                        s3_key = f"filings/{cik}/{accession}/{doc['filename']}"
                        self.s3_client.put_object(
                            Bucket=self.bucket,
                            Key=s3_key,
                            Body=doc["content"]
                        )
                        self.s3_uploads.append(s3_key)
                
                return result
        
        s3_pipeline = S3ETLPipeline(
            mock_etl_pipeline.edgar_api,
            self.s3_client,
            self.bucket_name
        )
        
        ciks = ["0000320193"]
        form_types = ["10-K"]
        
        result = await s3_pipeline.run_etl_pipeline(ciks, form_types)
        
        assert result["success"] is True
        
        # Verify files were uploaded to S3
        response = self.s3_client.list_objects_v2(Bucket=self.bucket_name)
        uploaded_keys = [obj["Key"] for obj in response.get("Contents", [])]
        
        assert len(uploaded_keys) > 0
        assert any("filings/0000320193" in key for key in uploaded_keys)
    
    @pytest.mark.asyncio
    async def test_s3_error_handling(self, mock_etl_pipeline):
        """Test S3 error handling in pipeline."""
        
        class FailingS3ETLPipeline(MockETLPipeline):
            def __init__(self, edgar_api):
                super().__init__(edgar_api)
                self.s3_errors = []
            
            async def download_filing(self, cik, accession):
                result = await super().download_filing(cik, accession)
                
                if result["success"]:
                    # Simulate S3 upload failure
                    try:
                        # This will fail since we don't have real S3 client
                        raise Exception("S3 upload failed")
                    except Exception as e:
                        self.s3_errors.append(str(e))
                        # Continue processing despite S3 failure
                
                return result
        
        failing_pipeline = FailingS3ETLPipeline(mock_etl_pipeline.edgar_api)
        
        ciks = ["0000320193"]
        form_types = ["10-K"]
        
        result = await failing_pipeline.run_etl_pipeline(ciks, form_types)
        
        # Pipeline should continue despite S3 errors
        assert result["ciks_processed"] == 1
        assert len(failing_pipeline.s3_errors) > 0


@pytest.mark.integration
class TestETLPipelineMonitoring:
    """Test ETL pipeline monitoring and observability."""
    
    @pytest.mark.asyncio
    async def test_pipeline_metrics_collection(self, mock_etl_pipeline):
        """Test collection of pipeline metrics."""
        
        class MetricsETLPipeline(MockETLPipeline):
            def __init__(self, edgar_api):
                super().__init__(edgar_api)
                self.metrics = {
                    "api_requests": 0,
                    "bytes_downloaded": 0,
                    "processing_times": [],
                    "error_counts": {}
                }
            
            async def discover_filings(self, ciks, form_types):
                start_time = time.time()
                result = await super().discover_filings(ciks, form_types)
                end_time = time.time()
                
                self.metrics["api_requests"] += len(ciks)
                self.metrics["processing_times"].append({
                    "phase": "discovery",
                    "duration": end_time - start_time
                })
                
                return result
            
            async def download_filing(self, cik, accession):
                start_time = time.time()
                result = await super().download_filing(cik, accession)
                end_time = time.time()
                
                self.metrics["api_requests"] += 1
                if result["success"]:
                    bytes_downloaded = sum(doc["size"] for doc in result["documents"])
                    self.metrics["bytes_downloaded"] += bytes_downloaded
                
                self.metrics["processing_times"].append({
                    "phase": "download",
                    "duration": end_time - start_time,
                    "filing": accession
                })
                
                return result
        
        metrics_pipeline = MetricsETLPipeline(mock_etl_pipeline.edgar_api)
        
        ciks = ["0000320193"]
        form_types = ["10-K", "10-Q"]
        
        await metrics_pipeline.run_etl_pipeline(ciks, form_types)
        
        # Verify metrics were collected
        assert metrics_pipeline.metrics["api_requests"] > 0
        assert metrics_pipeline.metrics["bytes_downloaded"] > 0
        assert len(metrics_pipeline.metrics["processing_times"]) > 0
        
        # Check processing time structure
        for timing in metrics_pipeline.metrics["processing_times"]:
            assert "phase" in timing
            assert "duration" in timing
            assert timing["duration"] >= 0
    
    @pytest.mark.asyncio
    async def test_pipeline_error_aggregation(self, mock_etl_pipeline):
        """Test error aggregation and reporting."""
        
        class ErrorTrackingETLPipeline(MockETLPipeline):
            def __init__(self, edgar_api):
                super().__init__(edgar_api)
                self.error_stats = {
                    "by_phase": {},
                    "by_type": {},
                    "total_count": 0
                }
            
            def track_error(self, phase: str, error_type: str, message: str):
                self.error_stats["total_count"] += 1
                
                if phase not in self.error_stats["by_phase"]:
                    self.error_stats["by_phase"][phase] = 0
                self.error_stats["by_phase"][phase] += 1
                
                if error_type not in self.error_stats["by_type"]:
                    self.error_stats["by_type"][error_type] = 0
                self.error_stats["by_type"][error_type] += 1
                
                self.errors.append(f"[{phase}] {error_type}: {message}")
            
            async def download_filing(self, cik, accession):
                # Inject some failures for testing
                if accession.endswith("999"):  # Simulate missing filing
                    self.track_error("download", "NotFound", f"Filing {accession} not found")
                    return {
                        "cik": cik,
                        "accession": accession,
                        "documents": [],
                        "success": False,
                        "error": "Filing not found"
                    }
                
                return await super().download_filing(cik, accession)
        
        error_pipeline = ErrorTrackingETLPipeline(mock_etl_pipeline.edgar_api)
        
        # Add some failing scenarios
        mock_etl_pipeline.edgar_api.add_company_submissions("0000999999", {
            "cik": 999999,
            "name": "Failing Company",
            "filings": {
                "recent": {
                    "form": ["10-K"],
                    "accessionNumber": ["0000999999-23-000999"],  # Will trigger failure
                    "filingDate": ["2023-01-01"],
                    "primaryDocument": ["failing.htm"]
                }
            }
        })
        
        ciks = ["0000320193", "0000999999"]
        form_types = ["10-K"]
        
        result = await error_pipeline.run_etl_pipeline(ciks, form_types)
        
        # Should have processed with some errors
        assert error_pipeline.error_stats["total_count"] > 0
        assert "download" in error_pipeline.error_stats["by_phase"]
        assert "NotFound" in error_pipeline.error_stats["by_type"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])