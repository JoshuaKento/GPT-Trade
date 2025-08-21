"""Performance tests to validate SLA compliance for ETL pipeline."""

import asyncio
import concurrent.futures
import memory_profiler
import psutil
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from unittest.mock import Mock, patch

import pytest

from tests.test_database_models import (
    CompanyFactory,
    FilingFactory,
    DocumentFactory,
    EtlRunFactory
)
from tests.test_etl_pipeline import MockETLPipeline, MockEdgarAPI


class PerformanceMetrics:
    """Collect and analyze performance metrics."""
    
    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.memory_samples = []
        self.cpu_samples = []
        self.throughput_metrics = {}
        self.latency_metrics = {}
        self.resource_usage = {}
    
    def start_monitoring(self):
        """Start performance monitoring."""
        self.start_time = time.time()
        self.memory_samples = []
        self.cpu_samples = []
    
    def sample_resources(self):
        """Sample current resource usage."""
        process = psutil.Process()
        memory_mb = process.memory_info().rss / 1024 / 1024
        cpu_percent = process.cpu_percent()
        
        self.memory_samples.append({
            'timestamp': time.time(),
            'memory_mb': memory_mb
        })
        self.cpu_samples.append({
            'timestamp': time.time(),
            'cpu_percent': cpu_percent
        })
    
    def stop_monitoring(self):
        """Stop performance monitoring and calculate metrics."""
        self.end_time = time.time()
        
        if self.memory_samples:
            memory_values = [s['memory_mb'] for s in self.memory_samples]
            self.resource_usage['peak_memory_mb'] = max(memory_values)
            self.resource_usage['avg_memory_mb'] = sum(memory_values) / len(memory_values)
        
        if self.cpu_samples:
            cpu_values = [s['cpu_percent'] for s in self.cpu_samples]
            self.resource_usage['peak_cpu_percent'] = max(cpu_values)
            self.resource_usage['avg_cpu_percent'] = sum(cpu_values) / len(cpu_values)
        
        self.resource_usage['total_duration'] = self.end_time - self.start_time if self.start_time else 0
    
    def calculate_throughput(self, processed_items: int, item_type: str = "items"):
        """Calculate throughput metrics."""
        duration = self.resource_usage.get('total_duration', 0)
        if duration > 0:
            self.throughput_metrics[f"{item_type}_per_second"] = processed_items / duration
            self.throughput_metrics[f"{item_type}_per_minute"] = processed_items / duration * 60
    
    def add_latency_metric(self, operation: str, duration: float):
        """Add latency measurement for an operation."""
        if operation not in self.latency_metrics:
            self.latency_metrics[operation] = []
        self.latency_metrics[operation].append(duration)
    
    def get_latency_stats(self, operation: str) -> Dict[str, float]:
        """Get latency statistics for an operation."""
        if operation not in self.latency_metrics or not self.latency_metrics[operation]:
            return {}
        
        latencies = self.latency_metrics[operation]
        return {
            'min': min(latencies),
            'max': max(latencies),
            'avg': sum(latencies) / len(latencies),
            'p95': sorted(latencies)[int(len(latencies) * 0.95)] if len(latencies) > 1 else latencies[0]
        }
    
    def assert_sla_compliance(self, sla_requirements: Dict[str, float]):
        """Assert that performance meets SLA requirements."""
        for metric, threshold in sla_requirements.items():
            if metric == "max_duration_minutes":
                actual = self.resource_usage.get('total_duration', 0) / 60
                assert actual <= threshold, f"Duration {actual:.2f}min exceeds SLA {threshold}min"
            
            elif metric == "max_memory_mb":
                actual = self.resource_usage.get('peak_memory_mb', 0)
                assert actual <= threshold, f"Memory {actual:.1f}MB exceeds SLA {threshold}MB"
            
            elif metric == "min_throughput_per_minute":
                actual = self.throughput_metrics.get('items_per_minute', 0)
                assert actual >= threshold, f"Throughput {actual:.1f}/min below SLA {threshold}/min"
            
            elif metric.startswith("max_latency_"):
                operation = metric.replace("max_latency_", "")
                stats = self.get_latency_stats(operation)
                if stats:
                    actual = stats.get('avg', 0)
                    assert actual <= threshold, f"Latency {actual:.3f}s exceeds SLA {threshold}s"


@contextmanager
def performance_monitor():
    """Context manager for performance monitoring."""
    metrics = PerformanceMetrics()
    metrics.start_monitoring()
    
    # Start background monitoring
    monitoring_active = True
    
    async def monitor_resources():
        while monitoring_active:
            metrics.sample_resources()
            await asyncio.sleep(0.1)  # Sample every 100ms
    
    monitor_task = None
    try:
        # Start monitoring task if in async context
        try:
            loop = asyncio.get_event_loop()
            monitor_task = loop.create_task(monitor_resources())
        except RuntimeError:
            # Not in async context, just sample once
            metrics.sample_resources()
        
        yield metrics
        
    finally:
        monitoring_active = False
        if monitor_task:
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass
        
        metrics.stop_monitoring()


class PerformanceETLPipeline(MockETLPipeline):
    """ETL pipeline with performance instrumentation."""
    
    def __init__(self, edgar_api: MockEdgarAPI, performance_metrics: PerformanceMetrics):
        super().__init__(edgar_api)
        self.performance_metrics = performance_metrics
        self.operation_times = {}
    
    def _time_operation(self, operation_name: str):
        """Decorator to time operations."""
        def decorator(func):
            async def wrapper(*args, **kwargs):
                start_time = time.time()
                try:
                    result = await func(*args, **kwargs)
                    return result
                finally:
                    end_time = time.time()
                    duration = end_time - start_time
                    self.performance_metrics.add_latency_metric(operation_name, duration)
                    
                    if operation_name not in self.operation_times:
                        self.operation_times[operation_name] = []
                    self.operation_times[operation_name].append(duration)
            
            return wrapper
        return decorator
    
    async def discover_filings(self, ciks: List[str], form_types: List[str]) -> List[Dict]:
        """Instrumented filing discovery."""
        operation = self._time_operation("filing_discovery")
        return await operation(super().discover_filings)(ciks, form_types)
    
    async def download_filing(self, cik: str, accession: str) -> Dict:
        """Instrumented filing download."""
        operation = self._time_operation("filing_download")
        return await operation(super().download_filing)(cik, accession)
    
    async def process_documents(self, filing_data: Dict) -> Dict:
        """Instrumented document processing."""
        operation = self._time_operation("document_processing")
        return await operation(super().process_documents)(filing_data)


@pytest.fixture
def performance_edgar_api():
    """Create EDGAR API with performance test data."""
    api = MockEdgarAPI()
    api.delay_ms = 10  # Realistic API delay
    
    # Create data for 50 companies (SLA requirement)
    for i in range(50):
        cik = f"000032{i:04d}"
        submissions = {
            "cik": int(cik),
            "name": f"Test Company {i}",
            "filings": {
                "recent": {
                    "form": ["10-K", "10-Q", "8-K"] * 5,  # 15 filings per company
                    "accessionNumber": [f"{cik}-23-{j:06d}" for j in range(15)],
                    "filingDate": ["2023-11-01"] * 15,
                    "primaryDocument": [f"filing-{i}-{j}.htm" for j in range(15)]
                }
            }
        }
        api.add_company_submissions(cik, submissions)
        
        # Add filing indexes and documents
        for j in range(15):
            accession = f"{cik}-23-{j:06d}"
            filing_index = f"""
            <html>
            <table class="tableFile">
            <tr><td>1</td><td>primary-{j}.htm</td><td>10-K</td></tr>
            <tr><td>2</td><td>exhibit-{j}-1.htm</td><td>EX-21.1</td></tr>
            <tr><td>3</td><td>exhibit-{j}-2.htm</td><td>EX-23.1</td></tr>
            </table>
            </html>
            """
            api.add_filing_index(cik, accession, filing_index)
            
            # Add document content (varying sizes)
            for doc_idx, doc_name in enumerate([f"primary-{j}.htm", f"exhibit-{j}-1.htm", f"exhibit-{j}-2.htm"]):
                content_size = 1000 + (doc_idx * 500)  # 1KB, 1.5KB, 2KB
                content = b"Document content " * (content_size // 16)
                api.add_document(cik, accession, doc_name, content)
    
    return api


@pytest.mark.performance
@pytest.mark.slow
class TestSLACompliance:
    """Test SLA compliance for ETL pipeline performance."""
    
    @pytest.mark.asyncio
    async def test_single_ticker_processing_time(self, performance_edgar_api):
        """Test single ticker processing meets SLA."""
        with performance_monitor() as metrics:
            pipeline = PerformanceETLPipeline(performance_edgar_api, metrics)
            
            ciks = ["0000320000"]  # Single company
            form_types = ["10-K", "10-Q", "8-K"]
            
            result = await pipeline.run_etl_pipeline(ciks, form_types)
            
            assert result["success"] is True
            
            # Calculate throughput
            metrics.calculate_throughput(result["filings_discovered"], "filings")
            
            # SLA: Single ticker should process quickly
            sla_requirements = {
                "max_duration_minutes": 0.5,  # 30 seconds max for single ticker
                "max_memory_mb": 200,  # 200MB max memory
                "min_throughput_per_minute": 60  # At least 1 filing per second
            }
            
            metrics.assert_sla_compliance(sla_requirements)
    
    @pytest.mark.asyncio
    async def test_fifty_tickers_sla_compliance(self, performance_edgar_api):
        """Test 50 tickers processing meets 30-minute SLA."""
        with performance_monitor() as metrics:
            pipeline = PerformanceETLPipeline(performance_edgar_api, metrics)
            
            # All 50 companies
            ciks = [f"000032{i:04d}" for i in range(50)]
            form_types = ["10-K", "10-Q", "8-K"]
            
            result = await pipeline.run_etl_pipeline(ciks, form_types)
            
            assert result["success"] is True
            assert result["ciks_processed"] == 50
            
            # Calculate throughput metrics
            metrics.calculate_throughput(result["filings_discovered"], "filings")
            metrics.calculate_throughput(result["ciks_processed"], "companies")
            
            # Core SLA: 50 tickers in 30 minutes max
            sla_requirements = {
                "max_duration_minutes": 30.0,
                "max_memory_mb": 1000,  # 1GB max memory
                "min_throughput_per_minute": 1.5  # At least 1.5 companies per minute
            }
            
            metrics.assert_sla_compliance(sla_requirements)
            
            # Print performance summary
            print(f"\nPerformance Summary:")
            print(f"Duration: {metrics.resource_usage['total_duration']:.2f}s")
            print(f"Companies/minute: {metrics.throughput_metrics.get('companies_per_minute', 0):.1f}")
            print(f"Filings/minute: {metrics.throughput_metrics.get('filings_per_minute', 0):.1f}")
            print(f"Peak memory: {metrics.resource_usage.get('peak_memory_mb', 0):.1f}MB")
    
    @pytest.mark.asyncio
    async def test_memory_usage_scalability(self, performance_edgar_api):
        """Test memory usage scales linearly with data volume."""
        memory_measurements = []
        
        # Test with increasing data volumes
        for num_companies in [1, 5, 10, 25]:
            with performance_monitor() as metrics:
                pipeline = PerformanceETLPipeline(performance_edgar_api, metrics)
                
                ciks = [f"000032{i:04d}" for i in range(num_companies)]
                form_types = ["10-K"]  # Limit to one form type for consistency
                
                result = await pipeline.run_etl_pipeline(ciks, form_types)
                
                memory_measurements.append({
                    'companies': num_companies,
                    'peak_memory': metrics.resource_usage.get('peak_memory_mb', 0),
                    'avg_memory': metrics.resource_usage.get('avg_memory_mb', 0),
                    'filings_processed': result.get('filings_discovered', 0)
                })
        
        # Verify memory usage doesn't grow exponentially
        for i in range(1, len(memory_measurements)):
            prev = memory_measurements[i-1]
            curr = memory_measurements[i]
            
            # Memory growth should be roughly linear with data volume
            company_ratio = curr['companies'] / prev['companies']
            memory_ratio = curr['peak_memory'] / prev['peak_memory'] if prev['peak_memory'] > 0 else 1
            
            # Memory growth should not exceed 2x the company growth ratio
            assert memory_ratio <= company_ratio * 2, \
                f"Memory grew {memory_ratio:.1f}x when companies grew {company_ratio:.1f}x"
        
        print(f"\nMemory Scalability:")
        for measurement in memory_measurements:
            print(f"{measurement['companies']} companies: {measurement['peak_memory']:.1f}MB peak")
    
    @pytest.mark.asyncio
    async def test_concurrent_processing_performance(self, performance_edgar_api):
        """Test concurrent processing improves performance."""
        
        # Test sequential processing
        sequential_start = time.time()
        pipeline = PerformanceETLPipeline(performance_edgar_api, PerformanceMetrics())
        
        ciks = [f"000032{i:04d}" for i in range(10)]
        form_types = ["10-K"]
        
        # Process sequentially
        for cik in ciks:
            await pipeline.discover_filings([cik], form_types)
        
        sequential_time = time.time() - sequential_start
        
        # Test concurrent processing
        concurrent_start = time.time()
        
        # Process concurrently
        tasks = [pipeline.discover_filings([cik], form_types) for cik in ciks]
        await asyncio.gather(*tasks)
        
        concurrent_time = time.time() - concurrent_start
        
        # Concurrent should be faster (with mocked delays)
        speedup_ratio = sequential_time / concurrent_time if concurrent_time > 0 else 1
        print(f"\nConcurrency speedup: {speedup_ratio:.1f}x")
        print(f"Sequential: {sequential_time:.2f}s, Concurrent: {concurrent_time:.2f}s")
        
        # Should achieve at least 2x speedup with 10 concurrent operations
        assert speedup_ratio >= 2.0, f"Concurrency speedup {speedup_ratio:.1f}x is too low"


@pytest.mark.performance
class TestLatencyRequirements:
    """Test latency requirements for individual operations."""
    
    @pytest.mark.asyncio
    async def test_filing_discovery_latency(self, performance_edgar_api):
        """Test filing discovery latency per company."""
        metrics = PerformanceMetrics()
        pipeline = PerformanceETLPipeline(performance_edgar_api, metrics)
        
        # Test with multiple companies
        ciks = [f"000032{i:04d}" for i in range(10)]
        form_types = ["10-K", "10-Q"]
        
        for cik in ciks:
            await pipeline.discover_filings([cik], form_types)
        
        # Check latency statistics
        discovery_stats = metrics.get_latency_stats("filing_discovery")
        
        assert discovery_stats["avg"] <= 1.0, f"Avg discovery latency {discovery_stats['avg']:.3f}s too high"
        assert discovery_stats["p95"] <= 2.0, f"P95 discovery latency {discovery_stats['p95']:.3f}s too high"
        
        print(f"Filing discovery latency - Avg: {discovery_stats['avg']:.3f}s, P95: {discovery_stats['p95']:.3f}s")
    
    @pytest.mark.asyncio
    async def test_filing_download_latency(self, performance_edgar_api):
        """Test filing download latency."""
        metrics = PerformanceMetrics()
        pipeline = PerformanceETLPipeline(performance_edgar_api, metrics)
        
        # Test multiple downloads
        test_downloads = [
            ("0000320000", "0000320000-23-000000"),
            ("0000320001", "0000320001-23-000001"),
            ("0000320002", "0000320002-23-000002"),
        ]
        
        for cik, accession in test_downloads:
            await pipeline.download_filing(cik, accession)
        
        download_stats = metrics.get_latency_stats("filing_download")
        
        assert download_stats["avg"] <= 0.5, f"Avg download latency {download_stats['avg']:.3f}s too high"
        assert download_stats["p95"] <= 1.0, f"P95 download latency {download_stats['p95']:.3f}s too high"
        
        print(f"Filing download latency - Avg: {download_stats['avg']:.3f}s, P95: {download_stats['p95']:.3f}s")
    
    @pytest.mark.asyncio
    async def test_document_processing_latency(self, performance_edgar_api):
        """Test document processing latency."""
        metrics = PerformanceMetrics()
        pipeline = PerformanceETLPipeline(performance_edgar_api, metrics)
        
        # Create test filing data
        test_filings = [
            {
                "accession": f"test-filing-{i}",
                "documents": [
                    {"filename": f"doc-{j}.htm", "content": b"Document content " * 100}
                    for j in range(5)
                ]
            }
            for i in range(10)
        ]
        
        for filing_data in test_filings:
            await pipeline.process_documents(filing_data)
        
        processing_stats = metrics.get_latency_stats("document_processing")
        
        assert processing_stats["avg"] <= 0.2, f"Avg processing latency {processing_stats['avg']:.3f}s too high"
        assert processing_stats["p95"] <= 0.5, f"P95 processing latency {processing_stats['p95']:.3f}s too high"
        
        print(f"Document processing latency - Avg: {processing_stats['avg']:.3f}s, P95: {processing_stats['p95']:.3f}s")


@pytest.mark.performance
class TestThroughputRequirements:
    """Test throughput requirements for the pipeline."""
    
    @pytest.mark.asyncio
    async def test_filing_discovery_throughput(self, performance_edgar_api):
        """Test filing discovery throughput."""
        with performance_monitor() as metrics:
            pipeline = PerformanceETLPipeline(performance_edgar_api, metrics)
            
            # Large batch for throughput testing
            ciks = [f"000032{i:04d}" for i in range(50)]
            form_types = ["10-K", "10-Q", "8-K"]
            
            discovered = await pipeline.discover_filings(ciks, form_types)
            
            metrics.calculate_throughput(len(discovered), "filings")
            metrics.calculate_throughput(len(ciks), "companies")
            
            # Should discover at least 100 filings per minute
            filings_per_min = metrics.throughput_metrics.get("filings_per_minute", 0)
            companies_per_min = metrics.throughput_metrics.get("companies_per_minute", 0)
            
            assert filings_per_min >= 100, f"Filing discovery throughput {filings_per_min:.1f}/min too low"
            assert companies_per_min >= 10, f"Company processing throughput {companies_per_min:.1f}/min too low"
            
            print(f"Discovery throughput - Filings: {filings_per_min:.1f}/min, Companies: {companies_per_min:.1f}/min")
    
    @pytest.mark.asyncio
    async def test_document_download_throughput(self, performance_edgar_api):
        """Test document download throughput."""
        with performance_monitor() as metrics:
            pipeline = PerformanceETLPipeline(performance_edgar_api, metrics)
            
            # Test downloading multiple filings
            download_tasks = [
                pipeline.download_filing(f"000032{i:04d}", f"000032{i:04d}-23-000000")
                for i in range(20)
            ]
            
            results = await asyncio.gather(*download_tasks)
            successful_downloads = [r for r in results if r.get("success")]
            total_documents = sum(len(r.get("documents", [])) for r in successful_downloads)
            
            metrics.calculate_throughput(total_documents, "documents")
            
            # Should download at least 50 documents per minute
            docs_per_min = metrics.throughput_metrics.get("documents_per_minute", 0)
            
            assert docs_per_min >= 50, f"Document download throughput {docs_per_min:.1f}/min too low"
            
            print(f"Download throughput - Documents: {docs_per_min:.1f}/min")


@pytest.mark.performance
@pytest.mark.slow
class TestStressAndLoad:
    """Stress and load testing for the ETL pipeline."""
    
    @pytest.mark.asyncio
    async def test_high_concurrency_stress(self, performance_edgar_api):
        """Test pipeline under high concurrency load."""
        with performance_monitor() as metrics:
            pipeline = PerformanceETLPipeline(performance_edgar_api, metrics)
            
            # High concurrency test
            num_concurrent = 100
            tasks = [
                pipeline.discover_filings([f"000032{i%50:04d}"], ["10-K"])
                for i in range(num_concurrent)
            ]
            
            start_time = time.time()
            results = await asyncio.gather(*tasks, return_exceptions=True)
            end_time = time.time()
            
            # Check for failures
            exceptions = [r for r in results if isinstance(r, Exception)]
            successful = len(results) - len(exceptions)
            
            # Should handle high concurrency with minimal failures
            success_rate = successful / len(results) * 100
            assert success_rate >= 95.0, f"Success rate {success_rate:.1f}% too low under high concurrency"
            
            # Performance should still be reasonable
            total_time = end_time - start_time
            requests_per_second = num_concurrent / total_time if total_time > 0 else 0
            
            print(f"High concurrency test - {success_rate:.1f}% success, {requests_per_second:.1f} req/s")
    
    @pytest.mark.asyncio
    async def test_sustained_load(self, performance_edgar_api):
        """Test pipeline under sustained load."""
        with performance_monitor() as metrics:
            pipeline = PerformanceETLPipeline(performance_edgar_api, metrics)
            
            # Simulate sustained load for multiple batches
            batch_size = 10
            num_batches = 5
            
            for batch in range(num_batches):
                start_idx = batch * batch_size
                ciks = [f"000032{(start_idx + i)%50:04d}" for i in range(batch_size)]
                
                await pipeline.discover_filings(ciks, ["10-K", "10-Q"])
                
                # Small delay between batches
                await asyncio.sleep(0.1)
            
            # Memory usage should be stable (not growing continuously)
            if len(metrics.memory_samples) >= 2:
                early_memory = sum(s['memory_mb'] for s in metrics.memory_samples[:5]) / 5
                late_memory = sum(s['memory_mb'] for s in metrics.memory_samples[-5:]) / 5
                
                memory_growth = (late_memory - early_memory) / early_memory * 100 if early_memory > 0 else 0
                
                # Memory growth should be minimal under sustained load
                assert memory_growth <= 50, f"Memory grew {memory_growth:.1f}% during sustained load"
                
                print(f"Sustained load test - Memory growth: {memory_growth:.1f}%")


@pytest.mark.performance
class TestResourceUtilization:
    """Test resource utilization efficiency."""
    
    @pytest.mark.asyncio
    async def test_cpu_utilization_efficiency(self, performance_edgar_api):
        """Test CPU utilization during processing."""
        with performance_monitor() as metrics:
            pipeline = PerformanceETLPipeline(performance_edgar_api, metrics)
            
            # CPU-intensive task simulation
            ciks = [f"000032{i:04d}" for i in range(20)]
            form_types = ["10-K", "10-Q", "8-K"]
            
            await pipeline.run_etl_pipeline(ciks, form_types)
            
            avg_cpu = metrics.resource_usage.get('avg_cpu_percent', 0)
            peak_cpu = metrics.resource_usage.get('peak_cpu_percent', 0)
            
            # Should utilize CPU efficiently but not peg it at 100%
            assert avg_cpu >= 10, f"CPU utilization {avg_cpu:.1f}% too low (may indicate blocking)"
            assert peak_cpu <= 90, f"CPU utilization {peak_cpu:.1f}% too high (may indicate inefficiency)"
            
            print(f"CPU utilization - Avg: {avg_cpu:.1f}%, Peak: {peak_cpu:.1f}%")
    
    @pytest.mark.asyncio
    async def test_memory_efficiency(self, performance_edgar_api):
        """Test memory usage efficiency."""
        with performance_monitor() as metrics:
            pipeline = PerformanceETLPipeline(performance_edgar_api, metrics)
            
            # Process data that should fit efficiently in memory
            ciks = [f"000032{i:04d}" for i in range(10)]
            form_types = ["10-K"]
            
            result = await pipeline.run_etl_pipeline(ciks, form_types)
            
            peak_memory = metrics.resource_usage.get('peak_memory_mb', 0)
            avg_memory = metrics.resource_usage.get('avg_memory_mb', 0)
            
            # Memory usage should be reasonable for the data volume
            docs_processed = result.get('documents_processed', 0)
            if docs_processed > 0:
                memory_per_doc = peak_memory / docs_processed
                
                # Should use less than 1MB per document on average
                assert memory_per_doc <= 1.0, f"Memory per document {memory_per_doc:.2f}MB too high"
            
            print(f"Memory efficiency - Peak: {peak_memory:.1f}MB, Avg: {avg_memory:.1f}MB")
            print(f"Memory per document: {memory_per_doc:.2f}MB" if docs_processed > 0 else "No documents processed")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "performance"])