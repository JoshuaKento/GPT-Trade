"""
Test cases for GPT Trader ETL pipeline components.
"""

import pytest
import time
import threading
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
from queue import Empty

from gpt_trader.etl import (
    ETLTask, TaskQueue, ETLWorker, ETLPipeline, ETLScheduler, ETLJobResult
)
from gpt_trader.config import GPTTraderConfig, TickerConfig, ETLConfig
from gpt_trader.models import ProcessingJob, ProcessingStatus


@pytest.fixture
def sample_config():
    """Create sample GPT Trader configuration."""
    config = GPTTraderConfig()
    config.etl = ETLConfig(
        batch_size=10,
        max_concurrent_jobs=2,
        job_timeout_minutes=5,
        sla_processing_time_minutes=15
    )
    config.add_ticker("AAPL", "0000320193", priority=1)
    config.add_ticker("MSFT", "0000789019", priority=2)
    return config


@pytest.fixture
def sample_ticker_config():
    """Create sample ticker configuration."""
    return TickerConfig(
        ticker="AAPL",
        cik="0000320193",
        priority=1,
        forms_to_process=["10-K", "10-Q"]
    )


@pytest.fixture
def sample_etl_task(sample_ticker_config):
    """Create sample ETL task."""
    return ETLTask(
        task_id="test_task_001",
        task_type="fetch_filings",
        ticker_config=sample_ticker_config,
        parameters={"max_filings": 10},
        priority=1
    )


class TestETLTask:
    """Test cases for ETLTask."""
    
    def test_task_creation(self, sample_ticker_config):
        """Test creating an ETL task."""
        task = ETLTask(
            task_id="test_001",
            task_type="fetch_filings",
            ticker_config=sample_ticker_config,
            parameters={"batch_size": 50}
        )
        
        assert task.task_id == "test_001"
        assert task.task_type == "fetch_filings"
        assert task.ticker_config.ticker == "AAPL"
        assert task.parameters["batch_size"] == 50
        assert task.priority == 1
        assert task.max_retries == 3
        assert task.retry_count == 0
        assert task.created_at is not None
    
    def test_task_post_init(self, sample_ticker_config):
        """Test task post-initialization."""
        task = ETLTask(
            task_id="test_002",
            task_type="fetch_filings",
            ticker_config=sample_ticker_config,
            parameters={}
        )
        
        # created_at should be set automatically
        assert isinstance(task.created_at, datetime)
        assert task.created_at <= datetime.utcnow()


class TestTaskQueue:
    """Test cases for TaskQueue."""
    
    def test_queue_creation(self):
        """Test creating a task queue."""
        queue = TaskQueue(max_size=100)
        assert queue._total_tasks == 0
        assert queue._completed_tasks == 0
        assert queue.is_empty() is True
    
    def test_put_and_get_task(self, sample_etl_task):
        """Test putting and getting tasks."""
        queue = TaskQueue()
        
        # Put task
        queue.put_task(sample_etl_task)
        assert queue._total_tasks == 1
        assert not queue.is_empty()
        
        # Get task
        retrieved_task = queue.get_task(timeout=0.1)
        assert retrieved_task is not None
        assert retrieved_task.task_id == sample_etl_task.task_id
    
    def test_priority_queue(self, sample_ticker_config):
        """Test priority queue functionality."""
        queue = TaskQueue()
        
        # Add regular task
        regular_task = ETLTask(
            task_id="regular",
            task_type="fetch_filings",
            ticker_config=sample_ticker_config,
            parameters={},
            priority=1
        )
        queue.put_task(regular_task)
        
        # Add high priority task
        priority_task = ETLTask(
            task_id="priority",
            task_type="fetch_filings", 
            ticker_config=sample_ticker_config,
            parameters={},
            priority=10
        )
        queue.put_task(priority_task, high_priority=True)
        
        # Priority task should come first
        first_task = queue.get_task(timeout=0.1)
        assert first_task.task_id == "priority"
        
        second_task = queue.get_task(timeout=0.1)
        assert second_task.task_id == "regular"
    
    def test_task_done(self, sample_etl_task):
        """Test marking tasks as done."""
        queue = TaskQueue()
        queue.put_task(sample_etl_task)
        
        queue.task_done()
        assert queue._completed_tasks == 1
    
    def test_queue_stats(self, sample_etl_task):
        """Test getting queue statistics."""
        queue = TaskQueue()
        queue.put_task(sample_etl_task)
        
        stats = queue.get_queue_stats()
        assert stats['total_tasks'] == 1
        assert stats['completed_tasks'] == 0
        assert stats['pending_tasks'] == 1
    
    def test_get_task_timeout(self):
        """Test getting task with timeout when queue is empty."""
        queue = TaskQueue()
        
        start_time = time.time()
        task = queue.get_task(timeout=0.1)
        end_time = time.time()
        
        assert task is None
        assert end_time - start_time >= 0.1


class TestETLWorker:
    """Test cases for ETLWorker."""
    
    @patch('gpt_trader.etl.BatchFilingProcessor')
    def test_worker_creation(self, mock_processor, sample_config):
        """Test creating an ETL worker."""
        queue = TaskQueue()
        callback = Mock()
        
        worker = ETLWorker("worker-1", sample_config, queue, callback)
        
        assert worker.worker_id == "worker-1"
        assert worker.config == sample_config
        assert worker.task_queue == queue
        assert worker.result_callback == callback
        assert worker.is_running is False
        assert worker.current_task is None
    
    @patch('gpt_trader.etl.BatchFilingProcessor')
    def test_worker_start_stop(self, mock_processor, sample_config):
        """Test starting and stopping worker."""
        queue = TaskQueue()
        callback = Mock()
        
        worker = ETLWorker("worker-1", sample_config, queue, callback)
        
        # Start worker
        worker.start()
        assert worker.is_running is True
        assert hasattr(worker, 'thread')
        assert worker.thread.is_alive()
        
        # Stop worker
        worker.stop()
        assert worker.is_running is False
        # Thread should be joined
        assert not worker.thread.is_alive()
    
    @patch('gpt_trader.etl.session_scope')
    @patch('gpt_trader.etl.BatchFilingProcessor')
    def test_process_task_success(self, mock_processor_class, mock_session_scope, 
                                 sample_config, sample_etl_task):
        """Test successful task processing."""
        # Mock the session context manager
        mock_session = Mock()
        mock_session_scope.return_value.__enter__.return_value = mock_session
        mock_session_scope.return_value.__exit__.return_value = None
        
        # Mock the processor
        mock_processor_instance = Mock()
        mock_processor_class.return_value.processor = Mock()
        mock_processor_class.return_value.processor.fetch_and_store_filings.return_value = (5, 2)
        
        # Mock job creation
        mock_job = Mock()
        mock_job.id = 123
        mock_job.job_uuid = "test-uuid-123"
        
        with patch('gpt_trader.etl.ProcessingJob') as mock_job_class:
            mock_job_class.create_job.return_value = mock_job
            
            queue = TaskQueue()
            callback = Mock()
            
            worker = ETLWorker("worker-1", sample_config, queue, callback)
            worker.processor = mock_processor_class.return_value
            
            result = worker._process_task(sample_etl_task)
            
            assert result.success is True
            assert result.items_processed == 7  # 5 + 2
            assert result.items_failed == 0
            assert result.job_id == 123
            assert result.job_uuid == "test-uuid-123"
    
    @patch('gpt_trader.etl.session_scope')
    @patch('gpt_trader.etl.BatchFilingProcessor')
    def test_process_task_failure(self, mock_processor_class, mock_session_scope,
                                 sample_config, sample_etl_task):
        """Test task processing failure."""
        # Mock the session context manager
        mock_session = Mock()
        mock_session_scope.return_value.__enter__.return_value = mock_session
        mock_session_scope.return_value.__exit__.return_value = None
        
        # Mock the processor to raise an exception
        mock_processor_instance = Mock()
        mock_processor_class.return_value.processor = Mock()
        mock_processor_class.return_value.processor.fetch_and_store_filings.side_effect = Exception("Test error")
        
        # Mock job creation
        mock_job = Mock()
        mock_job.id = 123
        mock_job.job_uuid = "test-uuid-123"
        
        with patch('gpt_trader.etl.ProcessingJob') as mock_job_class:
            mock_job_class.create_job.return_value = mock_job
            
            queue = TaskQueue()
            callback = Mock()
            
            worker = ETLWorker("worker-1", sample_config, queue, callback)
            worker.processor = mock_processor_class.return_value
            
            result = worker._process_task(sample_etl_task)
            
            assert result.success is False
            assert result.items_processed == 0
            assert result.items_failed == 1
            assert "Test error" in result.error_message


class TestETLPipeline:
    """Test cases for ETLPipeline."""
    
    @patch('gpt_trader.etl.PerformanceMonitor')
    @patch('gpt_trader.etl.SLAMonitor')
    def test_pipeline_creation(self, mock_sla_monitor, mock_perf_monitor, sample_config):
        """Test creating ETL pipeline."""
        pipeline = ETLPipeline(sample_config)
        
        assert pipeline.config == sample_config
        assert pipeline.is_running is False
        assert len(pipeline.workers) == 0
        assert len(pipeline.results) == 0
    
    @patch('gpt_trader.etl.PerformanceMonitor')
    @patch('gpt_trader.etl.SLAMonitor')
    @patch('gpt_trader.etl.ETLWorker')
    def test_pipeline_start_stop(self, mock_worker_class, mock_sla_monitor, 
                                mock_perf_monitor, sample_config):
        """Test starting and stopping pipeline."""
        # Mock worker instances
        mock_workers = []
        for i in range(sample_config.etl.max_concurrent_jobs):
            mock_worker = Mock()
            mock_workers.append(mock_worker)
        mock_worker_class.side_effect = mock_workers
        
        pipeline = ETLPipeline(sample_config)
        
        # Start pipeline
        pipeline.start()
        assert pipeline.is_running is True
        assert len(pipeline.workers) == sample_config.etl.max_concurrent_jobs
        
        # Verify workers were started
        for mock_worker in mock_workers:
            mock_worker.start.assert_called_once()
        
        # Stop pipeline
        pipeline.stop()
        assert pipeline.is_running is False
        
        # Verify workers were stopped
        for mock_worker in mock_workers:
            mock_worker.stop.assert_called_once()
    
    @patch('gpt_trader.etl.PerformanceMonitor')
    @patch('gpt_trader.etl.SLAMonitor')
    def test_submit_ticker_task(self, mock_sla_monitor, mock_perf_monitor, 
                               sample_config, sample_ticker_config):
        """Test submitting a ticker task."""
        pipeline = ETLPipeline(sample_config)
        
        task_id = pipeline.submit_ticker_task(
            ticker_config=sample_ticker_config,
            task_type="fetch_filings",
            parameters={"max_filings": 20}
        )
        
        assert task_id is not None
        assert "fetch_filings_AAPL" in task_id
        
        # Verify task was added to queue
        stats = pipeline.task_queue.get_queue_stats()
        assert stats['total_tasks'] == 1
    
    @patch('gpt_trader.etl.PerformanceMonitor')
    @patch('gpt_trader.etl.SLAMonitor')
    def test_submit_batch_job(self, mock_sla_monitor, mock_perf_monitor, sample_config):
        """Test submitting a batch job."""
        pipeline = ETLPipeline(sample_config)
        
        ticker_configs = sample_config.get_active_tickers()
        job_name = pipeline.submit_batch_job(ticker_configs, "test_batch")
        
        assert job_name == "test_batch"
        
        # Verify tasks were added to queue
        stats = pipeline.task_queue.get_queue_stats()
        assert stats['total_tasks'] == len(ticker_configs)
    
    @patch('gpt_trader.etl.PerformanceMonitor')
    @patch('gpt_trader.etl.SLAMonitor')
    def test_handle_result(self, mock_sla_monitor, mock_perf_monitor, sample_config):
        """Test handling job results."""
        pipeline = ETLPipeline(sample_config)
        
        result = ETLJobResult(
            job_id=123,
            job_uuid="test-job",
            success=True,
            duration_seconds=45.5,
            items_processed=10,
            items_failed=0
        )
        
        pipeline._handle_result(result)
        
        assert len(pipeline.results) == 1
        assert pipeline.results[0] == result
    
    @patch('gpt_trader.etl.PerformanceMonitor')
    @patch('gpt_trader.etl.SLAMonitor')
    @patch('psutil.cpu_percent')
    @patch('psutil.virtual_memory')
    @patch('psutil.disk_usage')
    def test_get_pipeline_status(self, mock_disk, mock_memory, mock_cpu,
                                mock_sla_monitor, mock_perf_monitor, sample_config):
        """Test getting pipeline status."""
        # Mock system metrics
        mock_cpu.return_value = 25.5
        mock_memory.return_value.percent = 60.0
        mock_disk.return_value.percent = 45.0
        
        pipeline = ETLPipeline(sample_config)
        
        status = pipeline.get_pipeline_status()
        
        assert 'is_running' in status
        assert 'queue_stats' in status
        assert 'worker_status' in status
        assert 'system_metrics' in status
        assert status['system_metrics']['cpu_usage'] == 25.5
        assert status['system_metrics']['memory_usage'] == 60.0


class TestETLScheduler:
    """Test cases for ETLScheduler."""
    
    @patch('gpt_trader.etl.ETLPipeline')
    def test_scheduler_creation(self, mock_pipeline_class, sample_config):
        """Test creating ETL scheduler."""
        scheduler = ETLScheduler(sample_config)
        
        assert scheduler.config == sample_config
        assert scheduler.is_running is False
        assert scheduler.scheduler_thread is None
    
    @patch('gpt_trader.etl.ETLPipeline')
    def test_scheduler_start_stop(self, mock_pipeline_class, sample_config):
        """Test starting and stopping scheduler."""
        mock_pipeline = Mock()
        mock_pipeline_class.return_value = mock_pipeline
        
        scheduler = ETLScheduler(sample_config)
        
        # Start scheduler
        scheduler.start()
        assert scheduler.is_running is True
        assert scheduler.scheduler_thread is not None
        mock_pipeline.start.assert_called_once()
        
        # Stop scheduler
        scheduler.stop()
        assert scheduler.is_running is False
        mock_pipeline.stop.assert_called_once()
    
    def test_should_run_daily_etl(self, sample_config):
        """Test daily ETL scheduling logic."""
        scheduler = ETLScheduler(sample_config)
        
        # Test with target hour
        target_time = datetime.utcnow().replace(hour=6, minute=0, second=0)
        should_run = scheduler._should_run_daily_etl(target_time, None)
        assert should_run is True
        
        # Test with different hour
        other_time = datetime.utcnow().replace(hour=10, minute=0, second=0)
        should_run = scheduler._should_run_daily_etl(other_time, None)
        assert should_run is False
        
        # Test with same day (already run)
        same_day_time = datetime.utcnow().replace(hour=6, minute=30, second=0)
        should_run = scheduler._should_run_daily_etl(same_day_time, target_time)
        assert should_run is False


class TestETLJobResult:
    """Test cases for ETLJobResult."""
    
    def test_job_result_creation(self):
        """Test creating job result."""
        result = ETLJobResult(
            job_id=123,
            job_uuid="test-uuid",
            success=True,
            duration_seconds=45.5,
            items_processed=100,
            items_failed=5,
            error_message=None,
            performance_metrics={"metric1": "value1"}
        )
        
        assert result.job_id == 123
        assert result.job_uuid == "test-uuid"
        assert result.success is True
        assert result.duration_seconds == 45.5
        assert result.items_processed == 100
        assert result.items_failed == 5
        assert result.error_message is None
        assert result.performance_metrics == {"metric1": "value1"}
    
    def test_job_result_failure(self):
        """Test creating job result for failure."""
        result = ETLJobResult(
            job_id=456,
            job_uuid="failed-job",
            success=False,
            duration_seconds=10.0,
            items_processed=0,
            items_failed=1,
            error_message="Processing failed"
        )
        
        assert result.success is False
        assert result.error_message == "Processing failed"
        assert result.items_processed == 0
        assert result.items_failed == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])