"""ETL Pipeline Orchestration System for GPT Trader.

This package provides a comprehensive orchestration framework for managing
ETL workflows that process SEC filings for multiple tickers while respecting
rate limits and ensuring high performance.

Core Components:
    - OrchestrationManager: Main coordinator for ETL workflows
    - JobScheduler: Task scheduling and queue management
    - ProgressTracker: Real-time progress monitoring and reporting
    - ConfigManager: Configuration management for orchestration settings
    - PerformanceMonitor: SLA compliance and performance tracking
    - ResourceManager: Rate limiting and resource throttling
    - CLIRunner: Command-line interface for job execution
    - Dashboard: Web-based monitoring interface

Key Features:
    - Process 50 tickers within 30 minutes SLA
    - SEC rate limit compliance (6 requests/second)
    - Concurrent worker pool management
    - Real-time progress tracking
    - Performance monitoring and alerting
    - Configurable retry strategies
    - Resource throttling and backpressure
"""

from .orchestrator import OrchestrationManager, OrchestrationConfig
from .scheduler import JobScheduler, JobConfig, JobStatus, ScheduleType
from .progress import ProgressTracker, JobProgress, ProgressStatus
from .performance import PerformanceMonitor, SLAMetrics, PerformanceReport
from .resource_manager import ResourceManager, ResourceConfig, RateLimiter
from .cli import CLIRunner, CLIConfig
from .dashboard import DashboardService, DashboardConfig

__version__ = "1.0.0"

__all__ = [
    # Core orchestration
    "OrchestrationManager",
    "OrchestrationConfig",
    
    # Job scheduling
    "JobScheduler", 
    "JobConfig",
    "JobStatus",
    "ScheduleType",
    
    # Progress tracking
    "ProgressTracker",
    "JobProgress", 
    "ProgressStatus",
    
    # Performance monitoring
    "PerformanceMonitor",
    "SLAMetrics",
    "PerformanceReport",
    
    # Resource management
    "ResourceManager",
    "ResourceConfig", 
    "RateLimiter",
    
    # CLI interface
    "CLIRunner",
    "CLIConfig",
    
    # Dashboard
    "DashboardService",
    "DashboardConfig",
]