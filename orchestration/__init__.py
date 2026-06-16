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

Key Features:
    - Process 50 tickers within 30 minutes SLA
    - SEC rate limit compliance (6 requests/second)
    - Concurrent worker pool management
    - Real-time progress tracking
    - Performance monitoring and alerting
    - Configurable retry strategies
    - Resource throttling and backpressure
"""

from importlib import import_module
from typing import Any

__version__ = "1.0.0"

_EXPORTS = {
    "OrchestrationManager": ".orchestrator",
    "OrchestrationConfig": ".orchestrator",
    "JobScheduler": ".scheduler",
    "JobConfig": ".scheduler",
    "JobStatus": ".scheduler",
    "ScheduleType": ".scheduler",
    "ProgressTracker": ".progress",
    "JobProgress": ".progress",
    "ProgressStatus": ".progress",
    "PerformanceMonitor": ".performance",
    "SLAMetrics": ".performance",
    "PerformanceReport": ".performance",
    "ResourceManager": ".resource_manager",
    "ResourceConfig": ".resource_manager",
    "RateLimiter": ".resource_manager",
    "CLIRunner": ".cli",
    "CLIConfig": ".cli",
}

__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    """Lazily load public exports from their implementation modules."""
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module = import_module(_EXPORTS[name], __name__)
    value = getattr(module, name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted([*globals(), *_EXPORTS])
