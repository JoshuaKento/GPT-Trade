"""
GPT Trader - Financial Data ETL and Analysis Platform

A comprehensive toolkit for collecting, storing, and analyzing corporate filings,
market data, and social signals to generate investor-ready reports and trading signals.
"""

__version__ = "0.1.0"
__author__ = "GPT Trader Team"

from .database import DatabaseManager, get_session
from .models import Company, Filing, Document, ProcessingJob
from .etl import ETLPipeline
from .config import GPTTraderConfig

__all__ = [
    "DatabaseManager",
    "get_session", 
    "Company",
    "Filing",
    "Document",
    "ProcessingJob",
    "ETLPipeline",
    "GPTTraderConfig",
]