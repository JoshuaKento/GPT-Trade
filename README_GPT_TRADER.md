# GPT Trader - Financial Data ETL Platform

A comprehensive platform for collecting, storing, and analyzing SEC EDGAR filings with advanced ETL capabilities, database persistence, and performance monitoring.

## 🚀 Features

### Phase 2 - Database & ETL Platform (v0.1)

- **Database Integration**: PostgreSQL/SQLite support with SQLAlchemy ORM
- **Multi-Ticker Processing**: Batch processing for up to 50+ tickers
- **ETL Pipeline**: Orchestrated workflows with job scheduling and monitoring
- **Performance Monitoring**: SLA compliance tracking and alerting
- **Comprehensive CLI**: Complete command-line interface for all operations
- **Production Ready**: Enterprise-grade error handling, logging, and recovery

### Core Capabilities

- **SEC EDGAR Integration**: Download and process 10-K, 10-Q, 8-K filings
- **S3 Storage**: Automatic upload and manifest management
- **Database Persistence**: Store filing metadata and processing states
- **Job Orchestration**: Multi-threaded processing with priority queues
- **Progress Tracking**: Real-time monitoring and status reporting
- **Error Recovery**: Automatic retry mechanisms and failure handling

## 📋 Requirements

- Python 3.9+
- PostgreSQL 12+ (or SQLite for development)
- AWS S3 access (optional)
- 2GB+ RAM for processing 50 tickers
- Valid email for SEC user-agent compliance

## 🛠 Installation

### 1. Clone Repository
```bash
git clone https://github.com/JoshuaKento/GPT-Trade.git
cd GPT-Trade
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt  # For development
```

### 3. Environment Setup
```bash
# Copy sample environment
cp .env.example .env

# Edit configuration
export SEC_USER_AGENT="YourApp (your-email@example.com)"
export DATABASE_URL="postgresql://user:pass@localhost/gpt_trader"
export S3_BUCKET="your-s3-bucket"
```

### 4. Initialize Database
```bash
python scripts/gpt_trader_cli.py init-db
```

## ⚙️ Configuration

### Create Configuration File
```bash
python scripts/gpt_trader_cli.py config create-sample
```

This creates `gpt_trader_config.json` with sample tickers:

```json
{
  "edgar": {
    "rate_limit_per_sec": 6.0,
    "user_agent": "GPT-Trader (your-email@example.com)",
    "num_workers": 4
  },
  "database": {
    "url": "postgresql://localhost/gpt_trader",
    "pool_size": 20
  },
  "etl": {
    "batch_size": 50,
    "max_concurrent_jobs": 10,
    "sla_processing_time_minutes": 30
  },
  "s3": {
    "bucket_name": "gpt-trader-data",
    "region": "us-east-1"
  },
  "tickers": [
    {
      "ticker": "AAPL",
      "cik": "0000320193",
      "priority": 1,
      "forms_to_process": ["10-K", "10-Q", "8-K"]
    }
  ]
}
```

### Add Tickers
```bash
python scripts/gpt_trader_cli.py add-ticker NVDA 0000320193 --priority 1
python scripts/gpt_trader_cli.py add-ticker TSLA 0001318605 --priority 2
```

## 🚀 Quick Start

### 1. Initialize System
```bash
# Initialize database
python scripts/gpt_trader_cli.py init-db

# Create sample configuration
python scripts/gpt_trader_cli.py config create-sample

# Sync companies to database
python scripts/gpt_trader_cli.py sync-companies
```

### 2. Run ETL Process
```bash
# Process single ticker
python scripts/gpt_trader_cli.py run-etl --ticker AAPL

# Process all active tickers
python scripts/gpt_trader_cli.py run-etl

# Run with custom parameters
python scripts/gpt_trader_cli.py run-etl --max-filings 100 --job-name "monthly_update"
```

### 3. Monitor Progress
```bash
# List recent jobs
python scripts/gpt_trader_cli.py list-jobs --limit 10

# Get job details
python scripts/gpt_trader_cli.py job-status 123

# Start monitoring dashboard
python scripts/gpt_trader_cli.py monitor
```

### 4. Scheduled Processing
```bash
# Start ETL scheduler (runs daily at 6 AM UTC)
python scripts/gpt_trader_cli.py start-scheduler
```

## 📊 Usage Examples

### Programmatic Usage

```python
from gpt_trader import GPTTraderConfig, ETLPipeline, BatchFilingProcessor

# Load configuration
config = GPTTraderConfig.from_file("gpt_trader_config.json")

# Create batch processor
processor = BatchFilingProcessor(config)

# Process all active tickers
job = processor.process_all_active_tickers("daily_etl")
print(f"Job completed: {job.status}")

# Use ETL pipeline for advanced orchestration
pipeline = ETLPipeline(config)
pipeline.start()

# Submit ticker for processing
task_id = pipeline.submit_ticker_task(
    ticker_config=config.get_ticker_by_symbol("AAPL"),
    parameters={"max_filings": 50}
)

# Run daily ETL with SLA monitoring
result = pipeline.run_daily_etl()
print(f"SLA met: {result.performance_metrics['sla_met']}")

pipeline.stop()
```

### Database Queries

```python
from gpt_trader.database import session_scope
from gpt_trader.models import Company, Filing, Document

# Query companies and filings
with session_scope() as session:
    # Get Apple's recent filings
    apple = Company.get_by_cik(session, "0000320193")
    recent_filings = session.query(Filing).filter(
        Filing.company_id == apple.id,
        Filing.form_type == "10-K"
    ).order_by(Filing.filing_date.desc()).limit(5).all()
    
    # Get processing statistics
    pending_count = session.query(Filing).filter(
        Filing.is_processed == False
    ).count()
    
    print(f"Apple has {len(recent_filings)} recent 10-K filings")
    print(f"{pending_count} filings pending processing")
```

## 🔧 CLI Reference

### Database Commands
```bash
gpt_trader_cli.py init-db [--reset]           # Initialize database
gpt_trader_cli.py db-status                   # Show database status
```

### Configuration Commands
```bash
gpt_trader_cli.py config show                 # Show current config
gpt_trader_cli.py config create-sample        # Create sample config
gpt_trader_cli.py add-ticker SYMBOL CIK       # Add ticker
gpt_trader_cli.py sync-companies              # Sync to database
```

### ETL Commands
```bash
gpt_trader_cli.py run-etl [--ticker SYMBOL]   # Run ETL process
gpt_trader_cli.py start-scheduler             # Start scheduler daemon
```

### Job Management
```bash
gpt_trader_cli.py list-jobs [--status STATUS] # List jobs
gpt_trader_cli.py job-status JOB_ID           # Job details
gpt_trader_cli.py monitor [--refresh SECONDS] # Monitoring dashboard
```

## 📈 Performance & SLA

### Default SLA Targets
- **Processing Time**: ≤ 30 minutes for 50 tickers
- **Success Rate**: ≥ 95% of filings processed successfully
- **Memory Usage**: < 2GB peak usage
- **Error Rate**: < 10% failure rate

### Performance Optimizations
- **Connection Pooling**: 20 connections, 30 max overflow
- **Parallel Processing**: Up to 10 concurrent jobs
- **Adaptive Rate Limiting**: Auto-adjusts to SEC response times
- **Streaming Downloads**: Memory-efficient large file handling
- **Multipart S3 Uploads**: Concurrent uploads for large files

### Monitoring Metrics
- Job completion rates and durations
- System resource usage (CPU, memory, disk)
- SEC API response times and rate limiting
- Database connection health
- S3 upload success rates

## 🔐 Security Features

- **Input Validation**: Comprehensive path traversal protection
- **Credential Management**: Secure AWS credential handling
- **Rate Limiting**: SEC compliance with respectful access patterns
- **Error Handling**: Secure error messages without data leakage
- **Dependency Security**: Pinned versions with security scanning

## 🧪 Testing

### Run Tests
```bash
# Run all tests
pytest

# Run specific test categories
pytest tests/test_gpt_trader_models.py  # Database models
pytest tests/test_gpt_trader_etl.py     # ETL pipeline

# Run with coverage
pytest --cov=gpt_trader --cov-report=html
```

### Test Configuration
- SQLite in-memory databases for model tests
- Mocked external dependencies (SEC API, S3)
- Performance and SLA compliance tests
- Integration tests with test containers

## 📁 Project Structure

```
gpt_trader/
├── __init__.py              # Main package exports
├── models.py                # SQLAlchemy database models
├── database.py              # Database connection management
├── config.py                # Configuration management
├── filing_processor_db.py   # Database-enabled filing processor
├── etl.py                  # ETL pipeline orchestration
└── monitoring.py           # Performance monitoring & SLA

scripts/
└── gpt_trader_cli.py       # Command-line interface

tests/
├── test_gpt_trader_models.py # Database model tests
└── test_gpt_trader_etl.py    # ETL pipeline tests
```

## 🚀 Future Roadmap

### Phase 3 - Analytics & RAG (Month 3)
- Vector embeddings with pgvector
- RAG-powered report generation
- Sentiment analysis integration
- Streamlit dashboard

### Phase 4 - Trading Signals (Month 6)
- Backtesting framework integration
- Signal generation algorithms
- Portfolio optimization
- Live trading API connections

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Run tests (`pytest`)
4. Commit changes (`git commit -m 'Add amazing feature'`)
5. Push to branch (`git push origin feature/amazing-feature`)
6. Open Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 📧 Support

- **Issues**: [GitHub Issues](https://github.com/JoshuaKento/GPT-Trade/issues)
- **Discussions**: [GitHub Discussions](https://github.com/JoshuaKento/GPT-Trade/discussions)
- **Email**: support@gpt-trader.com

## ⚠️ Disclaimer

This software is for educational and research purposes only. Not financial advice. Always comply with SEC regulations and respect API rate limits.