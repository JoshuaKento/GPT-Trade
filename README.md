# EDGAR Tools - Modern SEC Filing Toolkit

A comprehensive Python toolkit for downloading and monitoring SEC EDGAR filings with modern architecture, enhanced error handling, and production-ready features.

## ✨ Features

- **🏗️ Modern Architecture**: Dependency injection, type safety, and modular design
- **⚡ High Performance**: Connection pooling, async processing, and resource management
- **🛡️ Robust Error Handling**: Comprehensive validation and specific exception types
- **🔄 Real-time Monitoring**: Track new filings with S3 upload and manifest management
- **📊 Multiple Output Formats**: JSON, CSV, and table outputs for filing data
- **🧪 Production Ready**: Comprehensive testing and enterprise-grade reliability

## 🚀 Quick Start

### Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Install as editable package (recommended)
pip install -e .
```

### Basic Usage

```bash
# Download Apple's latest 10-K filing
python scripts/fetch_10k_new.py 0000320193

# List files in latest 10-K with JSON output
python scripts/list_files_new.py 0000320193 --json --output files.json

# Monitor for new filings with S3 upload
python scripts/monitor_new.py 0000320193 --bucket my-bucket --async
```

## 📋 Requirements

- **Python 3.11+** (managed via pyenv with `.python-version` file)
- **Core Dependencies**: `requests`, `beautifulsoup4`, `boto3`, `aiohttp`, `tqdm`
- **Testing**: `pytest`

### Environment Setup

```bash
# Using pyenv (recommended)
pyenv install 3.11.12
pyenv virtualenv 3.11.12 edgar-tools
pyenv local edgar-tools
pip install -r requirements.txt
```

For detailed setup instructions, see [PYENV_SETUP.md](PYENV_SETUP.md).

## 🎯 Enhanced Scripts (Recommended)

### fetch_10k_new.py - Enhanced 10-K Fetcher
```bash
# Basic usage
python scripts/fetch_10k_new.py 0000320193

# With custom directory and verbose logging
python scripts/fetch_10k_new.py 0000320193 --dir apple_10k --verbose

# Using custom configuration
python scripts/fetch_10k_new.py 0000320193 --config myconfig.json
```

### list_files_new.py - Enhanced File Lister
```bash
# Table output (default)
python scripts/list_files_new.py 0000320193

# JSON output to file
python scripts/list_files_new.py 0000320193 --json --output files.json

# CSV format
python scripts/list_files_new.py 0000320193 --csv --output data.csv

# Different form types
python scripts/list_files_new.py 0000320193 --form 10-Q
```

### monitor_new.py - Enhanced Monitoring
```bash
# Basic monitoring with S3 upload
python scripts/monitor_new.py 0000320193 --bucket my-bucket

# Async processing with manifest tracking
python scripts/monitor_new.py 0000320193 --bucket my-bucket \
  --manifest manifests/manifest.json --async

# Dry run to see what would be processed
python scripts/monitor_new.py 0000320193 --bucket my-bucket --dry-run

# Multiple CIKs with custom configuration
python scripts/monitor_new.py 0000320193 0000789019 --bucket my-bucket \
  --config config.json --async
```

## 📦 Package Usage (New Architecture)

### Basic Example
```python
from edgar import EdgarClient, ConfigManager, S3Manager

# Load configuration
config_manager = ConfigManager()
config = config_manager.get_config()

# Use enhanced clients with resource management
with EdgarClient(config) as client, S3Manager() as s3:
    # Enhanced functionality with full error handling
    from edgar import FilingProcessor
    processor = FilingProcessor(client, s3)
    
    # Process filings with comprehensive validation
    filings = processor.get_recent_filings("0000320193")
    for filing in filings:
        if filing.form == "10-K":
            print(f"Found 10-K: {filing.accession}")
```

### URL Building and Validation
```python
from edgar.urls import URLBuilder, validate_cik

# Robust CIK validation
cik = validate_cik("320193")  # Returns "0000320193"

# Type-safe URL construction
submissions_url = URLBuilder.submissions_url(cik)
filing_url = URLBuilder.filing_index_url(cik, "0000320193-23-000006")
document_url = URLBuilder.document_url(cik, accession, "aapl-20230930.htm")
```

### Configuration Management
```python
from edgar.config_manager import ConfigManager, EdgarConfig

# Type-safe configuration
config = EdgarConfig(
    rate_limit_per_sec=8.0,
    num_workers=10,
    s3_prefix="edgar-data",
    form_types=["10-K", "10-Q"]
)

# Load from file with validation
manager = ConfigManager()
config = manager.load_config("config.json")
```

## ⚙️ Configuration

### Environment Variables
```bash
# Required
export SEC_USER_AGENT="YourApp (contact@example.com)"

# Optional enhancements
export EDGAR_RATE_LIMIT=6.0
export EDGAR_TIMEOUT=30
export EDGAR_MAX_RETRIES=3
export EDGAR_S3_REGION=us-east-1
export EDGAR_NUM_WORKERS=6
export EDGAR_FORM_TYPES="10-K,10-Q"
export EDGAR_LOG_LEVEL=INFO
```

### Configuration File (Enhanced)
```json
{
  "rate_limit_per_sec": 6.0,
  "num_workers": 6,
  "max_pool_connections": 50,
  "s3_prefix": "edgar",
  "s3_region": "us-east-1",
  "form_types": ["10-K", "10-Q"],
  "timeout": 30,
  "max_retries": 3,
  "backoff_factor": 0.5,
  "user_agent": "MyApp (contact@example.com)",
  "log_level": "INFO"
}
```

## 🧪 Testing

```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test suite
python -m pytest tests/test_refactored_components.py -v

# Test enhanced functionality
python -m pytest tests/test_refactored_components.py::TestURLValidation -v
```

## 🏢 Enterprise Features

- **🔒 Type Safety**: Complete type annotations prevent runtime errors
- **🔄 Connection Pooling**: HTTP and S3 connections are reused for performance
- **📊 Async Processing**: Handle multiple filings concurrently with proper backpressure
- **🛡️ Comprehensive Validation**: CIK, accession numbers, and documents are validated
- **📈 Resource Management**: Automatic cleanup prevents memory leaks
- **🔍 Detailed Logging**: Structured logging with configurable levels
- **⚡ Retry Logic**: Automatic retry with exponential backoff for network issues

## 📚 Documentation

- **[CLAUDE.md](CLAUDE.md)** - Development guidance and architecture overview
- **[MIGRATION.md](MIGRATION.md)** - Migration guide from legacy architecture  
- **[Issues.md](Issues.md)** - Detailed analysis of improvements implemented
- **[FINAL_MIGRATION_REPORT.md](FINAL_MIGRATION_REPORT.md)** - Complete migration summary

## 🔄 Legacy Compatibility

Legacy scripts remain available with deprecation warnings:
```bash
# These still work but show deprecation warnings
python scripts/fetch_10k.py 0000320193      # Use fetch_10k_new.py instead
python scripts/list_files.py 0000320193     # Use list_files_new.py instead
```

Legacy package functions are preserved:
```python
# Backward compatibility maintained
from edgar import cik_to_10digit, fetch_latest_10k, list_recent_filings
```

## 🚨 SEC Compliance

- **Rate Limiting**: Automatic throttling to SEC guidelines (6 requests/second default)
- **User-Agent**: Required contact email in User-Agent header
- **Respectful Access**: Built-in delays and retry logic to avoid overwhelming SEC servers

## 📝 Examples

### Monitor Multiple Companies
```bash
# Monitor Apple and Microsoft for 10-K/10-Q filings
python scripts/monitor_new.py 0000320193 0000789019 \
  --bucket edgar-filings \
  --manifest manifests/tech_companies.json \
  --config config/production.json \
  --async
```

### Batch Processing
```python
from edgar import EdgarClient, FilingProcessor, ConfigManager

config_manager = ConfigManager()
config = config_manager.get_config()

companies = ["0000320193", "0000789019", "0001018724"]  # Apple, Microsoft, Amazon

with EdgarClient(config) as client:
    processor = FilingProcessor(client, None)
    
    for cik in companies:
        try:
            filings = processor.get_recent_filings(cik)
            print(f"CIK {cik}: {len(filings)} recent filings")
        except Exception as e:
            print(f"Error processing {cik}: {e}")
```

## 🤝 Contributing

1. **Setup**: Follow the installation instructions above
2. **Testing**: Ensure all tests pass with `python -m pytest tests/ -v`
3. **Code Quality**: Use the new architecture classes for consistency
4. **Documentation**: Update relevant documentation for changes

## 📄 License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

---

```
 /\_/\
(=^.^=)
 /     \
JoshuaKent
```