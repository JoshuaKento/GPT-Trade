# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an EDGAR 10-K fetcher - a Python toolkit for downloading and monitoring SEC filings. The project has been **fully refactored** with improved architecture, comprehensive error handling, and modern Python practices. It exposes both a package (`edgar/`) and command-line tools (`scripts/`) for working with SEC EDGAR data.

## Refactored Architecture (CURRENT)

### New Core Classes (`edgar/`)
- **`client_new.py`** - `EdgarClient` class with thread-safe rate limiting and connection pooling
- **`s3_manager.py`** - `S3Manager` class with resource pooling and comprehensive error handling
- **`filing_processor.py`** - `FilingProcessor` class for business logic separation
- **`config_manager.py`** - `ConfigManager` and `EdgarConfig` classes with validation
- **`urls.py`** - `URLBuilder` class and validation functions for all SEC endpoints
- **`parser.py`** - Enhanced HTML parsing with error handling (updated)
- **`state.py`** - Enhanced state management with validation (updated)

### Enhanced Scripts (`scripts/`)
- **`fetch_10k_new.py`** - Enhanced 10-K fetcher with comprehensive error handling
- **`list_files_new.py`** - Enhanced file lister with multiple output formats
- **`monitor_new.py`** - Enhanced monitoring with async/sync modes and dry-run support

### Legacy Code (DEPRECATED)
- `client.py`, `s3util.py`, `config.py` - Original implementations (kept for compatibility)
- `fetch_10k.py`, `list_files.py`, `monitor.py` - Original scripts (use new versions)

## Key Architectural Improvements

1. **Eliminated Global State** - All components use dependency injection
2. **Enhanced Error Handling** - Specific exception types with clear messages
3. **Complete Type Safety** - Full type annotations and validation
4. **Resource Management** - Connection pooling and proper cleanup
5. **Modular Design** - Separated concerns with clear interfaces

## Development Commands

### Setup
```bash
# Install dependencies (includes pytest)
pip install -r requirements.txt

# Or install as editable package
pip install -e .
```

### Testing
```bash
# Run all tests including refactored components
python -m pytest tests/ -v

# Run specific refactored component tests
python -m pytest tests/test_refactored_components.py -v

# Test original parser functionality
python -m pytest tests/test_parser.py -v
```

### Usage Examples (NEW SCRIPTS)
```bash
# Enhanced 10-K fetcher with validation and error handling
python scripts/fetch_10k_new.py 0000320193 --verbose

# Enhanced file lister with multiple output formats
python scripts/list_files_new.py 0000320193 --json --output files.json

# Enhanced monitor with async processing and dry-run
python scripts/monitor_new.py 0000320193 --bucket my-bucket --async --dry-run
```

## Configuration Management

### Environment Variables (Enhanced)
```bash
# Core settings
export SEC_USER_AGENT="YourApp (contact@example.com)"  # REQUIRED
export EDGAR_RATE_LIMIT=6.0
export EDGAR_TIMEOUT=30
export EDGAR_MAX_RETRIES=3

# S3 settings
export EDGAR_S3_REGION=us-east-1
export EDGAR_S3_PREFIX=edgar-data

# Processing settings
export EDGAR_NUM_WORKERS=6
export EDGAR_FORM_TYPES="10-K,10-Q"
export EDGAR_LOG_LEVEL=INFO
```

### Configuration Files (Enhanced)
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

## Error Handling

### New Exception Types
- `CIKValidationError` - Invalid CIK format
- `AccessionValidationError` - Invalid accession number format  
- `S3CredentialsError` - AWS credentials issues
- `S3AccessError` - S3 permission issues
- `S3NotFoundError` - S3 resource not found

### Validation Functions
```python
from edgar.urls import validate_cik, validate_accession_number
from edgar.urls import URLBuilder

# All inputs are validated
cik = validate_cik("320193")  # Returns "0000320193"
url = URLBuilder.submissions_url(cik)  # Builds validated URL
```

## Modern Usage Patterns

### Using the New Architecture
```python
from edgar.client_new import EdgarClient, ClientConfig
from edgar.s3_manager import S3Manager
from edgar.filing_processor import FilingProcessor
from edgar.config_manager import ConfigManager

# Load configuration
config_manager = ConfigManager()
config = config_manager.get_config()

# Create client with explicit configuration
client_config = ClientConfig(
    rate_limit_per_sec=config.rate_limit_per_sec,
    user_agent=config.user_agent
)

# Use context managers for resource cleanup
with EdgarClient(client_config) as client:
    with S3Manager() as s3:
        processor = FilingProcessor(client, s3)
        # Process filings...
```

### Dependency Injection Pattern
```python
# Old: Hidden global state
response = sec_get(url)  # Rate limiting hidden

# New: Explicit dependencies
with EdgarClient(config) as client:
    response = client.get(url)  # Rate limiting explicit
```

## Dependencies

- **Python 3.11+** (managed via pyenv with `.python-version` file)
- **Core**: `requests`, `beautifulsoup4`, `boto3`, `aiohttp`, `tqdm`
- **Enhanced**: `urllib3` (for retry logic), `pathlib` (for path handling)
- **Testing**: `pytest`

## Important Files

- **`MIGRATION.md`** - Complete guide for migrating to the new architecture
- **`Issues.md`** - Detailed analysis of problems and solutions implemented
- **`tests/test_refactored_components.py`** - Comprehensive tests for new components

## Rate Limiting & Performance

- Thread-safe rate limiting (6 req/sec default, configurable)
- HTTP connection pooling for better performance
- S3 connection pooling with configurable pool size
- Async processing support with proper backpressure
- Resource cleanup with context managers

## Migration Note

**Use the new architecture for all new development.** The old modules are maintained for backward compatibility but are deprecated. See `MIGRATION.md` for a complete migration guide with examples.