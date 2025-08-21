# Migration Guide - Refactored EDGAR Tools

This guide helps you migrate from the old codebase to the refactored architecture.

## Overview of Changes

The refactoring implements all the improvements identified in `Issues.md`:

1. **Eliminated global state** - Replaced with dependency injection
2. **Enhanced error handling** - Comprehensive validation and specific exceptions
3. **Improved type safety** - Complete type annotations and validation
4. **Resource management** - Connection pooling and proper cleanup
5. **Modular architecture** - Separated concerns into focused classes

## New Architecture

### Core Classes

- **`EdgarClient`** - Thread-safe HTTP client with rate limiting
- **`S3Manager`** - S3 operations with connection pooling 
- **`FilingProcessor`** - Business logic for processing filings
- **`ConfigManager`** - Type-safe configuration management
- **`URLBuilder`** - Centralized URL construction with validation

### New Modules

- `edgar/client_new.py` - Enhanced HTTP client
- `edgar/s3_manager.py` - S3 operations manager
- `edgar/filing_processor.py` - Filing processing logic
- `edgar/config_manager.py` - Configuration management
- `edgar/urls.py` - URL building and validation

### Enhanced Scripts

- `scripts/fetch_10k_new.py` - Enhanced 10-K fetcher
- `scripts/list_files_new.py` - Enhanced file lister
- `scripts/monitor_new.py` - Enhanced monitoring with async support

## Migration Steps

### Step 1: Update Imports

**Old:**
```python
from edgar.client import sec_get, get_submissions
from edgar.s3util import upload_bytes_to_s3
from edgar.config import get_config
```

**New:**
```python
from edgar.client_new import EdgarClient, ClientConfig
from edgar.s3_manager import S3Manager
from edgar.config_manager import ConfigManager
from edgar.urls import URLBuilder
```

### Step 2: Replace Global Functions with Classes

**Old:**
```python
# Global function with hidden state
response = sec_get("https://example.com")
```

**New:**
```python
# Explicit dependency injection
config = ClientConfig(rate_limit_per_sec=6.0)
with EdgarClient(config) as client:
    response = client.get("https://example.com")
```

### Step 3: Update Configuration Usage

**Old:**
```python
from edgar.config import get_config
cfg = get_config()
rate = cfg.get("rate_limit_per_sec", 6)
```

**New:**
```python
from edgar.config_manager import ConfigManager
manager = ConfigManager()
config = manager.get_config()
rate = config.rate_limit_per_sec
```

### Step 4: Update S3 Operations

**Old:**
```python
from edgar.s3util import upload_bytes_to_s3
upload_bytes_to_s3(data, bucket, key)
```

**New:**
```python
from edgar.s3_manager import S3Manager
with S3Manager() as s3:
    s3.upload_bytes(data, bucket, key)
```

### Step 5: Use URL Builders

**Old:**
```python
# Manual URL construction
acc_no_nodash = accession.replace('-', '')
url = f"{SEC_ARCHIVES}/edgar/data/{int(cik):d}/{acc_no_nodash}/{doc}"
```

**New:**
```python
from edgar.urls import URLBuilder
url = URLBuilder.document_url(cik, accession, doc)
```

## Error Handling Improvements

### New Exception Types

- **`CIKValidationError`** - Invalid CIK format
- **`AccessionValidationError`** - Invalid accession number format
- **`S3CredentialsError`** - AWS credentials issues
- **`S3AccessError`** - S3 permission issues
- **`S3NotFoundError`** - S3 resource not found

### Example Error Handling

```python
from edgar.urls import validate_cik, CIKValidationError
from edgar.s3_manager import S3Manager, S3CredentialsError

try:
    validate_cik(user_input)
    with S3Manager() as s3:
        s3.upload_bytes(data, bucket, key)
        
except CIKValidationError as e:
    print(f"Invalid CIK: {e}")
except S3CredentialsError as e:
    print(f"AWS credentials error: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
```

## Configuration Changes

### Environment Variables

New environment variables are supported:

```bash
# Client settings
export EDGAR_RATE_LIMIT=6.0
export EDGAR_TIMEOUT=30
export EDGAR_MAX_RETRIES=3

# S3 settings  
export EDGAR_S3_REGION=us-east-1
export EDGAR_S3_PREFIX=edgar-data

# Processing settings
export EDGAR_NUM_WORKERS=6
export EDGAR_FORM_TYPES="10-K,10-Q"

# Logging
export EDGAR_LOG_LEVEL=INFO
```

### Configuration Files

Configuration files now support full validation:

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

## Testing Your Migration

### Run the Test Suite

```bash
python -m pytest tests/test_refactored_components.py -v
```

### Test Scripts

```bash
# Test new scripts
python scripts/fetch_10k_new.py 0000320193
python scripts/list_files_new.py 0000320193 --json
python scripts/monitor_new.py 0000320193 --bucket test-bucket --dry-run
```

### Validate Configuration

```python
from edgar.config_manager import ConfigManager

manager = ConfigManager()
config = manager.load_config("your_config.json")
print(f"Loaded config: {config.to_dict()}")
```

## Performance Improvements

The refactored code provides several performance benefits:

1. **Connection Pooling** - HTTP and S3 connections are reused
2. **Better Concurrency** - Async processing with proper rate limiting
3. **Resource Management** - Automatic cleanup prevents memory leaks
4. **Caching** - Configuration is cached after first load

## Backward Compatibility

Legacy functions are maintained for backward compatibility:

- `edgar.client.sec_get()` - Still works but deprecated
- `edgar.s3util.upload_bytes_to_s3()` - Still works but deprecated
- `edgar.config.get_config()` - Still works but deprecated

However, **new code should use the refactored classes** for better:
- Error handling
- Type safety
- Testing capability
- Resource management

## Common Issues

### 1. Import Errors

If you get import errors, make sure you're importing from the correct modules:

```python
# OLD - may cause issues
from edgar.client import sec_get

# NEW - recommended
from edgar.client_new import EdgarClient
```

### 2. Configuration Issues

The new configuration system is stricter:

```python
# This will raise ValidationError if invalid
config = EdgarConfig(rate_limit_per_sec=-1)  # Error!

# Handle validation errors
try:
    config = EdgarConfig.from_dict(user_data)
except ValueError as e:
    print(f"Invalid configuration: {e}")
```

### 3. S3 Credential Issues

The new S3Manager validates credentials on initialization:

```python
try:
    s3 = S3Manager()
except S3CredentialsError as e:
    print("Please configure AWS credentials")
    print("Run: aws configure")
```

## Getting Help

1. **Check the test file** - `tests/test_refactored_components.py` shows usage examples
2. **Read the Issues.md** - Explains the reasoning behind changes
3. **Use type hints** - IDEs will show better autocomplete and error detection
4. **Enable logging** - Set `LOG_LEVEL=DEBUG` for detailed information

## Next Steps

1. Start with the new scripts to see the refactored architecture in action
2. Gradually migrate your existing code using this guide
3. Add comprehensive error handling using the new exception types
4. Take advantage of the improved type safety and IDE support