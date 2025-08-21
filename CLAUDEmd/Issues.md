# Code Refactoring Analysis - Issues and Improvements

## 1. Identified Issues

### High Priority Issues
- **Global state in `client.py`**: Global variables `_last_sec_request` and `_rate_lock` create thread-safety concerns and testing difficulties
- **Large monolithic function**: `monitor_cik()` in `monitor.py` (65+ lines) handles too many responsibilities
- **Missing error handling**: CIK validation, network timeouts, and S3 credential validation
- **Type safety gaps**: Missing return type annotations, `Dict` without generics
- **Resource management**: S3 client creation on every call without connection pooling

### Medium Priority Issues
- **Code duplication**: URL construction patterns repeated across files
- **Configuration anti-pattern**: Global singleton config with mutable state
- **String formatting inconsistency**: Mix of f-strings, `.format()`, and `%` formatting
- **Path manipulation**: Manual path joins instead of `pathlib`

### Low Priority Issues
- **Import organization**: sys.path manipulation in scripts
- **Naming conventions**: Some unclear variable names (`acc_no_nodash`)

## 2. Refactoring Strategy (Prioritized)

### Priority 1: Critical Architecture Issues

1. **Extract Rate Limiter Class** (High Impact, Medium Effort)
   - Remove global state from `client.py`
   - Create proper rate limiter with dependency injection

2. **Break Down Large Functions** (High Impact, Medium Effort)
   - Split `monitor_cik()` into smaller, focused functions
   - Extract file processing logic

### Priority 2: Error Handling & Safety

3. **Add Comprehensive Error Handling** (High Impact, Low Effort)
   - CIK validation with proper error messages
   - Network timeout and retry logic
   - S3 credential validation

4. **Improve Type Safety** (Medium Impact, Low Effort)
   - Add missing type annotations
   - Use proper generic types

### Priority 3: Code Quality Improvements

5. **Resource Management** (Medium Impact, Medium Effort)
   - S3 client connection pooling
   - Context managers for file operations

6. **Eliminate Code Duplication** (Medium Impact, Low Effort)
   - Extract URL builders
   - Centralize common patterns

## 3. Refactored Code Examples

### Example 1: Refactored Rate Limiter & Client

**Before:**
```python
# edgar/client.py - Problems: Global state, testing difficulties
_last_sec_request = 0.0
_rate_lock = threading.Lock()

def sec_get(url: str, **kwargs) -> requests.Response:
    cfg = get_config()
    interval = 1 / float(cfg.get("rate_limit_per_sec", 6))
    global _last_sec_request
    with _rate_lock:
        wait = interval - (time.time() - _last_sec_request)
        if wait > 0:
            time.sleep(wait)
        _last_sec_request = time.time()
    resp = requests.get(url, headers=HEADERS, **kwargs)
    return resp
```

**After:**
```python
# edgar/client.py - Clean, testable, injectable
from typing import Dict, Optional
import time
import threading
import requests
from dataclasses import dataclass

@dataclass
class ClientConfig:
    rate_limit_per_sec: float = 6.0
    timeout: int = 30
    user_agent: str = "GPT-Trade-Agent (your-email@example.com)"

class EdgarClient:
    """Thread-safe SEC EDGAR API client with rate limiting."""
    
    def __init__(self, config: Optional[ClientConfig] = None):
        self.config = config or ClientConfig()
        self._last_request = 0.0
        self._lock = threading.Lock()
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": self.config.user_agent})
    
    def get(self, url: str, **kwargs) -> requests.Response:
        """GET request with automatic rate limiting."""
        with self._lock:
            interval = 1.0 / self.config.rate_limit_per_sec
            wait_time = interval - (time.time() - self._last_request)
            if wait_time > 0:
                time.sleep(wait_time)
            self._last_request = time.time()
        
        kwargs.setdefault('timeout', self.config.timeout)
        response = self._session.get(url, **kwargs)
        response.raise_for_status()
        return response
    
    def close(self):
        """Clean up session resources."""
        self._session.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

# Usage with dependency injection
def get_submissions(cik: str, client: EdgarClient) -> Dict[str, Any]:
    """Fetch submissions JSON for a CIK."""
    cik10 = cik_to_10digit(cik)
    url = f"{SEC_BASE}/submissions/CIK{cik10}.json"
    resp = client.get(url)
    return resp.json()
```

### Example 2: Refactored URL Builder & Validation

**Before:**
```python
# Scattered across multiple files
acc_no_nodash = accession.replace('-', '')
url = f"{SEC_ARCHIVES}/edgar/data/{int(cik):d}/{acc_no_nodash}/{doc}"
```

**After:**
```python
# edgar/urls.py - Centralized URL construction
from typing import Optional
import re

class CIKValidationError(ValueError):
    """Raised when CIK format is invalid."""
    pass

def validate_cik(cik: str) -> str:
    """Validate and normalize CIK format."""
    if not cik or not cik.strip():
        raise CIKValidationError("CIK cannot be empty")
    
    # Remove leading zeros and validate numeric
    try:
        cik_int = int(cik.strip())
        if cik_int < 0:
            raise CIKValidationError("CIK must be positive")
    except ValueError:
        raise CIKValidationError(f"CIK must be numeric: {cik}")
    
    return f"{cik_int:010d}"

def validate_accession_number(accession: str) -> str:
    """Validate SEC accession number format."""
    pattern = r'^\d{10}-\d{2}-\d{6}$'
    if not re.match(pattern, accession):
        raise ValueError(f"Invalid accession number format: {accession}")
    return accession

class URLBuilder:
    """Centralized URL construction for SEC EDGAR endpoints."""
    
    SEC_BASE = "https://data.sec.gov"
    SEC_ARCHIVES = "https://www.sec.gov/Archives"
    
    @classmethod
    def submissions_url(cls, cik: str) -> str:
        """Build submissions JSON URL."""
        cik10 = validate_cik(cik)
        return f"{cls.SEC_BASE}/submissions/CIK{cik10}.json"
    
    @classmethod
    def filing_index_url(cls, cik: str, accession: str) -> str:
        """Build filing index HTML URL."""
        cik10 = validate_cik(cik)
        accession = validate_accession_number(accession)
        acc_no_nodash = accession.replace('-', '')
        return f"{cls.SEC_ARCHIVES}/edgar/data/{int(cik10)}/{acc_no_nodash}/{accession}-index.html"
    
    @classmethod
    def document_url(cls, cik: str, accession: str, document: str) -> str:
        """Build document download URL."""
        cik10 = validate_cik(cik)
        accession = validate_accession_number(accession)
        acc_no_nodash = accession.replace('-', '')
        return f"{cls.SEC_ARCHIVES}/edgar/data/{int(cik10)}/{acc_no_nodash}/{document}"
```

### Example 3: Refactored Monitor Function

**Before:**
```python
# monitor.py - 65+ line monolithic function
async def monitor_cik(session, limiter, sem, cik, bucket, prefix, state, manifest, form_types):
    # ... massive function with multiple responsibilities
```

**After:**
```python
# edgar/filing_processor.py - Separated concerns
from dataclasses import dataclass
from typing import List, Dict, Set, Optional
import asyncio
import logging

@dataclass
class ProcessingResult:
    """Result of processing a single filing."""
    accession: str
    success: bool
    documents_processed: int
    error: Optional[str] = None

class FilingProcessor:
    """Handles processing of SEC filings for a single CIK."""
    
    def __init__(
        self,
        client: EdgarClient,
        s3_uploader: S3Uploader,
        logger: logging.Logger
    ):
        self.client = client
        self.s3_uploader = s3_uploader
        self.logger = logger
    
    async def get_new_filings(
        self, 
        cik: str, 
        processed_accessions: Set[str],
        form_types: Optional[List[str]] = None
    ) -> List[Dict[str, str]]:
        """Get filings that haven't been processed yet."""
        all_filings = await self._fetch_recent_filings(cik)
        
        # Filter by form type if specified
        if form_types:
            forms_set = {f.upper() for f in form_types}
            all_filings = [
                f for f in all_filings 
                if f["form"].upper() in forms_set
            ]
        
        # Filter out already processed
        new_filings = [
            f for f in all_filings 
            if f["accession"] not in processed_accessions
        ]
        
        self.logger.info(
            f"CIK {cik}: {len(all_filings)} total, {len(new_filings)} new filings"
        )
        return new_filings
    
    async def process_filing(
        self, 
        cik: str, 
        filing: Dict[str, str],
        bucket: str,
        prefix: str
    ) -> ProcessingResult:
        """Process a single filing - get files and upload to S3."""
        accession = filing["accession"]
        
        try:
            # Get list of documents in this filing
            documents = await self._get_filing_documents(cik, accession)
            if not documents:
                return ProcessingResult(
                    accession=accession,
                    success=False,
                    documents_processed=0,
                    error="No documents found"
                )
            
            # Upload all documents
            uploaded_count = await self._upload_documents(
                cik, accession, documents, bucket, prefix
            )
            
            return ProcessingResult(
                accession=accession,
                success=True,
                documents_processed=uploaded_count
            )
            
        except Exception as e:
            self.logger.error(f"Failed to process {accession}: {e}")
            return ProcessingResult(
                accession=accession,
                success=False,
                documents_processed=0,
                error=str(e)
            )

# Simplified monitor function
async def monitor_cik(
    processor: FilingProcessor,
    cik: str,
    bucket: str,
    prefix: str,
    state: Dict[str, Set[str]],
    form_types: Optional[List[str]] = None
) -> None:
    """Monitor a single CIK for new filings."""
    logger = logging.getLogger("monitor")
    logger.info(f"Checking CIK {cik}")
    
    processed = state.setdefault(cik, set())
    new_filings = await processor.get_new_filings(cik, processed, form_types)
    
    if not new_filings:
        logger.info(f"No new filings for {cik}")
        return
    
    # Process all filings
    results = []
    for filing in new_filings:
        result = await processor.process_filing(cik, filing, bucket, prefix)
        results.append(result)
        
        if result.success:
            processed.add(result.accession)
            logger.info(f"Processed {result.accession}: {result.documents_processed} documents")
        else:
            logger.error(f"Failed {result.accession}: {result.error}")
    
    successful = sum(1 for r in results if r.success)
    total_docs = sum(r.documents_processed for r in results)
    logger.info(f"CIK {cik}: {successful}/{len(results)} filings, {total_docs} documents")
```

### Example 4: Improved S3 Resource Management

**Before:**
```python
# edgar/s3util.py - Creates client on every call
def upload_bytes_to_s3(data: bytes, bucket: str, key: str) -> None:
    s3 = boto3.client("s3")  # New client every time!
    s3.put_object(Bucket=bucket, Key=key, Body=data)
```

**After:**
```python
# edgar/s3_manager.py - Resource pooling and error handling
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from typing import List, Dict, Optional
import json
import logging

class S3Manager:
    """Manages S3 operations with connection pooling and error handling."""
    
    def __init__(self, region_name: Optional[str] = None):
        try:
            self._client = boto3.client('s3', region_name=region_name)
            self.logger = logging.getLogger('s3_manager')
        except NoCredentialsError:
            raise ValueError("AWS credentials not found. Please configure AWS credentials.")
    
    def upload_bytes(self, data: bytes, bucket: str, key: str) -> None:
        """Upload bytes to S3 with error handling."""
        try:
            self._client.put_object(Bucket=bucket, Key=key, Body=data)
            self.logger.debug(f"Uploaded {len(data)} bytes to s3://{bucket}/{key}")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket':
                raise ValueError(f"S3 bucket '{bucket}' does not exist")
            elif error_code == 'AccessDenied':
                raise PermissionError(f"Access denied to S3 bucket '{bucket}'")
            else:
                raise RuntimeError(f"S3 upload failed: {e}")
    
    def load_manifest(self, bucket: str, key: str) -> List[Dict[str, str]]:
        """Load manifest from S3 with proper error handling."""
        try:
            response = self._client.get_object(Bucket=bucket, Key=key)
            data = response['Body'].read()
            return json.loads(data.decode('utf-8'))
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                self.logger.info(f"Manifest {key} not found, starting fresh")
                return []
            raise RuntimeError(f"Failed to load manifest: {e}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in manifest {key}: {e}")
    
    def save_manifest(self, manifest: List[Dict[str, str]], bucket: str, key: str) -> None:
        """Save manifest to S3."""
        try:
            body = json.dumps(manifest, indent=2).encode('utf-8')
            self._client.put_object(Bucket=bucket, Key=key, Body=body)
            self.logger.info(f"Saved manifest with {len(manifest)} entries to {key}")
        except ClientError as e:
            raise RuntimeError(f"Failed to save manifest: {e}")
```

## 4. Migration Steps

1. **Phase 1**: Create new classes alongside existing code
2. **Phase 2**: Update one script at a time to use new classes
3. **Phase 3**: Remove old global functions
4. **Phase 4**: Add comprehensive tests for new classes

## 5. Benefits Summary

### Improved Maintainability
- Dependency injection enables easier testing
- Smaller, focused functions are easier to understand
- Centralized URL building reduces duplication

### Better Error Handling
- Specific exception types for different error conditions
- Proper validation with clear error messages
- Resource cleanup and connection management

### Enhanced Type Safety
- Complete type annotations improve IDE support
- Dataclasses provide structured data with validation
- Generic types catch more errors at development time

### Testing Improvements
- No global state makes unit testing straightforward
- Dependency injection allows mocking external services
- Smaller functions are easier to test in isolation

## 6. Testing Considerations

### Unit Tests Required
- `EdgarClient` rate limiting behavior
- URL validation and construction
- S3Manager error handling scenarios
- FilingProcessor business logic

### Integration Tests Required
- End-to-end filing processing
- S3 upload/download workflows
- Rate limiting under concurrent load

### Mock Requirements
- SEC API responses
- S3 service calls
- File system operations
- Network timeouts and errors