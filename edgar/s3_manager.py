"""Enhanced S3 operations with connection pooling and error handling."""
import boto3
from botocore.exceptions import ClientError, NoCredentialsError, BotoCoreError
from botocore.config import Config
from typing import List, Dict, Optional, Any
import json
import logging
from contextlib import contextmanager


class S3CredentialsError(Exception):
    """Raised when AWS credentials are not available."""
    pass


class S3AccessError(Exception):
    """Raised when S3 access is denied."""
    pass


class S3NotFoundError(Exception):
    """Raised when S3 object or bucket is not found."""
    pass


class S3Manager:
    """Manages S3 operations with connection pooling and comprehensive error handling."""
    
    def __init__(
        self,
        region_name: Optional[str] = None,
        max_pool_connections: int = 50
    ):
        """Initialize S3 manager with connection pooling.
        
        Args:
            region_name: AWS region name
            max_pool_connections: Maximum number of connections in pool
            
        Raises:
            S3CredentialsError: If AWS credentials are not available
        """
        self.logger = logging.getLogger('s3_manager')
        
        try:
            # Configure connection pooling
            config = Config(
                region_name=region_name,
                retries={'max_attempts': 3, 'mode': 'adaptive'},
                max_pool_connections=max_pool_connections
            )
            self._client = boto3.client('s3', config=config)
            
            # Test credentials by listing buckets (minimal operation)
            self._client.list_buckets()
            
        except NoCredentialsError:
            raise S3CredentialsError(
                "AWS credentials not found. Please configure AWS credentials using "
                "AWS CLI, environment variables, or IAM roles."
            )
        except Exception as e:
            raise S3CredentialsError(f"Failed to initialize S3 client: {e}")
    
    def upload_bytes(self, data: bytes, bucket: str, key: str, **kwargs) -> None:
        """Upload bytes to S3 with comprehensive error handling.
        
        Args:
            data: Data to upload
            bucket: S3 bucket name
            key: S3 object key
            **kwargs: Additional arguments for put_object
            
        Raises:
            S3NotFoundError: If bucket doesn't exist
            S3AccessError: If access is denied
            RuntimeError: For other S3 errors
        """
        try:
            self._client.put_object(Bucket=bucket, Key=key, Body=data, **kwargs)
            self.logger.debug(f"Uploaded {len(data)} bytes to s3://{bucket}/{key}")
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            
            if error_code == 'NoSuchBucket':
                raise S3NotFoundError(f"S3 bucket '{bucket}' does not exist")
            elif error_code in ('AccessDenied', 'Forbidden'):
                raise S3AccessError(f"Access denied to S3 bucket '{bucket}'")
            elif error_code == 'InvalidBucketName':
                raise ValueError(f"Invalid bucket name: '{bucket}'")
            else:
                raise RuntimeError(f"S3 upload failed: {e}")
                
        except BotoCoreError as e:
            raise RuntimeError(f"AWS SDK error during upload: {e}")
    
    def upload_file(self, file_path: str, bucket: str, key: str, **kwargs) -> None:
        """Upload file to S3.
        
        Args:
            file_path: Local file path
            bucket: S3 bucket name
            key: S3 object key
            **kwargs: Additional arguments for upload_file
        """
        try:
            self._client.upload_file(file_path, bucket, key, **kwargs)
            self.logger.debug(f"Uploaded file {file_path} to s3://{bucket}/{key}")
            
        except FileNotFoundError:
            raise FileNotFoundError(f"Local file not found: {file_path}")
        except ClientError as e:
            self._handle_client_error(e, f"upload file to s3://{bucket}/{key}")
    
    def download_bytes(self, bucket: str, key: str) -> bytes:
        """Download object from S3 as bytes.
        
        Args:
            bucket: S3 bucket name
            key: S3 object key
            
        Returns:
            Object data as bytes
            
        Raises:
            S3NotFoundError: If object or bucket doesn't exist
            S3AccessError: If access is denied
        """
        try:
            response = self._client.get_object(Bucket=bucket, Key=key)
            return response['Body'].read()
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            
            if error_code in ('NoSuchKey', 'NoSuchBucket'):
                raise S3NotFoundError(f"Object s3://{bucket}/{key} not found")
            elif error_code in ('AccessDenied', 'Forbidden'):
                raise S3AccessError(f"Access denied to s3://{bucket}/{key}")
            else:
                raise RuntimeError(f"S3 download failed: {e}")
    
    def object_exists(self, bucket: str, key: str) -> bool:
        """Check if object exists in S3.
        
        Args:
            bucket: S3 bucket name
            key: S3 object key
            
        Returns:
            True if object exists, False otherwise
        """
        try:
            self._client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            if error_code in ('NoSuchKey', '404'):
                return False
            # Re-raise other errors
            raise RuntimeError(f"Error checking object existence: {e}")
    
    def load_json_manifest(self, bucket: str, key: str) -> List[Dict[str, Any]]:
        """Load JSON manifest from S3 with proper error handling.
        
        Args:
            bucket: S3 bucket name
            key: S3 object key
            
        Returns:
            Parsed JSON data as list of dictionaries
        """
        try:
            data = self.download_bytes(bucket, key)
            manifest = json.loads(data.decode('utf-8'))
            
            if not isinstance(manifest, list):
                raise ValueError("Manifest must be a JSON array")
                
            self.logger.info(f"Loaded manifest with {len(manifest)} entries from {key}")
            return manifest
            
        except S3NotFoundError:
            self.logger.info(f"Manifest {key} not found, starting with empty manifest")
            return []
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in manifest s3://{bucket}/{key}: {e}")
    
    def save_json_manifest(self, manifest: List[Dict[str, Any]], bucket: str, key: str) -> None:
        """Save JSON manifest to S3.
        
        Args:
            manifest: List of dictionaries to save as JSON
            bucket: S3 bucket name
            key: S3 object key
        """
        try:
            body = json.dumps(manifest, indent=2, default=str).encode('utf-8')
            self.upload_bytes(body, bucket, key, ContentType='application/json')
            self.logger.info(f"Saved manifest with {len(manifest)} entries to {key}")
            
        except Exception as e:
            raise RuntimeError(f"Failed to save manifest: {e}")
    
    def list_objects(self, bucket: str, prefix: str = "", max_keys: int = 1000) -> List[Dict[str, Any]]:
        """List objects in S3 bucket with optional prefix.
        
        Args:
            bucket: S3 bucket name
            prefix: Object key prefix to filter by
            max_keys: Maximum number of objects to return
            
        Returns:
            List of object metadata dictionaries
        """
        try:
            paginator = self._client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(
                Bucket=bucket,
                Prefix=prefix,
                PaginationConfig={'MaxItems': max_keys}
            )
            
            objects = []
            for page in page_iterator:
                objects.extend(page.get('Contents', []))
            
            return objects
            
        except ClientError as e:
            self._handle_client_error(e, f"list objects in s3://{bucket}/{prefix}")
    
    def _handle_client_error(self, error: ClientError, operation: str) -> None:
        """Handle common ClientError patterns.
        
        Args:
            error: The ClientError that occurred
            operation: Description of the operation that failed
            
        Raises:
            Appropriate exception based on error code
        """
        error_code = error.response.get('Error', {}).get('Code', 'Unknown')
        
        if error_code in ('NoSuchBucket', 'NoSuchKey'):
            raise S3NotFoundError(f"Resource not found during {operation}: {error}")
        elif error_code in ('AccessDenied', 'Forbidden'):
            raise S3AccessError(f"Access denied during {operation}: {error}")
        else:
            raise RuntimeError(f"S3 error during {operation}: {error}")
    
    @contextmanager
    def batch_operations(self):
        """Context manager for batch operations (future enhancement)."""
        # Placeholder for future batching optimization
        yield self
    
    def close(self):
        """Clean up resources (boto3 handles connection pooling automatically)."""
        # boto3 client connections are managed automatically
        pass
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# Legacy function for backward compatibility
def upload_bytes_to_s3(data: bytes, bucket: str, key: str) -> None:
    """Legacy function - use S3Manager for new code."""
    manager = S3Manager()
    manager.upload_bytes(data, bucket, key)


def load_manifest(bucket: str, key: str) -> List[Dict[str, str]]:
    """Legacy function - use S3Manager for new code."""
    manager = S3Manager()
    return manager.load_json_manifest(bucket, key)


def save_manifest(manifest: List[Dict[str, str]], bucket: str, key: str) -> None:
    """Legacy function - use S3Manager for new code."""
    manager = S3Manager()
    manager.save_json_manifest(manifest, bucket, key)