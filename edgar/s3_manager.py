"""Enhanced S3 operations with connection pooling and error handling."""

import io
import json
import logging
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional, Union

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError


class S3CredentialsError(Exception):
    """Raised when AWS credentials are not available."""

    pass


class S3AccessError(Exception):
    """Raised when S3 access is denied."""

    pass


class S3NotFoundError(Exception):
    """Raised when S3 object or bucket is not found."""

    pass


class S3ValidationError(Exception):
    """Raised when S3 bucket or key validation fails."""

    pass


def validate_s3_bucket_name(bucket_name: str) -> str:
    """Validate S3 bucket name according to AWS naming rules and security best practices.

    Args:
        bucket_name: S3 bucket name to validate

    Returns:
        Validated bucket name

    Raises:
        S3ValidationError: If bucket name is invalid or potentially dangerous
    """
    if not bucket_name or not bucket_name.strip():
        raise S3ValidationError("S3 bucket name cannot be empty")

    bucket_name = bucket_name.strip().lower()

    # AWS S3 bucket naming rules
    if len(bucket_name) < 3 or len(bucket_name) > 63:
        raise S3ValidationError(
            f"S3 bucket name must be between 3 and 63 characters: {bucket_name}"
        )

    # Must start and end with letter or number
    if not bucket_name[0].isalnum() or not bucket_name[-1].isalnum():
        raise S3ValidationError(
            f"S3 bucket name must start and end with letter or number: {bucket_name}"
        )

    # Only lowercase letters, numbers, dots, and hyphens
    if not re.match(r"^[a-z0-9.-]+$", bucket_name):
        raise S3ValidationError(
            f"S3 bucket name contains invalid characters: {bucket_name}"
        )

    # Cannot have consecutive periods or period-hyphen combinations
    if ".." in bucket_name or ".-" in bucket_name or "-." in bucket_name:
        raise S3ValidationError(
            f"S3 bucket name has invalid character combinations: {bucket_name}"
        )

    # Cannot be formatted as IP address
    ip_pattern = r"^(\d{1,3}\.){3}\d{1,3}$"
    if re.match(ip_pattern, bucket_name):
        raise S3ValidationError(
            f"S3 bucket name cannot be formatted as IP address: {bucket_name}"
        )

    # Security: Block common dangerous patterns
    dangerous_patterns = ["xn--", "sthree-", "sthree-configurator", "amzn-s3-demo-"]
    for pattern in dangerous_patterns:
        if pattern in bucket_name:
            raise S3ValidationError(
                f"S3 bucket name contains restricted pattern '{pattern}': {bucket_name}"
            )

    return bucket_name


def validate_s3_object_key(object_key: str) -> str:
    """Validate S3 object key for security and AWS compliance.

    Args:
        object_key: S3 object key to validate

    Returns:
        Validated object key

    Raises:
        S3ValidationError: If object key is invalid or potentially dangerous
    """
    if not object_key or not object_key.strip():
        raise S3ValidationError("S3 object key cannot be empty")

    original_key = object_key
    object_key = object_key.strip()

    # Length validation - AWS limit is 1024 bytes UTF-8 encoded
    if len(object_key.encode("utf-8")) > 1024:
        raise S3ValidationError(
            f"S3 object key too long (max 1024 bytes UTF-8): {len(object_key.encode('utf-8'))}"
        )

    # Security: Prevent path traversal attempts
    if "../" in object_key or "/.." in object_key:
        raise S3ValidationError(
            f"S3 object key contains path traversal patterns: {object_key}"
        )

    # Normalize and check for dangerous characters
    import urllib.parse

    try:
        decoded_key = urllib.parse.unquote(object_key)
    except Exception:
        decoded_key = object_key

    # Check for null bytes and control characters
    if "\x00" in decoded_key or any(
        ord(c) < 32 for c in decoded_key if c not in "\t\n\r"
    ):
        raise S3ValidationError(
            f"S3 object key contains control characters: {object_key}"
        )

    # AWS recommends avoiding these characters for better compatibility
    problematic_chars = [
        "\\",
        "{",
        "}",
        "^",
        "%",
        "`",
        "[",
        "]",
        '"',
        ">",
        "<",
        "~",
        "#",
        "|",
    ]
    for char in problematic_chars:
        if char in object_key:
            logging.warning(
                f"S3 object key contains potentially problematic character '{char}': {object_key}"
            )

    # Prevent keys that start with special patterns
    dangerous_prefixes = ["/", "../", "./", ".aws/", ".s3/"]
    for prefix in dangerous_prefixes:
        if object_key.startswith(prefix):
            raise S3ValidationError(
                f"S3 object key starts with dangerous prefix '{prefix}': {object_key}"
            )

    return object_key


class S3Manager:
    """Manages S3 operations with connection pooling and comprehensive error handling."""

    # Multipart upload configuration
    MULTIPART_THRESHOLD = 100 * 1024 * 1024  # 100MB
    MULTIPART_CHUNKSIZE = 10 * 1024 * 1024  # 10MB per part
    MAX_CONCURRENCY = 10  # Maximum concurrent part uploads

    def __init__(
        self,
        region_name: Optional[str] = None,
        max_pool_connections: int = 50,
        multipart_threshold: int = None,
        multipart_chunksize: int = None,
        max_concurrency: int = None,
    ):
        """Initialize S3 manager with connection pooling.

        Args:
            region_name: AWS region name
            max_pool_connections: Maximum number of connections in pool
            multipart_threshold: Minimum size for multipart upload (bytes)
            multipart_chunksize: Size of each multipart chunk (bytes)
            max_concurrency: Maximum concurrent part uploads

        Raises:
            S3CredentialsError: If AWS credentials are not available
        """
        self.logger = logging.getLogger("s3_manager")

        # Configure multipart upload settings
        self.multipart_threshold = multipart_threshold or self.MULTIPART_THRESHOLD
        self.multipart_chunksize = multipart_chunksize or self.MULTIPART_CHUNKSIZE
        self.max_concurrency = max_concurrency or self.MAX_CONCURRENCY

        try:
            # Configure connection pooling
            config = Config(
                region_name=region_name,
                retries={"max_attempts": 3, "mode": "adaptive"},
                max_pool_connections=max_pool_connections,
            )
            self._client = boto3.client("s3", config=config)

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
            S3ValidationError: If bucket or key validation fails
            S3NotFoundError: If bucket doesn't exist
            S3AccessError: If access is denied
            RuntimeError: For other S3 errors
        """
        # Validate inputs before making API call
        bucket = validate_s3_bucket_name(bucket)
        key = validate_s3_object_key(key)

        data_size = len(data)

        # Use multipart upload for large files
        if data_size >= self.multipart_threshold:
            self.logger.debug(f"Using multipart upload for {data_size} bytes")
            self._upload_multipart(io.BytesIO(data), bucket, key, data_size, **kwargs)
        else:
            try:
                self._client.put_object(Bucket=bucket, Key=key, Body=data, **kwargs)
                self.logger.debug(f"Uploaded {data_size} bytes to s3://{bucket}/{key}")

            except ClientError as e:
                self._handle_client_error(e, f"upload to s3://{bucket}/{key}")
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
        # Validate inputs before making API call
        bucket = validate_s3_bucket_name(bucket)
        key = validate_s3_object_key(key)

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
            S3ValidationError: If bucket or key validation fails
            S3NotFoundError: If object or bucket doesn't exist
            S3AccessError: If access is denied
        """
        # Validate inputs before making API call
        bucket = validate_s3_bucket_name(bucket)
        key = validate_s3_object_key(key)

        try:
            response = self._client.get_object(Bucket=bucket, Key=key)
            return response["Body"].read()

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")

            if error_code in ("NoSuchKey", "NoSuchBucket"):
                raise S3NotFoundError(f"Object s3://{bucket}/{key} not found")
            elif error_code in ("AccessDenied", "Forbidden"):
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

        Raises:
            S3ValidationError: If bucket or key validation fails
        """
        # Validate inputs before making API call
        bucket = validate_s3_bucket_name(bucket)
        key = validate_s3_object_key(key)

        try:
            self._client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            if error_code in ("NoSuchKey", "404"):
                return False
            # Re-raise other errors
            raise RuntimeError(f"Error checking object existence: {e}")

    def _upload_multipart(
        self, file_obj: io.IOBase, bucket: str, key: str, total_size: int, **kwargs
    ) -> None:
        """Upload large file using multipart upload with concurrent parts.

        Args:
            file_obj: File-like object to upload
            bucket: S3 bucket name
            key: S3 object key
            total_size: Total size of the file
            **kwargs: Additional arguments for create_multipart_upload
        """
        try:
            # Create multipart upload
            response = self._client.create_multipart_upload(
                Bucket=bucket, Key=key, **kwargs
            )
            upload_id = response["UploadId"]

            try:
                # Calculate parts
                parts = []
                part_number = 1
                position = 0

                # Use ThreadPoolExecutor for concurrent part uploads
                with ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
                    futures = []

                    while position < total_size:
                        # Calculate part size
                        part_size = min(self.multipart_chunksize, total_size - position)

                        # Read part data
                        file_obj.seek(position)
                        part_data = file_obj.read(part_size)

                        # Submit upload task
                        future = executor.submit(
                            self._upload_part,
                            bucket,
                            key,
                            upload_id,
                            part_number,
                            part_data,
                        )
                        futures.append((future, part_number))

                        part_number += 1
                        position += part_size

                    # Collect results
                    for future, part_num in futures:
                        try:
                            part_info = future.result(
                                timeout=300
                            )  # 5 minute timeout per part
                            parts.append(part_info)
                        except Exception as e:
                            self.logger.error(f"Failed to upload part {part_num}: {e}")
                            raise

                # Sort parts by part number
                parts.sort(key=lambda x: x["PartNumber"])

                # Complete multipart upload
                self._client.complete_multipart_upload(
                    Bucket=bucket,
                    Key=key,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": parts},
                )

                self.logger.info(
                    f"Completed multipart upload: {total_size} bytes, {len(parts)} parts"
                )

            except Exception as e:
                # Abort multipart upload on failure
                try:
                    self._client.abort_multipart_upload(
                        Bucket=bucket, Key=key, UploadId=upload_id
                    )
                    self.logger.warning(f"Aborted multipart upload {upload_id}")
                except Exception as abort_error:
                    self.logger.error(
                        f"Failed to abort multipart upload: {abort_error}"
                    )
                raise

        except ClientError as e:
            self._handle_client_error(e, f"multipart upload to s3://{bucket}/{key}")
        except Exception as e:
            raise RuntimeError(f"Multipart upload failed: {e}")

    def _upload_part(
        self, bucket: str, key: str, upload_id: str, part_number: int, data: bytes
    ) -> Dict[str, Any]:
        """Upload a single part of a multipart upload.

        Args:
            bucket: S3 bucket name
            key: S3 object key
            upload_id: Multipart upload ID
            part_number: Part number (1-based)
            data: Part data

        Returns:
            Part info dictionary with ETag and PartNumber
        """
        try:
            response = self._client.upload_part(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=data,
            )

            return {"ETag": response["ETag"], "PartNumber": part_number}

        except ClientError as e:
            raise RuntimeError(f"Failed to upload part {part_number}: {e}")

    def upload_stream(
        self,
        stream: Iterator[bytes],
        bucket: str,
        key: str,
        total_size: Optional[int] = None,
        **kwargs,
    ) -> None:
        """Upload data from a stream with automatic multipart handling.

        Args:
            stream: Iterator yielding bytes chunks
            bucket: S3 bucket name
            key: S3 object key
            total_size: Optional total size hint for optimization
            **kwargs: Additional arguments for upload
        """
        # Validate inputs
        bucket = validate_s3_bucket_name(bucket)
        key = validate_s3_object_key(key)

        # Collect stream data into memory for multipart upload
        buffer = io.BytesIO()
        total_read = 0

        for chunk in stream:
            buffer.write(chunk)
            total_read += len(chunk)

            # Use multipart upload if we exceed threshold
            if total_read >= self.multipart_threshold:
                break
        else:
            # Stream fits in memory, use regular upload
            buffer.seek(0)
            self.upload_bytes(buffer.getvalue(), bucket, key, **kwargs)
            buffer.close()
            return

        # Continue reading stream for multipart upload
        for chunk in stream:
            buffer.write(chunk)
            total_read += len(chunk)

        # Upload using multipart
        buffer.seek(0)
        self._upload_multipart(buffer, bucket, key, total_read, **kwargs)
        buffer.close()

    def load_json_manifest(self, bucket: str, key: str) -> List[Dict[str, Any]]:
        """Load JSON manifest from S3 with proper error handling.

        Args:
            bucket: S3 bucket name
            key: S3 object key

        Returns:
            Parsed JSON data as list of dictionaries

        Raises:
            S3ValidationError: If bucket or key validation fails
        """
        # Input validation is handled by download_bytes
        try:
            data = self.download_bytes(bucket, key)
            manifest = json.loads(data.decode("utf-8"))

            if not isinstance(manifest, list):
                raise ValueError("Manifest must be a JSON array")

            self.logger.info(f"Loaded manifest with {len(manifest)} entries from {key}")
            return manifest

        except S3NotFoundError:
            self.logger.info(f"Manifest {key} not found, starting with empty manifest")
            return []
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in manifest s3://{bucket}/{key}: {e}")

    def save_json_manifest(
        self, manifest: List[Dict[str, Any]], bucket: str, key: str
    ) -> None:
        """Save JSON manifest to S3.

        Args:
            manifest: List of dictionaries to save as JSON
            bucket: S3 bucket name
            key: S3 object key

        Raises:
            S3ValidationError: If bucket or key validation fails
        """
        # Input validation is handled by upload_bytes
        try:
            body = json.dumps(manifest, indent=2, default=str).encode("utf-8")
            self.upload_bytes(body, bucket, key, ContentType="application/json")
            self.logger.info(f"Saved manifest with {len(manifest)} entries to {key}")

        except Exception as e:
            raise RuntimeError(f"Failed to save manifest: {e}")

    def list_objects(
        self, bucket: str, prefix: str = "", max_keys: int = 1000
    ) -> List[Dict[str, Any]]:
        """List objects in S3 bucket with optional prefix.

        Args:
            bucket: S3 bucket name
            prefix: Object key prefix to filter by
            max_keys: Maximum number of objects to return

        Returns:
            List of object metadata dictionaries

        Raises:
            S3ValidationError: If bucket or prefix validation fails
        """
        # Validate inputs before making API call
        bucket = validate_s3_bucket_name(bucket)
        if prefix:
            prefix = validate_s3_object_key(prefix)

        try:
            paginator = self._client.get_paginator("list_objects_v2")
            page_iterator = paginator.paginate(
                Bucket=bucket, Prefix=prefix, PaginationConfig={"MaxItems": max_keys}
            )

            objects = []
            for page in page_iterator:
                objects.extend(page.get("Contents", []))

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
        error_code = error.response.get("Error", {}).get("Code", "Unknown")

        if error_code in ("NoSuchBucket", "NoSuchKey"):
            raise S3NotFoundError(f"Resource not found during {operation}: {error}")
        elif error_code in ("AccessDenied", "Forbidden"):
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
