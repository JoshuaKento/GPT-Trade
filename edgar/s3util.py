"""Helpers for S3 uploads and manifest handling."""
from typing import List, Dict
import json
import boto3


def upload_bytes_to_s3(data: bytes, bucket: str, key: str) -> None:
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=data)


def load_manifest(bucket: str, key: str) -> List[Dict[str, str]]:
    s3 = boto3.client("s3")
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        data = resp["Body"].read()
        return json.loads(data)
    except Exception:
        return []


def save_manifest(manifest: List[Dict[str, str]], bucket: str, key: str) -> None:
    s3 = boto3.client("s3")
    body = json.dumps(manifest, indent=2).encode("utf-8")
    s3.put_object(Bucket=bucket, Key=key, Body=body)
