import os
import io
import json
from typing import Any, List, Optional

import boto3
from botocore.client import Config

class MinIOClient:
    def __init__(self):
        self.endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        self.access = os.getenv("MINIO_ROOT_USER", "minioadmin")
        self.secret = os.getenv("MINIO_ROOT_PASSWORD", "miniopassword123")
        self.s3 = boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access,
            aws_secret_access_key=self.secret,
            region_name="us-east-1",
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )
        self.bronze = os.getenv("MINIO_BUCKET_BRONZE", "bronze")
        self.silver = os.getenv("MINIO_BUCKET_SILVER", "silver")
        self.gold = os.getenv("MINIO_BUCKET_GOLD", "gold")

    def ensure_bucket(self, bucket: str):
        try:
            self.s3.head_bucket(Bucket=bucket)
        except Exception:
            try:
                self.s3.create_bucket(Bucket=bucket)
            except Exception:
                pass

    def put_json(self, bucket: str, key: str, obj: Any):
        self.ensure_bucket(bucket)
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType="application/json")

    def put_bytes(self, bucket: str, key: str, data: bytes, content_type: str = "application/octet-stream"):
        self.ensure_bucket(bucket)
        self.s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)

    def get_json(self, bucket: str, key: str):
        resp = self.s3.get_object(Bucket=bucket, Key=key)
        return json.loads(resp["Body"].read())

    def list_keys(self, bucket: str, prefix: str = "") -> List[str]:
        self.ensure_bucket(bucket)
        keys = []
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys

    def get_object_stream(self, bucket: str, key: str):
        resp = self.s3.get_object(Bucket=bucket, Key=key)
        return resp["Body"].read()
