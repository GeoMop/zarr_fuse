import json
import boto3
import uuid
import time
import logging

from common import configuration
from common.configuration import S3Config
from common.models.metadata_model import MetadataModel

from typing import Tuple

import boto3
from botocore.config import Config
from botocore.client import BaseClient

LOG = logging.getLogger("s3io")

def _client_from_cfg(cfg: S3Config) -> BaseClient:
    config = Config(
        signature_version="s3v4",
        s3={
            "addressing_style": "path",
        },
        request_checksum_calculation="when_required",
        response_checksum_validation="when_required",
        retries={"max_attempts": 3, "mode": "standard"},
    )
    return boto3.client(
        "s3",
        aws_access_key_id=cfg.access_key,
        aws_secret_access_key=cfg.secret_key,
        endpoint_url=cfg.endpoint_url,
        config=config
    )

def _get_bucket_from_store_url(store_url: str) -> str:
    if not store_url.startswith("s3://"):
        raise ValueError(f"Invalid S3 store URL: {store_url}")

    without = store_url[len("s3://") :]
    parts = without.split("/", 1)
    bucket = parts[0]

    return bucket

def _build_keys(
    store_url: str,
    name: str,
    node_path: str,
    suffix: str = ".json",
) -> Tuple[str, str, str]:
    bucket = _get_bucket_from_store_url(store_url)

    node = (node_path or "").strip().lstrip("/")

    base = f"queue/accepted/{name}"
    if node:
        base = f"{base}/{node}"

    ts = time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    uid = uuid.uuid4().hex[:12]
    filename = f"{ts}_{uid}{suffix}"
    data_key = f"{base}/{filename}"
    meta_key = f"{data_key}.meta.json"
    return bucket, data_key, meta_key

def _put_atomic(
    s3: BaseClient,
    bucket: str,
    key: str,
    body: bytes,
    content_type: str,
):
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType=content_type,
    )

def _write_pair(
    data_key: str,
    meta_key: str,
    payload: bytes,
    content_type: str,
    meta_data: MetadataModel,
    s3_client: BaseClient,
    bucket: str,
) -> str:
    _put_atomic(s3_client, bucket, data_key, payload, content_type)

    meta_bytes = meta_data.model_dump_json().encode("utf-8")

    _put_atomic(s3_client, bucket, meta_key, meta_bytes, "application/json")
    return f"s3://{bucket}/{data_key}"

def save_accepted_object(
    name: str,
    node_path: str,
    content_type: str,
    payload: bytes,
    meta_data: MetadataModel
) -> str:
    s3cfg = configuration.load_s3_config()
    if not s3cfg:
        return "S3 configuration is missing or invalid"

    s3_client = _client_from_cfg(s3cfg)

    bucket, data_key, meta_key = _build_keys(
        store_url=s3cfg.store_url,
        name=name,
        node_path=node_path,
        suffix=".csv" if "csv" in content_type else ".json",
    )

    return _write_pair(
        data_key=data_key,
        payload=payload,
        content_type=content_type,
        meta_key=meta_key,
        meta_data=meta_data,
        s3_client=s3_client,
        bucket=bucket,
    )
