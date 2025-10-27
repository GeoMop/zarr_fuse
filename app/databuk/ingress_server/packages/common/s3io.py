import uuid
import time
import logging

import configuration
from models.metadata_model import MetadataModel

from typing import Tuple

LOG = logging.getLogger("s3io")

S3_CONFIG = configuration.load_s3_config()
BOTO3_CLIENT = configuration.create_boto3_client()

def get_bucket_from_store_url() -> str:
    store_url = S3_CONFIG.store_url

    if not store_url.startswith("s3://"):
        raise ValueError(f"Invalid S3 store URL: {store_url}")

    without = store_url[len("s3://") :]
    parts = without.split("/", 1)
    bucket = parts[0]

    return bucket

def _build_keys(
    name: str,
    node_path: str,
    suffix: str = ".json",
) -> Tuple[str, str, str]:
    bucket = get_bucket_from_store_url()

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
    bucket: str,
    key: str,
    body: bytes,
    content_type: str,
):
    BOTO3_CLIENT.put_object(
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
    bucket: str,
) -> str:
    _put_atomic(bucket, data_key, payload, content_type)

    meta_bytes = meta_data.model_dump_json().encode("utf-8")

    _put_atomic(bucket, meta_key, meta_bytes, "application/json")
    return f"s3://{bucket}/{data_key}"

def save_accepted_object(
    name: str,
    node_path: str,
    content_type: str,
    payload: bytes,
    meta_data: MetadataModel
) -> str:
    bucket, data_key, meta_key = _build_keys(
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
        bucket=bucket,
    )
