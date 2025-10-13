import json
import boto3
import uuid
import time

from common import configuration
from common.configuration import S3Config
from common.models.metadata_model import MetadataModel

from pydantic import dataclasses
from typing import Tuple

import boto3
from botocore.config import Config
from botocore.client import BaseClient

@dataclasses.dataclass(frozen=True)
class ParsedStoreURL:
    bucket: str
    root_prefix: str

def _client_from_cfg(cfg: S3Config) -> BaseClient:
    return boto3.client(
        "s3",
        aws_access_key_id=cfg.access_key,
        aws_secret_access_key=cfg.secret_key,
        endpoint_url=cfg.endpoint,
        config=Config(
            s3={
                "addressing_style": "path"
            },
        ),
    )

def _parse_store_url(store_url: str) -> ParsedStoreURL:
    if not store_url.startswith("s3://"):
        raise ValueError(f"Invalid S3 store URL: {store_url}")
    without = store_url[len("s3://") :]
    parts = without.split("/", 1)
    bucket = parts[0]
    if len(parts) == 1:
        raise ValueError(f"Invalid S3 store URL (no bucket): {store_url}")
    else:
        root_prefix = parts[1].strip("/")

    return ParsedStoreURL(bucket=bucket, root_prefix=root_prefix)

def _build_keys(
    store_url: str,
    name: str,
    node_path: str,
    suffix: str = ".json",
) -> Tuple[ParsedStoreURL, str, str]:
    parsed = _parse_store_url(store_url)

    node = (node_path or "").strip().lstrip("/")

    base = parsed.root_prefix / "queue" / "accepted" / name
    if node:
        base = base / node

    ts = time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    uid = uuid.uuid4().hex[:12]
    filename = f"{ts}_{uid}{suffix}"
    data_key = str(base / filename)
    meta_key = f"{data_key}.meta.json"
    return parsed, data_key, meta_key

def _put_atomic(
    s3: BaseClient,
    bucket: str,
    key: str,
    body: bytes,
    content_type: str,
):
    tmp = f"{key}.tmp-{uuid.uuid4().hex[:8]}"
    s3.put_object(Bucket=bucket, Key=tmp, Body=body, ContentType=content_type)
    s3.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": tmp},
        Key=key,
        MetadataDirective="REPLACE",
        ContentType=content_type,
    )
    s3.delete_object(Bucket=bucket, Key=tmp)

def _write_pair(
    data_key: str,
    meta_key: str,
    payload: bytes,
    content_type: str,
    meta: MetadataModel,
    s3: BaseClient,
    bucket: str,
) -> str:
    _put_atomic(s3, bucket, data_key, payload, content_type)

    meta_bytes = json.dumps(meta).encode("utf-8")
    _put_atomic(s3, bucket, meta_key, meta_bytes, "application/json")
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
        raise RuntimeError("S3 configuration is missing or invalid")

    s3 = _client_from_cfg(s3cfg)

    parsed, data_key, meta_key = _build_keys(
        store_url=s3cfg.store_url,
        name=name,
        node_path=node_path,
        suffix=".csv" if "csv" in content_type else ".json",
    )

    return _write_pair(
        data_key=data_key,
        meta_key=meta_key,
        payload=payload,
        content_type=content_type,
        meta=meta_data,
        s3=s3,
        bucket=parsed.bucket,
    )
