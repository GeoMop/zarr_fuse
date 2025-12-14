import logging
import posixpath
from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel
from dotenv import load_dotenv

from packages.common import configuration, s3io
from packages.common.models import MetadataModel

LOG = logging.getLogger(__name__)

load_dotenv()
S3_CLIENT = configuration.create_boto3_client()


class S3Item(BaseModel):
    key: str
    last_modified: datetime


class Prefixes(BaseModel):
    bucket: str
    accepted: str
    processed: str
    failed: str


def build_prefixes(endpoint_name: str) -> Prefixes:
    bucket = s3io.get_bucket_from_store_url()
    base = "queue"
    return Prefixes(
        bucket=bucket,
        accepted=f"{base}/accepted/{endpoint_name}",
        processed=f"{base}/processed/{endpoint_name}",
        failed=f"{base}/failed/{endpoint_name}",
    )


def list_accepted_oldest_first(bucket: str, accepted_prefix: str) -> List[S3Item]:
    paginator = S3_CLIENT.get_paginator("list_objects_v2")
    items: list[S3Item] = []

    for page in paginator.paginate(Bucket=bucket, Prefix=accepted_prefix + "/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".meta.json"):
                continue
            items.append(S3Item(key=key, last_modified=obj["LastModified"]))

    items.sort(key=lambda x: x.last_modified)
    return items


def read_bytes(bucket: str, key: str) -> bytes:
    return S3_CLIENT.get_object(Bucket=bucket, Key=key)["Body"].read()


def _copy_then_delete(bucket: str, src_key: str, dst_key: str) -> None:
    S3_CLIENT.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": src_key},
        Key=dst_key,
    )
    S3_CLIENT.delete_object(Bucket=bucket, Key=src_key)


def move_pair(bucket: str, data_key: str, meta_key: str, target_prefix: str, accepted_prefix: str) -> None:
    """
    Přesune data + meta z accepted do target_prefix (processed/failed).
    """
    rel = data_key[len(accepted_prefix) :].lstrip("/")
    _copy_then_delete(bucket, data_key, posixpath.join(target_prefix, rel))

    try:
        rel_meta = meta_key[len(accepted_prefix) :].lstrip("/")
        _copy_then_delete(bucket, meta_key, posixpath.join(target_prefix, rel_meta))
    except Exception:
        LOG.warning("Failed to move meta %s", meta_key)


def load_meta(bucket: str, meta_key: str) -> Optional[MetadataModel]:
    try:
        raw = read_bytes(bucket, meta_key)
        return MetadataModel.model_validate_json(raw)
    except Exception:
        return None
