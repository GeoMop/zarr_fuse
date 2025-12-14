import os
import logging
import posixpath
from datetime import datetime
from typing import List, Optional
from pathlib import Path

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
    in_progress: str
    processed: str
    failed: str


def build_prefixes(endpoint_name: str) -> Prefixes:
    bucket = s3io.get_bucket_from_store_url()
    base = os.getenv("S3_QUEUE_BASE_PREFIX", "queue")
    return Prefixes(
        bucket=bucket,
        accepted=f"{base}/accepted/{endpoint_name}",
        in_progress=f"{base}/in_progress/{endpoint_name}",
        processed=f"{base}/processed/{endpoint_name}",
        failed=f"{base}/failed/{endpoint_name}",
    )


def list_accepted_oldest_first(pfx: Prefixes, batch_size: int) -> List[S3Item]:
    paginator = S3_CLIENT.get_paginator("list_objects_v2")
    items: list[S3Item] = []

    for page in paginator.paginate(Bucket=pfx.bucket, Prefix=pfx.accepted + "/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".meta.json"):
                continue
            if len(items) >= batch_size:
                break
            items.append(S3Item(key=key, last_modified=obj["LastModified"]))
        if len(items) >= batch_size:
            break

    items.sort(key=lambda x: x.last_modified)
    return items


def download_to_file(bucket: str, key: str, local_path: Path) -> None:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    with local_path.open("wb") as f:
        S3_CLIENT.download_fileobj(bucket, key, f)


def _copy_then_delete(bucket: str, src_key: str, dst_key: str) -> None:
    S3_CLIENT.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": src_key},
        Key=dst_key,
    )
    S3_CLIENT.delete_object(Bucket=bucket, Key=src_key)

def accepted_to_in_progress_key(pfx: Prefixes, accepted_key: str) -> str:
    rel = accepted_key[len(pfx.accepted):].lstrip("/")
    return posixpath.join(pfx.in_progress, rel)

def move_pair(data_key: str, meta_key: str, bucket: str, source: str, target: str) -> None:
    rel = data_key[len(source) :].lstrip("/")
    _copy_then_delete(bucket, data_key, posixpath.join(target, rel))

    rel_meta = meta_key[len(source) :].lstrip("/")
    _copy_then_delete(bucket, meta_key, posixpath.join(target, rel_meta))


def load_meta(bucket: str, meta_key: str) -> Optional[MetadataModel]:
    try:
        raw = S3_CLIENT.get_object(Bucket=bucket, Key=meta_key)["Body"].read()
        return MetadataModel.model_validate_json(raw)
    except Exception as e:
        LOG.warning("Failed to load metadata %s. Error: %s", meta_key, e)
        return None
