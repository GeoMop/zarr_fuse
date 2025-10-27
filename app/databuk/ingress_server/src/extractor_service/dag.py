from __future__ import annotations

import os
import posixpath
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from pydantic import BaseModel

from common import configuration
from common import validation
from common import io_utils
from common import s3io
from common.models.metadata_model import MetadataModel

from dotenv import load_dotenv

load_dotenv()

S3_CLIENT = configuration.create_boto3_client()

class S3Items(BaseModel):
    key: str
    last_modified: datetime

class Prefixes(BaseModel):
    bucket: str
    accepted: str
    processed: str
    failed: str


def _prefixes(endpoint_name: str) -> Prefixes:
    bucket = s3io.get_bucket_from_store_url()
    accepted = f"queue/accepted/{endpoint_name}"
    processed = f"queue/processed/{endpoint_name}"
    failed = f"queue/failed/{endpoint_name}"
    return Prefixes(bucket=bucket, accepted=accepted, processed=processed, failed=failed)


def _list_accepted_oldest_first(bucket: str, accepted_prefix: str) -> list[S3Items]:
    paginator = S3_CLIENT.get_paginator("list_objects_v2")
    items: list[S3Items] = []

    for page in paginator.paginate(Bucket=bucket, Prefix=accepted_prefix + "/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".meta.json"):
                continue
            items.append(S3Items(key=key, last_modified=obj["LastModified"]))

    items.sort(key=lambda x: x.last_modified)
    return items


def _read_bytes(bucket: str, key: str) -> bytes:
    return S3_CLIENT.get_object(Bucket=bucket, Key=key)["Body"].read()


def _copy_then_delete(bucket: str, src_key: str, dst_key: str) -> None:
    S3_CLIENT.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": src_key}, Key=dst_key)
    S3_CLIENT.delete_object(Bucket=bucket, Key=src_key)


def _move_pair(bucket: str, data_key: str, meta_key: str, target_prefix: str, accepted_prefix: str) -> None:
    rel = data_key[len(accepted_prefix):].lstrip("/")
    dst_data = posixpath.join(target_prefix, rel)
    _copy_then_delete(bucket, data_key, dst_data)

    try:
        rel_meta = meta_key[len(accepted_prefix):].lstrip("/")
        dst_meta = posixpath.join(target_prefix, rel_meta)
        _copy_then_delete(bucket, meta_key, dst_meta)
    except Exception:
        pass

def _load_meta(bucket: str, meta_key: str) -> MetadataModel | None:
    try:
        meta_raw = _read_bytes(bucket, meta_key)
        return MetadataModel.model_validate_json(meta_raw)
    except Exception:
        return None

def _df_from_payload(payload: bytes, content_type: str):
    df, err = validation.read_df_from_bytes(payload, content_type)
    if err:
        raise ValueError(err)
    return df

def _get_schema_dir(schema_name: str) -> Path:
    env = os.getenv("SCHEMAS_DIR")
    if env:
        return Path(env).resolve() / schema_name

    base = Path(__file__).resolve().parents[2] / "inputs/schemas" / schema_name
    return base.resolve()

def _write_to_zarr(schema_name: str, node_path: str, df) -> None:
    schema_path = _get_schema_dir(schema_name)
    root = io_utils.open_root(Path(schema_path))
    if node_path:
        root[node_path].update(df)
    else:
        root.update(df)

def _process_single(prefixes: Prefixes, data_key: str) -> bool:
    meta_key = data_key + ".meta.json"
    payload = _read_bytes(prefixes.bucket, data_key)

    meta = _load_meta(prefixes.bucket, meta_key)
    if not meta:
        _move_pair(prefixes.bucket, data_key, meta_key, prefixes.failed, prefixes.accepted)
        return False

    content_type = (meta.content_type or "application/json").lower()
    node_path = (meta.node_path or "").strip()
    schema_name = meta.schema_name
    if not schema_name:
        raise ValueError("Missing schema_name in meta")

    df = _df_from_payload(payload, content_type)
    _write_to_zarr(schema_name, node_path, df)

    _move_pair(prefixes.bucket, data_key, meta_key, prefixes.processed, prefixes.accepted)
    return True


# -----------------------
# DAG
# -----------------------
with DAG(
    dag_id="ingress_processor",
    description="Process the oldest accepted objects and store to Zarr (bukov) via zarr_fuse",
    start_date=datetime(2025, 1, 1),
    schedule="*/3 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data", "retries": 2},
    tags=["ingress", "s3"],
) as dag:

    @task()
    def process_endpoint(endpoint_name: str, batch_size: int = 10) -> str:
        prefixes = _prefixes(endpoint_name)

        items = _list_accepted_oldest_first(prefixes.bucket, prefixes.accepted)
        if not items:
            return "Nothing to do."

        processed = 0
        for item in items:
            if processed >= batch_size:
                break

            try:
                ok = _process_single(prefixes, item.key)
                processed += int(ok)
            except Exception:
                raise

        return f"Processed {processed} object(s)."

    endpoints = configuration.load_endpoints_config()
    for endpoint in endpoints:
        process_endpoint(endpoint_name=endpoint.name)

    scrappers = configuration.load_scrappers_config()
    for scrapper in scrappers:
        process_endpoint(endpoint_name=scrapper.name)
