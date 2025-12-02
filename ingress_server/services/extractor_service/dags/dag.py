from __future__ import annotations

import os
import io
import json
import logging
import posixpath

import pandas as pl
import zarr_fuse as zf

from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from pydantic import BaseModel
from dotenv import load_dotenv

from packages.common.models import MetadataModel
from packages.common import configuration, s3io, logging_setup

LOG = logging.getLogger("dag_extractor")

load_dotenv()

S3_CONFIG = configuration.load_s3_config()
S3_CLIENT = configuration.create_boto3_client()


class S3Items(BaseModel):
    key: str
    last_modified: datetime

class Prefixes(BaseModel):
    bucket: str
    accepted: str
    processed: str
    failed: str


def read_df_from_bytes(data: bytes, content_type: str) -> tuple[pl.DataFrame | None, str | None]:
    ct = content_type.lower()
    if "csv" in ct:
        return pl.read_csv(io.BytesIO(data)), None
    elif "json" in ct:
        return pl.read_json(io.BytesIO(data)), None
    else:
        return None, f"Unsupported content type: {content_type}. Use application/json or text/csv."

def open_root(schema_path: Path) -> zf.Node:
    opts = {
        "S3_ACCESS_KEY": S3_CONFIG.access_key,
        "S3_SECRET_KEY": S3_CONFIG.secret_key,
        "STORE_URL": S3_CONFIG.store_url,
        "S3_OPTIONS": json.dumps({
            "config_kwargs": {"s3": {"addressing_style": "path"}}
        }),
    }
    return zf.open_store(schema_path, **opts)

def _prefixes(endpoint_name: str) -> Prefixes:
    bucket = s3io.get_bucket_from_store_url()
    base = f"queue"
    return Prefixes(
        bucket=bucket,
        accepted=f"{base}/accepted/{endpoint_name}",
        processed=f"{base}/processed/{endpoint_name}",
        failed=f"{base}/failed/{endpoint_name}",
    )

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
    _copy_then_delete(bucket, data_key, posixpath.join(target_prefix, rel))

    try:
        rel_meta = meta_key[len(accepted_prefix):].lstrip("/")
        _copy_then_delete(bucket, meta_key, posixpath.join(target_prefix, rel_meta))
    except Exception:
        LOG.warning("Failed to move meta %s", meta_key)
        pass

def _load_meta(bucket: str, meta_key: str) -> MetadataModel | None:
    try:
        return MetadataModel.model_validate_json(_read_bytes(bucket, meta_key))
    except Exception:
        return None

def _df_from_payload(payload: bytes, content_type: str):
    df, err = read_df_from_bytes(payload, content_type)
    if err:
        raise ValueError(err)
    return df

def _get_schema_dir(schema_name: str) -> Path:
    env = os.getenv("SCHEMAS_DIR")
    if env:
        return Path(env).resolve() / schema_name
    return (Path(__file__).resolve().parents[3] / "inputs/schemas" / schema_name).resolve()

def _write_to_zarr(schema_name: str, node_path: str, df) -> None:
    root = open_root(_get_schema_dir(schema_name))
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
        LOG.warning("Missing meta for %s → moved to failed.", data_key)
        return False

    content_type = (meta.content_type or "application/json").lower()
    node_path = (meta.node_path or "").strip()
    schema_name = meta.schema_name
    if not schema_name:
        _move_pair(prefixes.bucket, data_key, meta_key, prefixes.failed, prefixes.accepted)
        raise ValueError("Missing schema_name in meta")

    df = _df_from_payload(payload, content_type)
    _write_to_zarr(schema_name, node_path, df)

    _move_pair(prefixes.bucket, data_key, meta_key, prefixes.processed, prefixes.accepted)
    LOG.info("Processed %s", data_key)
    return True


with DAG(
    dag_id="ingress_processor",
    description="Process accepted objects and store to Zarr via zarr_fuse",
    start_date=datetime(2025, 1, 1),
    schedule="*/3 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data", "retries": 2},
    tags=["ingress", "s3"],
) as dag:
    logging_setup.setup_logging(os.getenv("LOG_LEVEL", "INFO"))

    @task()
    def process_endpoint(endpoint_name: str, batch_size: int = 10) -> str:
        """Zpracuje až batch_size objektů pro daný endpoint."""
        pfx = _prefixes(endpoint_name)

        items = _list_accepted_oldest_first(pfx.bucket, pfx.accepted)
        if not items:
            LOG.info("Endpoint %s: nic ke zpracování", endpoint_name)
            return "Nothing to do."

        processed = 0
        for item in items:
            if processed >= batch_size:
                break

            try:
                if _process_single(pfx, item.key):
                    processed += 1
            except Exception:
                # necháme job failnout = retry přes Airflow retries
                raise

        msg = f"{endpoint_name}: processed {processed} object(s)."
        LOG.info(msg)
        return msg

    # Vytvoř tasks pro všechny endpoints (z configu) a scrappery
    for ep in configuration.load_endpoints_config():
        process_endpoint.override(task_id=f"process_endpoint__{ep.name}")(
            endpoint_name=ep.name,
            batch_size=10,
        )

    for scr in configuration.load_scrappers_config():
        process_endpoint.override(task_id=f"process_scrapper__{scr.name}")(
            endpoint_name=scr.name,
            batch_size=10,
        )
