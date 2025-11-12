import io
import os
import time
import uuid
import json
import csv
import polars as pl

import zarr_fuse as zf
from zarr_fuse import schema

from pathlib import Path
from extractor import apply_extractor_if_any
from dotenv import load_dotenv

from configs import ACCEPTED_DIR
from models import MetadataModel
import logging

LOG = logging.getLogger("io_utils")

load_dotenv()

# =========================
# Filesystem helpers
# =========================
def atomic_write(path: Path, data: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)

def new_msg_path(base: Path, suffix: str) -> Path:
    ts = time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    uid = uuid.uuid4().hex[:12]
    return base / f"{ts}_{uid}{suffix}"


def save_data(
    name: str,
    payload: bytes,
    content_type: str,
    schema_path: str,
    extract_fn: str | None,
    fn_module: str | None,
    username: str,
    node_path: str | None = None,
    dataframe_row: dict | None = None,
) -> str | None:
    safe_child, err = sanitize_node_path(node_path)
    if err:
        return f"Failed to sanitize node_path: {node_path}. Error: {err}"

    base = (ACCEPTED_DIR / name)
    suffix = ".csv" if "csv" in content_type else ".json"
    msg_path = new_msg_path(base, suffix)

    try:
        atomic_write(msg_path, payload)
    except Exception as e:
        return f"Failed to save data to {msg_path}: {e}"

    meta_data = MetadataModel(
        content_type=content_type,
        endpoint_name=name,
        node_path=safe_child,
        username=username,
        received_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        schema_path=schema_path,
        extract_fn=extract_fn,
        fn_module=fn_module,
        dataframe_row=dataframe_row,
    )

    atomic_write(msg_path.with_suffix(msg_path.suffix + ".meta.json"), meta_data.model_dump_json().encode("utf-8"))
    return None


# =========================
# DataFrame helpers
# =========================
def create_df_from_bytes(
    payload: bytes,
    extract_fn: str,
    fn_module: str,
    endpoint_name: str,
    dataframe_row: dict | None,
) -> tuple[pl.DataFrame | None, str | None]:
    if not extract_fn or not fn_module:
        try:
            df = pl.read_json(io.BytesIO(payload))
            return df, None
        except Exception as e:
            return None, f"Failed to read JSON data: {e}"

    try:
        return apply_extractor_if_any(
            payload=payload,
            extract_fn=extract_fn,
            fn_module=fn_module,
            endpoint_name=endpoint_name,
            dataframe_row=dataframe_row,
        ), None
    except Exception as e:
        return None, f"Failed to read JSON: {e}"

def read_df_from_bytes(
    payload: bytes,
    content_type: str,
    extract_fn: str,
    fn_module: str,
    endpoint_name: str,
    dataframe_row: dict | None,
) -> tuple[pl.DataFrame | None, str | None]:
    ct = content_type.lower()

    if "csv" in ct:
        return pl.read_csv(io.BytesIO(payload)), None
    elif "json" in ct:
        return create_df_from_bytes(payload, extract_fn, fn_module, endpoint_name, dataframe_row)
    else:
        return None, f"Unsupported content type: {content_type}. Use application/json or text/csv."


# =========================
# Validation helpers
# =========================
def validate_content_type(content_type: str | None) -> tuple[bool, str | None]:
    if not content_type:
        return False, "No Content-Type provided"

    ct = content_type.lower()
    if ("json" in ct) or ("csv" in ct):
        return True, None
    return False, f"Unsupported Content-Type: {content_type}"

def sanitize_node_path(p: str) -> tuple[Path | None, str | None]:
    p = (p or "").strip().lstrip("/")

    if not p:
        return None, None

    candidate = Path(p)

    if candidate.is_absolute() or any(part == ".." for part in candidate.parts):
        return None, f"Invalid node_path {p}"

    return candidate, None

def validate_data(data: bytes, content_type: str) -> str | None:
    if not data:
        return "No data provided"

    if "json" in content_type:
        try:
            json.loads(data.decode("utf-8"))
        except Exception as e:
            return f"Invalid JSON payload: {e}"

    elif "csv" in content_type:
        try:
            reader = csv.reader(io.StringIO(data.decode("utf-8")))
            next(reader)
        except Exception as e:
            return f"Invalid CSV payload: {e}"

    return None

def validate_response(payload: bytes, content_type: str) -> str | None:
    ok, err = validate_content_type(content_type)
    if not ok:
        return f"Unsupported Content-Type: {content_type}. Error: {err}"

    payload = payload
    err = validate_data(payload, content_type)
    if err:
        return f"Data validation failed: {err}"

    return None


# =========================
# S3 / zarr_fuse helpers
# =========================

# this could be removed, as open_store provides a logic to load environment variables.
def _get_env_vars() -> tuple[tuple[str, str, str] | None, str | None]:
    s3_key = os.getenv("S3_ACCESS_KEY")
    s3_sec = os.getenv("S3_SECRET_KEY")
    store_url = os.getenv("S3_STORE_URL")

    if s3_key is None:
        return None, "S3 access key must be provided"
    if s3_sec is None:
        return None, "S3 secret key must be provided"
    if store_url is None:
        return None, "S3 store URL must be provided"
    return (s3_key, s3_sec, store_url), None

def open_root(schema_path: Path) -> tuple[zf.Node | None, str | None]:
    # this could be removed, as open_store provides a logic to load environment variables.
    env, err = _get_env_vars()
    if err:
        return None, err

    s3_key, s3_sec, store_url = env
    opts = {
        "S3_ACCESS_KEY": s3_key,
        "S3_SECRET_KEY": s3_sec,
        "STORE_URL": store_url,
        "S3_OPTIONS": json.dumps({
            "config_kwargs": {"s3": {"addressing_style": "path"}}
        }),
    }

    return zf.open_store(schema_path, **opts), None
