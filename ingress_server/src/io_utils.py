import io
import os
import time
import uuid
import json
import csv
import polars as pl

import zarr_fuse as zf

from pathlib import Path
from extractor import apply_extractor_if_any
from dotenv import load_dotenv

from configs import ACCEPTED_DIR
from models import MetadataModel, DataSourceConfig
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
    metadata: MetadataModel,
    payload: bytes,
) -> str | None:
    safe_child, err = sanitize_node_path(metadata.node_path)
    if err:
        return f"Failed to sanitize node_path: {metadata.node_path}. Error: {err}"

    meta_data = metadata.model_copy(update={"node_path": str(safe_child) if safe_child else None})

    base = (ACCEPTED_DIR / meta_data.endpoint_name)
    suffix = ".csv" if "csv" in meta_data.content_type else ".json"
    msg_path = new_msg_path(base, suffix)

    try:
        atomic_write(msg_path, payload)
    except Exception as e:
        return f"Failed to save data to {msg_path}: {e}"

    atomic_write(msg_path.with_suffix(msg_path.suffix + ".meta.json"), meta_data.model_dump_json().encode("utf-8"))
    return None


# =========================
# DataFrame helpers
# =========================
def create_df_from_bytes(
    payload: bytes,
    metadata: MetadataModel,
) -> tuple[pl.DataFrame | None, str | None]:
    if not metadata.extract_fn or not metadata.fn_module:
        try:
            df = pl.read_json(io.BytesIO(payload))
            return df, None
        except Exception as e:
            return None, f"Failed to read JSON data: {e}"

    try:
        return apply_extractor_if_any(
            payload=payload,
            metadata=metadata,
        ), None
    except Exception as e:
        return None, f"Failed to read JSON: {e}"

def read_df_from_bytes(
    payload: bytes,
    metadata: MetadataModel,
) -> tuple[pl.DataFrame | None, str | None]:
    ct = metadata.content_type.lower()

    if "csv" in ct:
        return pl.read_csv(io.BytesIO(payload)), None
    elif "json" in ct:
        return create_df_from_bytes(payload, metadata)
    else:
        return None, f"Unsupported content type: {metadata.content_type}. Use application/json or text/csv."


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


def process_payload(
    data_source: DataSourceConfig,
    payload: bytes,
    content_type: str,
    username: str,
    node_path: str | None = None,
    dataframe_row: dict | None = None,
) -> tuple[bool, str | None]:
    err = validate_response(payload, content_type)
    if err:
        return False, err

    metadata = MetadataModel.from_data_source(
        data_source,
        content_type=content_type,
        username=username,
        node_path=node_path,
        dataframe_row=dataframe_row,
    )

    err = save_data(
        metadata=metadata,
        payload=payload,
    )
    if err:
        return False, err

    return True, None


# =========================
# S3 / zarr_fuse helpers
# =========================
def open_root(schema_path: Path) -> tuple[zf.Node | None, str | None]:
    opts = {
        "S3_OPTIONS": json.dumps({
            "config_kwargs": {"s3": {"addressing_style": "path"}}
        }),
    }

    return zf.open_store(schema_path, **opts), None
