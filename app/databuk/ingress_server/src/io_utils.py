import io
import os
import time
import uuid
import json
import logging
import polars as pl
import zarr_fuse as zf
import io
import csv
from pathlib import Path

LOG = logging.getLogger("io_utils")

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

def read_df_from_bytes(data: bytes, content_type: str) -> pl.DataFrame:
    ct = content_type.lower()
    if "text/csv" in ct:
        return pl.read_csv(io.BytesIO(data))
    elif "application/json" in ct:
        return pl.read_json(io.BytesIO(data))
    else:
        LOG.error("Unsupported content type: %s", content_type)
        raise ValueError(f"Unsupported content type: {content_type}. Use application/json or text/csv.")

def validate_content_type(content_type: str | None) -> tuple[bool, str | None]:
    if not content_type:
        return False, "No Content-Type provided"

    ct = content_type.lower()
    if ("application/json" not in ct) and ("text/csv" not in ct):
        LOG.error("Unsupported Content-Type: %s", content_type)
        return False, f"Unsupported Content-Type: {content_type}"
    return True, None

def sanitize_node_path(p: str) -> Path:
    p = (p or "").strip().lstrip("/")
    candidate = Path(p)

    if candidate.is_absolute() or any(part == ".." for part in candidate.parts):
        LOG.error("Invalid node_path: %s", p)
        raise ValueError("Invalid node_path")

    return candidate

def validate_data(data: bytes, content_type: str) -> tuple[bool, str | None]:
    if not data:
        return False, "No data provided"

    if "json" in content_type:
        try:
            json.loads(data.decode("utf-8"))
        except Exception as e:
            LOG.warning("Invalid JSON payload: %s", e)
            return False, "Invalid JSON"

    elif "csv" in content_type:
        try:
            reader = csv.reader(io.StringIO(data.decode("utf-8")))
            next(reader)
        except Exception as e:
            LOG.warning("Invalid CSV payload: %s", e)
            return False, "Invalid CSV"

    return True, None


# =========================
# S3 / zarr_fuse helpers
# =========================
def _get_env_vars():
    s3_key = os.getenv("S3_ACCESS_KEY")
    s3_sec = os.getenv("S3_SECRET_KEY")

    if s3_key is None:
        LOG.error("S3 access key is missing")
        raise ValueError("S3 access key must be provided")
    if s3_sec is None:
        LOG.error("S3 secret key is missing")
        raise ValueError("S3 secret key must be provided")
    return s3_key, s3_sec

def open_root(schema_path: Path):
    s3_key, s3_sec = _get_env_vars()

    opts = {
        "S3_ACCESS_KEY": s3_key,
        "S3_SECRET_KEY": s3_sec,
        "S3_OPTIONS": json.dumps({
            "config_kwargs": {"s3": {"addressing_style": "path"}}
        }),
    }

    return zf.open_store(schema_path, **opts)
