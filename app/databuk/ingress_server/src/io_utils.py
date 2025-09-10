import io
import os
import time
import uuid
import json
import csv
import polars as pl
import zarr_fuse as zf
from pathlib import Path
from dotenv import load_dotenv

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

def read_df_from_bytes(data: bytes, content_type: str) -> tuple[pl.DataFrame | None, str | None]:
    ct = content_type.lower()
    if "csv" in ct:
        return pl.read_csv(io.BytesIO(data)), None
    elif "json" in ct:
        return pl.read_json(io.BytesIO(data)), None
    else:
        return None, f"Unsupported content type: {content_type}. Use application/json or text/csv."

def validate_content_type(content_type: str | None) -> tuple[bool, str | None]:
    if not content_type:
        return False, "No Content-Type provided"

    ct = content_type.lower()
    if ("application/json" not in ct) and ("text/csv" not in ct):
        return False, f"Unsupported Content-Type: {content_type}"
    return True, None

def sanitize_node_path(p: str) -> tuple[Path, str | None]:
    p = (p or "").strip().lstrip("/")
    candidate = Path(p)

    if candidate.is_absolute() or any(part == ".." for part in candidate.parts):
        return None, f"Invalid node_path {p}"

    return candidate, None

def validate_data(data: bytes, content_type: str) -> tuple[bool, str | None]:
    if not data:
        return False, "No data provided"

    if "json" in content_type:
        try:
            json.loads(data.decode("utf-8"))
        except Exception as e:
            return False, f"Invalid JSON payload: {e}"

    elif "csv" in content_type:
        try:
            reader = csv.reader(io.StringIO(data.decode("utf-8")))
            next(reader)
        except Exception as e:
            return False, f"Invalid CSV payload: {e}"

    return True, None


# =========================
# S3 / zarr_fuse helpers
# =========================
def _get_env_vars() -> tuple[tuple[str, str] | None, str | None]:
    s3_key = os.getenv("S3_ACCESS_KEY")
    s3_sec = os.getenv("S3_SECRET_KEY")

    if s3_key is None:
        return None, "S3 access key must be provided"
    if s3_sec is None:
        return None, "S3 secret key must be provided"
    return (s3_key, s3_sec), None

def open_root(schema_path: Path) -> tuple[zf.Node | None, str | None]:
    (s3_key, s3_sec), err = _get_env_vars()
    if err:
        return None, err

    opts = {
        "S3_ACCESS_KEY": s3_key,
        "S3_SECRET_KEY": s3_sec,
        "S3_OPTIONS": json.dumps({
            "config_kwargs": {"s3": {"addressing_style": "path"}}
        }),
    }

    return zf.open_store(schema_path, **opts), None
