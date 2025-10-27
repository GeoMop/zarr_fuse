import io
import json

from pathlib import Path

import pandas as pl
import zarr_fuse as zf
import common.configuration as configuration

S3_CONFIG = configuration.load_s3_config()

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
