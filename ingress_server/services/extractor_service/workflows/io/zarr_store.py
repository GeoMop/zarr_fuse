import json
import os
from pathlib import Path

import zarr_fuse as zf
from dotenv import load_dotenv

from packages.common import configuration

load_dotenv()
S3_CONFIG = configuration.load_s3_config()

def open_root(schema_path: Path) -> zf.Node:
    opts = {
        "S3_OPTIONS": json.dumps(
            {
                "config_kwargs": {"s3": {"addressing_style": "path"}},
            }
        ),
    }
    return zf.open_store(schema_path, **opts)


def get_schema_dir(schema_name: str) -> Path:
    env = os.getenv("SCHEMAS_DIR")
    if env:
        return Path(env).resolve() / schema_name
    return (Path(__file__).resolve().parents[3] / "inputs/schemas" / schema_name).resolve()


def write_to_zarr(schema_name: str, node_path: str, df) -> None:
    root = open_root(get_schema_dir(schema_name))
    if node_path:
        root[node_path].update(df)
    else:
        root.update(df)
