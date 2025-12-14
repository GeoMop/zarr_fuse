import io
import logging
from typing import Tuple

import pandas as pl

from packages.common.models import MetadataModel
from workflows.io.zarr_store import write_to_zarr

LOG = logging.getLogger(__name__)


def read_df_from_bytes(data: bytes, content_type: str) -> Tuple[pl.DataFrame | None, str | None]:
    ct = content_type.lower()
    if "csv" in ct:
        return pl.read_csv(io.BytesIO(data)), None
    elif "json" in ct:
        return pl.read_json(io.BytesIO(data)), None
    else:
        return None, f"Unsupported content type: {content_type}. Use application/json or text/csv."


def df_from_payload(payload: bytes, content_type: str):
    df, err = read_df_from_bytes(payload, content_type)
    if err:
        raise ValueError(err)
    return df


def process_payload(meta: MetadataModel, payload: bytes) -> None:
    content_type = (meta.content_type or "application/json").lower()
    node_path = (meta.node_path or "").strip()
    schema_name = meta.schema_name
    if not schema_name:
        raise ValueError("Missing schema_name in meta")

    df = df_from_payload(payload, content_type)
    write_to_zarr(schema_name, node_path, df)
    LOG.info("Written to Zarr (schema=%s, node=%s)", schema_name, node_path)
