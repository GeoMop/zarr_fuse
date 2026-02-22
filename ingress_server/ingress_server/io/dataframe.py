import io
import bz2
import logging
import tempfile

import polars as pl
import xarray as xr

from pathlib import Path

from ..types import DataObject
from ..models import MetadataModel
from .extractor import apply_extractor
from .content_type import classify_content_type, SupportedContentType

LOG = logging.getLogger("io.dataframe")

def _read_json_from_bytes(payload: bytes) -> tuple[pl.DataFrame | None, str | None]:
    try:
        return pl.read_json(io.BytesIO(payload)), None
    except Exception as e:
        return None, f"Failed to read JSON data: {e}"


def _read_csv_from_bytes(payload: bytes) -> tuple[pl.DataFrame | None, str | None]:
    try:
        df = pl.read_csv(io.BytesIO(payload))
        return df, None
    except Exception as e:
        return None, f"Failed to read CSV data: {e}"


def _read_grib_from_bytes(payload: bytes, is_bz2: bool) -> tuple[xr.Dataset | None, str | None]:
    try:
        data = bz2.decompress(payload) if is_bz2 else payload
    except Exception as e:
        return None, f"Failed to decompress bz2 GRIB payload: {e}"

    try:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "input.grib"
            p.write_bytes(data)
            ds = xr.open_dataset(p, engine="cfgrib", backend_kwargs={"indexpath": ""}).load()
            return ds, None
    except Exception as e:
        return None, f"Failed to open GRIB with cfgrib/eccodes: {e}"


def apply_extractor_on_bytes(payload: bytes, metadata: MetadataModel) -> tuple[DataObject | None, str | None]:
    try:
        return apply_extractor(payload=payload, metadata=metadata), None
    except Exception as e:
        return None, f"Failed to read data via extractor: {e}"


def read_df_from_bytes(payload: bytes, metadata: MetadataModel) -> tuple[DataObject | None, str | None]:
    if metadata.extract_fn and metadata.fn_module:
        return apply_extractor_on_bytes(payload, metadata)

    ct = classify_content_type(metadata.content_type)
    if ct is None:
        return None, f"Unsupported content type: {metadata.content_type}"

    match ct:
        case SupportedContentType.CSV:
            return _read_csv_from_bytes(payload)
        case SupportedContentType.JSON:
            return _read_json_from_bytes(payload)
        case SupportedContentType.GRIB | SupportedContentType.GRIB_BZ2:
            return _read_grib_from_bytes(payload, is_bz2=(ct == SupportedContentType.GRIB_BZ2))
        case SupportedContentType.OCTET_STREAM:
            return None, f"Content type {metadata.content_type} is not supported for DataFrame extraction."
        case _:
            return None, f"Unsupported content type: {metadata.content_type}. Use application/json or text/csv."
