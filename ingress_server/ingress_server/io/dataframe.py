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

LOG = logging.getLogger(__name__)


def _read_grib_from_bytes(payload: bytes, is_bz2: bool) -> xr.Dataset:
    try:
        data = bz2.decompress(payload) if is_bz2 else payload
    except Exception:
        LOG.exception("Failed to decompress bz2 GRIB payload")
        raise

    try:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "input.grib"
            p.write_bytes(data)
            return xr.open_dataset(p, engine="cfgrib", backend_kwargs={"indexpath": ""}).load()
    except Exception:
        LOG.exception("Failed to open GRIB with cfgrib/eccodes")
        raise


def read_df_from_bytes(payload: bytes, metadata: MetadataModel) -> DataObject:
    try:
        if metadata.extract_fn and metadata.fn_module:
            return apply_extractor(payload=payload, metadata=metadata)

        ct = classify_content_type(metadata.content_type)
        if ct is None:
            raise ValueError(f"Unsupported content type: {metadata.content_type}")

        match ct:
            case SupportedContentType.CSV:
                return pl.read_csv(io.BytesIO(payload))
            case SupportedContentType.JSON:
                return pl.read_json(io.BytesIO(payload))
            case SupportedContentType.GRIB | SupportedContentType.GRIB_BZ2:
                return _read_grib_from_bytes(payload, is_bz2=(ct == SupportedContentType.GRIB_BZ2))
            case SupportedContentType.OCTET_STREAM:
                raise ValueError(
                    f"Content type {metadata.content_type} "
                    "is not supported for DataFrame extraction."
                )
            case _:
                raise ValueError(f"Unsupported content type: {metadata.content_type}")
    except ValueError:
        LOG.warning(
            "Invalid data object input endpoint=%s content_type=%s",
            metadata.endpoint_name,
            metadata.content_type,
        )
        raise
    except Exception:
        LOG.exception(
            "Failed to read data object endpoint=%s content_type=%s",
            metadata.endpoint_name,
            metadata.content_type,
        )
        raise
