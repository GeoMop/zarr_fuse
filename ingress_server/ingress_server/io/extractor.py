import logging

import polars as pl
import xarray as xr

from importlib import import_module

from ..models import MetadataModel
from ..types import DataObject

LOG = logging.getLogger("io.extractor")


def resolve_extractor(metadata: MetadataModel):
    if not metadata.fn_module or not metadata.extract_fn:
        return None
    try:
        module = import_module(metadata.fn_module)
        return getattr(module, metadata.extract_fn)
    except Exception as e:
        LOG.error("Failed to resolve extractor %s from %s: %s", metadata.extract_fn, metadata.fn_module, e)
        return None

def apply_extractor(
    payload: bytes,
    metadata: MetadataModel,
) -> DataObject:
    extractor = resolve_extractor(metadata)
    if not extractor:
        LOG.info("No extractor for endpoint %s, skipping extraction", metadata.endpoint_name)
        raise ValueError("No extractor found")

    metadata_dict = metadata.model_dump()

    LOG.info(
        "Applying extractor %s from %s for endpoint %s",
        metadata.extract_fn, metadata.fn_module, metadata.endpoint_name
    )

    try:
        out = extractor(payload=payload, metadata=metadata_dict)
    except Exception as e:
        LOG.error("Extractor %s from %s failed for endpoint %s: %s", metadata.extract_fn, metadata.fn_module, metadata.endpoint_name, e)
        raise ValueError(f"Extractor execution failed: {e}")

    return out
