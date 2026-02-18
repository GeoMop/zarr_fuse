import json
import logging
import polars as pl

from importlib import import_module
from .models import MetadataModel

LOG = logging.getLogger("extractor")

def resolve_extractor(metadata: MetadataModel):
    if not metadata.fn_module or not metadata.extract_fn:
        return None
    try:
        module = import_module(metadata.fn_module)
        return getattr(module, metadata.extract_fn)
    except Exception as e:
        LOG.error("Failed to resolve extractor %s from %s: %s", metadata.extract_fn, metadata.fn_module, e)
        return None

def apply_extractor_if_any(
    payload: bytes,
    metadata: MetadataModel,
) -> pl.DataFrame:
    extractor = resolve_extractor(metadata)
    if not extractor:
        LOG.info("No extractor for endpoint %s, skipping extraction", metadata.endpoint_name)
        raise ValueError("No extractor found")

    LOG.info("Applying extractor %s from %s for endpoint %s", metadata.extract_fn, metadata.fn_module, metadata.endpoint_name)
    result = extractor(payload, metadata)


    parsed_data = None
    try:
        parsed_data = json.loads(payload.decode("utf-8"))
    except Exception as e:
        LOG.error("Failed to parse JSON payload for endpoint %s: %s", metadata.endpoint_name, e)
        raise ValueError("Invalid JSON payload")

    if not parsed_data:
        LOG.error("Empty JSON payload for endpoint %s", metadata.endpoint_name)
        raise ValueError("Empty JSON payload")

    try:
        extracted_data = extractor(parsed_data, metadata.dataframe_row)
    except Exception as e:
        LOG.error("Extractor %s from %s failed for endpoint %s: %s", metadata.extract_fn, metadata.fn_module, metadata.endpoint_name, e)
        raise ValueError("Extractor function failed")

    return extracted_data
