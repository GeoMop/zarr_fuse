import json
import logging
import polars as pl

from importlib import import_module

LOG = logging.getLogger("extractor")

def resolve_extractor(fn_module: str, extract_fn: str):
    if not fn_module or not extract_fn:
        return None
    try:
        module = import_module(fn_module)
        return getattr(module, extract_fn)
    except Exception as e:
        LOG.error("Failed to resolve extractor %s from %s: %s", extract_fn, fn_module, e)
        return None

def apply_extractor_if_any(
    endpoint_name: str,
    payload: bytes,
    extract_fn: str,
    fn_module: str,
) -> pl.DataFrame:
    extractor = resolve_extractor(fn_module, extract_fn)
    if not extractor:
        LOG.info("No extractor for endpoint %s, skipping extraction", endpoint_name)
        raise ValueError("No extractor found")

    LOG.info("Applying extractor %s from %s for endpoint %s", extract_fn, fn_module, endpoint_name)

    parsed_data = None
    try:
        parsed_data = json.loads(payload.decode("utf-8"))
    except Exception as e:
        LOG.error("Failed to parse JSON payload for endpoint %s: %s", endpoint_name, e)
        raise ValueError("Invalid JSON payload")

    if not parsed_data:
        LOG.error("Empty JSON payload for endpoint %s", endpoint_name)
        raise ValueError("Empty JSON payload")

    try:
        extracted_data = extractor(parsed_data)
    except Exception as e:
        LOG.error("Extractor %s from %s failed for endpoint %s: %s", extract_fn, fn_module, endpoint_name, e)
        raise ValueError("Extractor function failed")

    return extracted_data
