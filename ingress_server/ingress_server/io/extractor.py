import sys
import logging

from pathlib import Path
from contextlib import contextmanager
from importlib import import_module
from collections.abc import Callable

from ..models import MetadataModel
from ..data_types import DataObject

LOG = logging.getLogger(__name__)


@contextmanager
def _temporary_sys_path(path: Path | None):
    if path is None:
        yield
        return

    path_str = str(path)
    inserted = False
    if path_str not in sys.path:
        sys.path.insert(0, path_str)
        inserted = True

    try:
        yield
    finally:
        if inserted:
            try:
                sys.path.remove(path_str)
            except ValueError:
                pass


def _resolve_extractor(metadata: MetadataModel, fallback_config_dir: Path | None = None) -> Callable[..., DataObject]:
    if not metadata.fn_module:
        raise ValueError(f"Missing fn_module for endpoint {metadata.endpoint_name}")

    if not metadata.extract_fn:
        raise ValueError(f"Missing extract_fn for endpoint {metadata.endpoint_name}")

    config_dir = metadata.config_dir or fallback_config_dir
    with _temporary_sys_path(config_dir):
        try:
            module = import_module(metadata.fn_module)
        except ModuleNotFoundError as exc:
            LOG.exception(
                "Failed to import extractor module %s for extractor %s "
                "(endpoint=%s, config_dir=%r, missing_module=%r)",
                metadata.fn_module,
                metadata.extract_fn,
                metadata.endpoint_name,
                config_dir,
                exc.name,
            )
            raise

    try:
        extractor = getattr(module, metadata.extract_fn)
    except AttributeError:
        LOG.exception(
            "Extractor function %s not found in module %s "
            "(endpoint=%s)",
            metadata.extract_fn,
            metadata.fn_module,
            metadata.endpoint_name,
        )
        raise

    if not callable(extractor):
        raise TypeError(
            f"Extractor {metadata.extract_fn} in module "
            f"{metadata.fn_module} is not callable"
        )

    return extractor


def apply_extractor(
    payload: bytes,
    metadata: MetadataModel,
    fallback_config_dir: Path | None = None,
) -> DataObject:
    extractor = _resolve_extractor(metadata, fallback_config_dir)
    metadata_dict = metadata.model_dump()

    LOG.info(
        "Applying extractor %s from %s for endpoint %s",
        metadata.extract_fn,
        metadata.fn_module,
        metadata.endpoint_name,
    )

    try:
        return extractor(payload=payload, metadata=metadata_dict)
    except Exception:
        LOG.exception(
            "Extractor %s from %s failed for endpoint %s",
            metadata.extract_fn,
            metadata.fn_module,
            metadata.endpoint_name,
        )
        raise
