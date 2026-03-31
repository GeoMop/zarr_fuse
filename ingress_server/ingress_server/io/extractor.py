import logging

from importlib import import_module
from collections.abc import Callable

from ..models import MetadataModel
from ..data_types import DataObject

LOG = logging.getLogger(__name__)


def _resolve_extractor(metadata: MetadataModel) -> Callable[..., DataObject]:
    try:
        # TODO SM: add the extractor's path
        module = import_module(metadata.fn_module)
        return getattr(module, metadata.extract_fn)
    except Exception:
        # TODO SM: catch ModuleNotFoundError
        # it is not catched reporting "ModuleNotFoundError: No module named 'inputs'"
        # TODO SM: Where 'inputs' comes from?
        LOG.exception(
            "Failed to resolve extractor %s from module %s",
            metadata.extract_fn,
            metadata.fn_module,
        )
        raise


def apply_extractor(
    payload: bytes,
    metadata: MetadataModel,
) -> DataObject:
    extractor = _resolve_extractor(metadata)
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
