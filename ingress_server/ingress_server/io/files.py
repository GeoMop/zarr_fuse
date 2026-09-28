import time
import uuid
import logging

from ..models import MetadataModel
from ..app_config import AppConfig
from ..local_cache import content_hash
from .content_type import classify_content_type, get_content_type_suffix
from .validate import sanitize_node_path

LOG = logging.getLogger(__name__)


def new_item_name(endpoint_name: str, suffix: str) -> str:
    """Flat name of a new queue item: ``<endpoint>_<UTC timestamp>_<uid><suffix>``."""
    ts = time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    uid = uuid.uuid4().hex[:12]
    return f"{endpoint_name}_{ts}_{uid}{suffix}"


def save_data(
    app_config: AppConfig,
    metadata: MetadataModel,
    payload: bytes,
) -> None:
    try:
        safe_child = sanitize_node_path(metadata.node_path)
    except ValueError:
        LOG.exception("Failed to sanitize node_path: %r", metadata.node_path)
        raise

    updated_md = metadata.model_copy(update={
        "node_path": str(safe_child) if safe_child else None,
        "sha256": content_hash(payload),
    })

    content_type = classify_content_type(updated_md.content_type)
    if content_type is None:
        raise ValueError(f"Unsupported content type: {updated_md.content_type}")

    name = new_item_name(updated_md.endpoint_name, get_content_type_suffix(content_type))

    # The queue storage is the source of truth, the local copy comes second
    # and only spares the worker a download.
    app_config.queue.put_item(
        name=name,
        payload=payload,
        meta=updated_md.model_dump_json().encode("utf-8"),
    )
    app_config.cache.put(name, payload)
