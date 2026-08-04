import logging

from ..models import MetadataModel
from ..app_config import AppConfig
from ..queue_storage import new_msg_name
from .content_type import classify_content_type, get_content_type_suffix
from .validate import sanitize_node_path

LOG = logging.getLogger(__name__)


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

    updated_md = metadata.model_copy(update={"node_path": str(safe_child) if safe_child else None})

    content_type = classify_content_type(updated_md.content_type)
    if content_type is None:
        raise ValueError(f"Unsupported content type: {updated_md.content_type}")

    app_config.queue.put_item(
        endpoint_name=updated_md.endpoint_name,
        name=new_msg_name(get_content_type_suffix(content_type)),
        payload=payload,
        meta=updated_md.model_dump_json().encode("utf-8"),
    )
