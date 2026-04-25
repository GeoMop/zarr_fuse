import os
import time
import uuid
import logging

from pathlib import Path

from ..models import MetadataModel
from .content_type import classify_content_type, get_content_type_suffix
from ..app_config import AppConfig
from .validate import sanitize_node_path

LOG = logging.getLogger(__name__)


def _atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)


def _new_msg_path(base: Path, suffix: str) -> Path:
    ts = time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    uid = uuid.uuid4().hex[:12]
    return base / f"{ts}_{uid}{suffix}"


def save_data_and_metadata(
    metadata: MetadataModel,
    payload: bytes,
    base: Path
) -> None:
    content_type = classify_content_type(metadata.content_type)
    if content_type is None:
        raise ValueError(f"Unsupported content type: {metadata.content_type}")

    suffix = get_content_type_suffix(content_type)
    msg_path = _new_msg_path(base, suffix)
    meta_path = msg_path.parent / f"{msg_path.name}.meta.json"

    try:
        _atomic_write(msg_path, payload)
    except Exception:
        LOG.exception("Failed to save data to %s", msg_path)
        raise

    try:
        _atomic_write(meta_path, metadata.model_dump_json().encode("utf-8"))
    except Exception:
        LOG.exception("Failed to save metadata to %s", meta_path)
        raise


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

    base = app_config.accepted_dir / updated_md.endpoint_name
    save_data_and_metadata(
        metadata=updated_md,
        payload=payload,
        base=base,
    )
