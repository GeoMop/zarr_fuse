import os
import time
import uuid
import logging

from pathlib import Path

from ..models import MetadataModel
from .content_type import classify_content_type, get_content_type_suffix
from ..app_config import AppConfig
from .validate import sanitize_node_path

LOG = logging.getLogger("io.files")


def _atomic_write(path: Path, data: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)


def _new_msg_path(base: Path, suffix: str) -> Path:
    ts = time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    uid = uuid.uuid4().hex[:12]
    return base / f"{ts}_{uid}{suffix}"


def save_data_and_metadata(meta_data: MetadataModel, payload: bytes, base: Path) -> str | None:
    content_type = classify_content_type(meta_data.content_type)
    if content_type is None:
        return f"Unsupported content type: {meta_data.content_type}"

    suffix = get_content_type_suffix(content_type)
    if suffix is None:
        return f"Cannot determine file suffix for content type: {meta_data.content_type}"

    msg_path = _new_msg_path(base, suffix)

    try:
        _atomic_write(msg_path, payload)
    except Exception as e:
        return f"Failed to save data to {msg_path}: {e}"

    try:
        meta_path = msg_path.parent / f"{msg_path.name}.meta.json"
        _atomic_write(meta_path, meta_data.model_dump_json().encode("utf-8"))
    except Exception as e:
        return f"Failed to save metadata to {meta_path}: {e}"
    return None


def save_data(
    app_config: AppConfig,
    metadata: MetadataModel,
    payload: bytes,
) -> str | None:
    try:
        safe_child = sanitize_node_path(metadata.node_path)
    except ValueError as e:
        return f"Failed to sanitize node_path: {metadata.node_path}. Error: {e}"

    meta_data = metadata.model_copy(update={"node_path": str(safe_child) if safe_child else None})

    base = app_config.accepted_dir / meta_data.endpoint_name
    err = save_data_and_metadata(
        meta_data=meta_data,
        payload=payload,
        base=base,
    )
    if err:
        return f"Failed to save data and metadata: {err}"
    return None
