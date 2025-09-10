import os
import json
import signal
import shutil
import logging

from pathlib import Path
from .configs import ACCEPTED_DIR, FAILED_DIR, SUCCESS_DIR, ENDPOINT_NAME_TO_SCHEMA, STOP
from .io_utils import open_root, read_df_from_bytes


LOG = logging.getLogger("worker")


def _move_tree_contents(src: Path, dst: Path):
    if not src.exists():
        LOG.warning("Source directory does not exist: %s", src)
        return

    dst.mkdir(parents=True, exist_ok=True)
    for item in src.iterdir():
        shutil.move(str(item), str(dst / item.name))


def _iter_accepted_files():
    if not ACCEPTED_DIR.exists():
        LOG.warning("Accepted directory does not exist: %s", ACCEPTED_DIR)
        return

    for root, _, files in os.walk(ACCEPTED_DIR):
        for name in files:
            if name.endswith(".meta.json"):
                continue
            LOG.info("Found accepted file: %s", Path(root) / name)
            yield Path(root) / name


def _load_meta(data_path: Path) -> dict:
    LOG.info("Loading meta for %s", data_path)
    meta_path = data_path.with_suffix(data_path.suffix + ".meta.json")
    try:
        LOG.info("Loading meta from %s", meta_path)
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        LOG.warning("Failed to load meta for %s", data_path)
        return {"content_type": "application/json", "node_path": "", "endpoint_name": data_path.parts[data_path.parts.index("accepted") + 1]}


def _target_dirs_for(data_path: Path) -> tuple[Path, Path]:
    rel = data_path.relative_to(ACCEPTED_DIR)
    return (SUCCESS_DIR / rel).parent, (FAILED_DIR / rel).parent


def _process_one(data_path: Path):
    meta = _load_meta(data_path)
    endpoint_name = meta.get("endpoint_name", "")
    node_path = meta.get("node_path", "")
    content_type = meta.get("content_type", "application/json")

    schema_path = ENDPOINT_NAME_TO_SCHEMA.get(endpoint_name)
    if not schema_path:
        LOG.warning("No schema_path mapping for endpoint_name=%s", endpoint_name)
        raise RuntimeError(f"No schema_path mapping for endpoint_name={endpoint_name}")

    payload = data_path.read_bytes()
    df = read_df_from_bytes(payload, content_type)

    root = open_root(schema_path)
    if not node_path:
        root.update(df)
    else:
        root[node_path].update(df)


def _save_to_queue(src: Path, dst: Path):
    dst.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), str(dst / src.name))

    meta = src.with_suffix(src.suffix + ".meta.json")
    if meta.exists():
        shutil.move(str(meta), str(dst / meta.name))


def working_loop(poll_sleep: float = 30.0):
    LOG.info("Worker loop started")

    while not STOP.is_set():
        progressed = False

        for data_path in list(_iter_accepted_files()):
            if STOP.is_set():
                break
            success_dir, failed_dir = _target_dirs_for(data_path)

            try:
                LOG.info("Processing data %s", data_path)
                _process_one(data_path)
            except Exception as e:
                LOG.exception("Processing failed for %s: %s", data_path, e)
                _save_to_queue(data_path, failed_dir)
            else:
                LOG.info("Processing succeeded for %s", data_path)
                _save_to_queue(data_path, success_dir)

            progressed = True
        if not progressed:
            STOP.wait(timeout=poll_sleep)
    LOG.info("Worker loop stopped")


def startup_recover():
    if FAILED_DIR.exists():
        LOG.info("Recovering: moving failed -> accepted")
        _move_tree_contents(FAILED_DIR, ACCEPTED_DIR)

def install_signal_handlers():
    def _on_term(signum, frame):
        LOG.info("SIGTERM received. Stopping worker…")
        STOP.set()
    try:
        signal.signal(signal.SIGTERM, _on_term)
    except Exception:
        pass
