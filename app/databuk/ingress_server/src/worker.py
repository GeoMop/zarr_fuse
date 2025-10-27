import os
import json
import signal
import shutil
import logging

from pathlib import Path
from configs import ACCEPTED_DIR, FAILED_DIR, SUCCESS_DIR, STOP
from io_utils import open_root, read_df_from_bytes

LOG = logging.getLogger("worker")

def _move_tree_contents(src: Path, dst: Path):
    if not src.exists():
        LOG.warning("Source directory does not exist: %s", src)
        return

    dst.mkdir(parents=True, exist_ok=True)
    for root, _, files in os.walk(src, topdown=False):
        root_p = Path(root)
        rel = root_p.relative_to(src)
        target_root = dst / rel
        target_root.mkdir(parents=True, exist_ok=True)
        for name in files:
            s = root_p / name
            d = target_root / name
            d.parent.mkdir(parents=True, exist_ok=True)
            try:
                os.replace(s, d)
            except Exception:
                shutil.copy2(s, d)
                s.unlink(missing_ok=True)
        if root_p != src:
            try:
                root_p.rmdir()
            except OSError:
                pass

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


def _load_meta(data_path: Path) -> tuple[dict, str | None]:
    LOG.info("Loading meta for %s", data_path)
    meta_path = data_path.with_suffix(data_path.suffix + ".meta.json")
    try:
        return json.loads(meta_path.read_text(encoding="utf-8")), None
    except Exception as e:
        return {}, f"Failed to load meta: {e}"


def _target_dirs_for(data_path: Path) -> tuple[Path, Path]:
    rel = data_path.relative_to(ACCEPTED_DIR)
    return (SUCCESS_DIR / rel).parent, (FAILED_DIR / rel).parent


def _process_one(data_path: Path) -> str | None:
    meta, err = _load_meta(data_path)
    if err:
        return err

    endpoint_name = meta.get("endpoint_name", "")
    node_path = meta.get("node_path", "")
    content_type = meta.get("content_type", "application/json")
    schema_path = meta.get("schema_path")
    if not schema_path:
        return f"No schema for endpoint {endpoint_name}"

    payload = data_path.read_bytes()
    df, err = read_df_from_bytes(payload, content_type)
    if err:
        return f"Failed to read DataFrame: {err}"

    root, err = open_root(schema_path)
    if err:
        return f"Failed to open root: {err}"

    if not node_path:
        root.update(df)
    else:
        root[node_path].update(df)
    return None


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
                err = _process_one(data_path)
                if err:
                    LOG.error("Processing failed for %s: %s", data_path, err)
                    _save_to_queue(data_path, failed_dir)
                else:
                    LOG.info("Processing succeeded for %s", data_path)
                    _save_to_queue(data_path, success_dir)
            except Exception as e:
                LOG.exception("Processing failed for %s: %s", data_path, e)
                _save_to_queue(data_path, failed_dir)

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
