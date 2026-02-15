import os
import signal
import shutil
import logging

from pathlib import Path

from .configs import STOP, get_settings
from .io_utils import open_root, read_df_from_bytes
from .models import MetadataModel

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


def _iter_accepted_files_in_dir(dir: Path):
    for root, _, files in os.walk(dir):
        for name in files:
            if name.endswith(".meta.json"):
                continue
            LOG.info("Found accepted file: %s", Path(root) / name)
            yield Path(root) / name


def _iter_accepted_files():
    settings = get_settings()

    if not settings.accepted_dir.exists():
        LOG.warning("Accepted directory does not exist: %s", settings.accepted_dir)
        yield from ()
        return
    yield from _iter_accepted_files_in_dir(settings.accepted_dir)


def _load_metadata(data_path: Path) -> tuple[MetadataModel | None, str | None]:
    LOG.info("Loading meta for %s", data_path)
    meta_path = data_path.with_suffix(data_path.suffix + ".meta.json")
    try:
        return MetadataModel.model_validate_json(meta_path.read_text(encoding="utf-8")), None
    except Exception as e:
        return None, f"Failed to load meta: {e}"


def _target_dirs_for(data_path: Path) -> tuple[Path, Path]:
    settings = get_settings()

    rel = data_path.relative_to(settings.accepted_dir)
    return (settings.success_dir / rel).parent, (settings.failed_dir / rel).parent


def _process_one(data_path: Path) -> str | None:
    metadata, err = _load_metadata(data_path)
    if err:
        return err
    assert metadata is not None

    schema_path = metadata.get_schema_path()
    if not schema_path.exists():
        return f"No schema for endpoint {metadata.endpoint_name}"

    df, err = read_df_from_bytes(
        payload= data_path.read_bytes(),
        metadata=metadata,
    )
    if err:
        return f"Failed to read DataFrame: {err}"

    root, err = open_root(schema_path)
    if err:
        return f"Failed to open root: {err}"

    if not metadata.node_path and metadata.schema_node:
        root[metadata.schema_node].update(df)
    elif not metadata.node_path and not metadata.schema_node:
        root.update(df)
    # TODO: handle more complex node paths (e.g. /a/b/c)
    elif not metadata.schema_node:
        root[metadata.node_path].update(df)
    else:
        root[metadata.schema_node][metadata.node_path].update(df)
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
    settings = get_settings()

    if settings.failed_dir.exists():
        LOG.info("Recovering: moving failed -> accepted")
        _move_tree_contents(settings.failed_dir, settings.accepted_dir)

def install_signal_handlers():
    def _on_term(signum, frame):
        LOG.info("SIGTERM received. Stopping worker…")
        STOP.set()
    try:
        signal.signal(signal.SIGTERM, _on_term)
    except Exception:
        pass
