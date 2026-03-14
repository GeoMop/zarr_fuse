import os
import signal
import shutil
import logging

import zarr_fuse as zf
import xarray as xr
from pathlib import Path
from collections.abc import Iterator

from .app_config import AppConfig
from .io import open_root, read_df_from_bytes
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


def _iter_accepted_files(app_config: AppConfig) -> Iterator[Path]:
    if not app_config.accepted_dir.exists():
        LOG.warning("Accepted directory does not exist: %s", app_config.accepted_dir)
        yield from ()
        return

    paths: list[Path] = []
    for root, _, files in os.walk(app_config.accepted_dir):
        for name in files:
            if name.endswith(".meta.json"):
                continue
            paths.append(Path(root) / name)

    for path in sorted(paths, key=lambda p: p.name):
        LOG.info("Found accepted file: %s", path)
        yield path


def _load_metadata(data_path: Path) -> tuple[MetadataModel | None, str | None]:
    LOG.info("Loading meta for %s", data_path)
    meta_path = data_path.with_suffix(data_path.suffix + ".meta.json")
    try:
        return MetadataModel.model_validate_json(meta_path.read_text(encoding="utf-8")), None
    except Exception as e:
        return None, f"Failed to load meta: {e}"


def _target_dirs_for(app_config: AppConfig, data_path: Path) -> tuple[Path, Path]:
    rel = data_path.relative_to(app_config.accepted_dir)
    return (app_config.success_dir / rel).parent, (app_config.failed_dir / rel).parent


def _process_one(app_config: AppConfig, data_path: Path) -> str | None:
    metadata, err = _load_metadata(data_path)
    if err:
        return err
    if metadata is None:
        return "Metadata could not be loaded"

    schema_path = metadata.resolve_schema_path(app_config.config_dir)
    if not schema_path.exists():
        return f"No schema for endpoint {metadata.endpoint_name}"

    obj, err = read_df_from_bytes(
        payload=data_path.read_bytes(),
        metadata=metadata,
    )
    if err:
        return f"Failed to read DataFrame: {err}"

    root, err = zf.open_store(schema_path)
    if err:
        return f"Failed to open root: {err}"

    target = root
    for path_value in (metadata.target_node, metadata.node_path):
        if path_value:
            for part in path_value.strip("/").split("/"):
                if part:
                    target = target[part]

    if isinstance(obj, xr.Dataset):
        target.merge_ds(obj)
    else:
        target.update(obj)
    return None


def _save_to_queue(src: Path, dst: Path):
    dst.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), str(dst / src.name))

    meta = src.with_suffix(src.suffix + ".meta.json")
    if meta.exists():
        shutil.move(str(meta), str(dst / meta.name))


def working_loop(app_config: AppConfig, poll_sleep: float = 30.0):
    LOG.info("Worker loop started")

    while not app_config.stop_event.is_set():
        progressed = False

        for data_path in list(_iter_accepted_files(app_config)):
            if app_config.stop_event.is_set():
                break
            success_dir, failed_dir = _target_dirs_for(app_config, data_path)

            try:
                LOG.info("Processing data %s", data_path)
                err = _process_one(app_config, data_path)
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
        if progressed:
            LOG.info("One iteration of processing done, checking for more files immediately")
        else:
            app_config.stop_event.wait(timeout=poll_sleep)
    LOG.info("Worker loop stopped")


def startup_recover(app_config: AppConfig):
    if app_config.failed_dir.exists():
        LOG.info("Recovering: moving failed -> accepted")
        _move_tree_contents(app_config.failed_dir, app_config.accepted_dir)


def install_signal_handlers(app_config: AppConfig):
    def _on_term(_signum, _frame):
        LOG.info("SIGTERM received. Stopping worker…")
        app_config.stop_event.set()
    try:
        signal.signal(signal.SIGTERM, _on_term)
    except Exception:
        pass
