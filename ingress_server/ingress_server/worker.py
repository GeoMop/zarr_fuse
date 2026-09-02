import os
import signal
import shutil
import logging

import zarr_fuse as zf
import xarray as xr
from pathlib import Path
from collections.abc import Iterator

from .app_config import AppConfig
from .io import read_df_from_bytes, send_anomaly_email
from .io.time_filter import (
    ExtractedItem,
    make_extracted_item,
    partition_by_retention,
    sort_by_data_time,
    time_key_type_conflict,
)
from .models import MetadataModel

LOG = logging.getLogger(__name__)


def _move_tree_contents(src: Path, dst: Path) -> None:
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
            except Exception as exc:
                LOG.warning(
                    "os.replace failed for %s -> %s, falling back to copy2: %s",
                    s,
                    d,
                    exc,
                )
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
        return

    paths: list[Path] = []
    for root, _, files in os.walk(app_config.accepted_dir):
        for name in files:
            if name.endswith(".meta.json"):
                continue
            paths.append(Path(root) / name)

    for path in sorted(paths, key=lambda p: p.name):
        yield path


def _load_metadata(data_path: Path) -> MetadataModel:
    meta_path = data_path.with_suffix(data_path.suffix + ".meta.json")
    try:
        return MetadataModel.model_validate_json(meta_path.read_text(encoding="utf-8"))
    except Exception:
        LOG.exception("Failed to load metadata from %s", meta_path)
        raise


def _target_dirs_for(app_config: AppConfig, data_path: Path) -> tuple[Path, Path]:
    rel = data_path.relative_to(app_config.accepted_dir)
    return (app_config.success_dir / rel).parent, (app_config.failed_dir / rel).parent


def _resolve_target(root: zf.Node, metadata: MetadataModel) -> zf.Node:
    target = root

    for path_value in (metadata.target_node, metadata.node_path):
        if not path_value:
            continue

        for part in path_value.strip("/").split("/"):
            if part:
                target = target[part]

    return target


def _extract_one(app_config: AppConfig, data_path: Path) -> ExtractedItem:
    metadata = _load_metadata(data_path)

    schema_path = metadata.resolve_schema_path(app_config.config_dir)
    if not schema_path.exists():
        raise ValueError(f"No schema for endpoint {metadata.endpoint_name}: {schema_path}")

    payload = data_path.read_bytes()
    obj = read_df_from_bytes(
        payload=payload,
        metadata=metadata,
        config_dir=app_config.config_dir,
    )

    return make_extracted_item(data_path, metadata, schema_path, obj)


def _store_one(item: ExtractedItem) -> None:
    metadata = item.metadata

    try:
        root = zf.open_store(item.schema_path)
    except Exception:
        LOG.exception("Failed to open zarr store for schema %s", item.schema_path)
        raise

    target = _resolve_target(root, metadata)

    try:
        if isinstance(item.obj, xr.Dataset):
            target.merge_ds(item.obj)
        else:
            target.update(item.obj)
    except Exception:
        LOG.exception(
            "Failed to write object to target endpoint=%s target_node=%r node_path=%r",
            metadata.endpoint_name,
            metadata.target_node,
            metadata.node_path,
        )
        raise


def _process_one(app_config: AppConfig, data_path: Path) -> None:
    _store_one(_extract_one(app_config, data_path))


def _save_to_queue(src: Path, dst: Path) -> None:
    dst.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), str(dst / src.name))

    meta = src.with_suffix(src.suffix + ".meta.json")
    if meta.exists():
        shutil.move(str(meta), str(dst / meta.name))


def _move_to_failed(app_config: AppConfig, data_path: Path) -> None:
    _, failed_dir = _target_dirs_for(app_config, data_path)
    try:
        _save_to_queue(data_path, failed_dir)
    except Exception:
        LOG.exception("Failed to move %s to failed queue", data_path)


def _notify_anomalies(app_config: AppConfig, anomalies: list[dict]) -> None:
    """Email the anomalies this process has not reported yet. A held batch is
    re-examined on every poll, so the same anomaly must not be re-sent."""
    def key(anomaly: dict) -> str:
        return f"{anomaly['type']}|{anomaly['error']}|{anomaly['context']}"

    unreported = [a for a in anomalies if key(a) not in app_config.notified_anomalies]
    if not unreported:
        return

    app_config.notified_anomalies.update(key(a) for a in unreported)

    try:
        send_anomaly_email(smtp_config=app_config.smtp, anomalies=unreported)
    except Exception:
        LOG.exception("Failed to send data anomaly notification")


def _process_available_files(app_config: AppConfig) -> bool:
    progressed = False
    batch: list[ExtractedItem] = []
    anomalies: list[dict] = []

    # Phase 1: extract all accepted files; nothing is written to the store yet,
    # so an interrupted batch is safely re-extracted on the next pass.
    for data_path in list(_iter_accepted_files(app_config)):

        # Check for stop signal at the beginning of each loop iteration to allow graceful shutdown.
        if app_config.stop_event.is_set():
            break

        try:
            LOG.info("Extracting data %s", data_path)
            item = _extract_one(app_config, data_path)
            batch.append(item)

            if item.time_error:
                anomalies.append({
                    "type": "time_key",
                    "error": item.time_error,
                    "context": {
                        "file": item.data_path.name,
                        "endpoint": item.metadata.endpoint_name,
                    },
                })

        except ValueError as exc:
            LOG.warning("Processing rejected for %s: %s", data_path, exc)
            _move_to_failed(app_config, data_path)
            progressed = True

        except Exception:
            LOG.exception("Extraction failed for %s", data_path)
            _move_to_failed(app_config, data_path)
            progressed = True

    # Phase 2: filter — order the batch by the time of the data instead of
    # the time of the payload receipt.
    sorted_batch = sort_by_data_time(batch)
    if [item.data_path for item in sorted_batch] != [item.data_path for item in batch]:
        LOG.info(
            "Batch reordered by data time: %s",
            [item.data_path.name for item in sorted_batch],
        )

    type_conflict = time_key_type_conflict(sorted_batch)
    if type_conflict:
        LOG.error("Time key type conflict: %s", type_conflict)
        anomalies.append({
            "type": "time_key_type_conflict",
            "error": type_conflict,
            "context": {"batch_size": len(sorted_batch)},
        })

    _notify_anomalies(app_config, anomalies)

    # Phase 3a: hold back items that are not yet older than retention_time
    # relative to the newest data seen in this batch — they stay in
    # accepted/ and are re-checked on the next pass, once newer data has
    # actually arrived to age them out.
    ready_items, held_items = partition_by_retention(sorted_batch, app_config.base.retention_time)

    if held_items:
        LOG.info(
            "Holding %d item(s) until %s hour(s) newer data has arrived",
            len(held_items),
            app_config.base.retention_time,
        )
        LOG.debug("Held items: %s", [item.data_path.name for item in held_items])

    # Phase 3b: write the ready items to the zarr store in data-time order.
    for item in ready_items:

        if app_config.stop_event.is_set():
            break

        success_dir, _ = _target_dirs_for(app_config, item.data_path)

        try:
            LOG.info("Storing data %s", item.data_path)
            _store_one(item)
            _save_to_queue(item.data_path, success_dir)
            LOG.info("Processing succeeded for %s", item.data_path)
            progressed = True

        except ValueError as exc:
            LOG.warning("Processing rejected for %s: %s", item.data_path, exc)
            _move_to_failed(app_config, item.data_path)
            progressed = True

        except Exception:
            LOG.exception("Processing failed for %s", item.data_path)
            _move_to_failed(app_config, item.data_path)
            progressed = True

    return progressed


def working_loop(app_config: AppConfig, poll_sleep: float = 30.0) -> None:
    LOG.info("Worker loop started")

    while not app_config.stop_event.is_set():
        progressed = _process_available_files(app_config)

        if not progressed:
            app_config.stop_event.wait(timeout=poll_sleep)

    LOG.info("Worker loop stopped")


def startup_recover(app_config: AppConfig) -> None:
    if app_config.failed_dir.exists():
        LOG.info("Recovering: moving failed -> accepted")
        _move_tree_contents(app_config.failed_dir, app_config.accepted_dir)


def install_signal_handlers(app_config: AppConfig) -> None:
    def _on_term(_signum, _frame) -> None:
        LOG.info("SIGTERM received. Stopping worker…")
        app_config.stop_event.set()
    try:
        signal.signal(signal.SIGTERM, _on_term)
    except Exception:
        LOG.exception("Failed to install SIGTERM handler")
