import signal
import logging
import warnings

import zarr_fuse as zf
import xarray as xr
from pathlib import Path

from .app_config import AppConfig
from .io import read_df_from_bytes, send_anomaly_email
from .io.time_filter import (
    ExtractedItem,
    make_extracted_item,
    partition_by_retention,
    sort_by_data_time,
    time_key_type_conflict,
)
from .local_cache import content_hash
from .models import MetadataModel
from .queue_storage import ACCEPTED, FAILED, SUCCESS, FileRef

LOG = logging.getLogger(__name__)


def _load_metadata(app_config: AppConfig, ref: FileRef) -> MetadataModel:
    try:
        return MetadataModel.model_validate_json(app_config.queue.read_meta_text(ref))
    except Exception:
        LOG.exception("Failed to load metadata for %s", ref)
        raise


def _resolve_target(root: zf.Node, metadata: MetadataModel) -> zf.Node:
    target = root

    for path_value in (metadata.target_node, metadata.node_path):
        if not path_value:
            continue

        for part in path_value.strip("/").split("/"):
            if part:
                target = target[part]

    return target


def _read_local_file(data_path: Path) -> tuple[MetadataModel, bytes]:
    """Read a payload + metadata sidecar directly from the local filesystem,
    bypassing the queue storage (used by tests and ad-hoc processing)."""
    meta_path = data_path.with_suffix(data_path.suffix + ".meta.json")
    try:
        metadata = MetadataModel.model_validate_json(meta_path.read_text(encoding="utf-8"))
    except Exception:
        LOG.exception("Failed to load metadata from %s", meta_path)
        raise
    return metadata, data_path.read_bytes()


def _extract_one(app_config: AppConfig, ref: FileRef | Path) -> ExtractedItem:
    """
    Read a payload with its metadata and extract the data object out of it.

    Passing a local `Path` instead of a queue `FileRef` is deprecated; put the
    payload into the queue instead.
    """
    if isinstance(ref, Path):
        warnings.warn(
            "Extracting a payload from a local path is deprecated, use a queue FileRef.",
            DeprecationWarning,
            stacklevel=2,
        )
        metadata, payload = _read_local_file(ref)
    else:
        metadata = _load_metadata(app_config, ref)
        payload = _read_payload(app_config, ref, metadata.sha256)

    schema_path = metadata.resolve_schema_path(app_config.config_dir)
    if not schema_path.exists():
        raise ValueError(f"No schema for endpoint {metadata.endpoint_name}: {schema_path}")

    obj = read_df_from_bytes(
        payload=payload,
        metadata=metadata,
        config_dir=app_config.config_dir,
    )

    return make_extracted_item(FileRef(str(ref)), metadata, schema_path, obj)


def _read_payload(app_config: AppConfig, ref: FileRef, sha256: str | None) -> bytes:
    name = _item_name(ref)
    payload = app_config.cache.get(name, sha256)
    if payload is None:
        payload = app_config.queue.read_bytes(ref)
        if sha256 and content_hash(payload) != sha256:
            raise ValueError(f"Content hash of {ref} does not match its metadata")
        app_config.cache.put(name, payload)
    return payload


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
    """Process a single local file. Deprecated, see `_extract_one`."""
    _store_one(_extract_one(app_config, data_path))


def _item_name(ref: FileRef) -> str:
    return ref.partition("/")[2]


def _outcome(item: ExtractedItem, state: str, error: str = "") -> dict:
    return {
        "item_name": _item_name(item.ref),
        "source": item.metadata.endpoint_name,
        "state": state,
        "sha256": item.metadata.sha256 or "",
        "data_time": "" if item.time_key is None else str(item.time_key),
        "error": error,
    }


def _sync_manifest(app_config: AppConfig) -> list[FileRef]:
    """Record the new accepted items, move the items the manifest marks as done; return the rest."""
    states = app_config.manifest.states()
    pending = []
    for ref in app_config.queue.list_accepted():
        state = states.get(_item_name(ref))
        if state in (SUCCESS, FAILED):
            app_config.queue.move(ref, state)
        else:
            pending.append(ref)

    app_config.manifest.add([
        {"item_name": _item_name(ref), "state": ACCEPTED}
        for ref in pending if _item_name(ref) not in states
    ])
    return pending


def _finish(app_config: AppConfig, done: list[tuple[FileRef, dict]]) -> bool:
    """Record the outcomes in the manifest, then move the items to the queue of their state."""
    app_config.manifest.add([row for _, row in done])
    for ref, row in done:
        try:
            app_config.queue.move(ref, row["state"])
            app_config.cache.evict(_item_name(ref))
        except Exception:
            LOG.exception("Failed to move %s to the %s queue", ref, row["state"])
    return bool(done)


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
    """
    Run one pass over the accepted queue and report whether at least one item
    left it. An item that can be neither stored nor moved to failed/ is not
    progress: counting it as such would make `working_loop` skip its sleep and
    re-list the whole queue forever. Items held back by the retention window
    are not progress either — they are waiting for newer data, not for the CPU.
    """
    done: list[tuple[FileRef, dict]] = []
    batch: list[ExtractedItem] = []
    anomalies: list[dict] = []

    # Phase 1: extract all accepted files; nothing is written to the store yet,
    # so an interrupted batch is safely re-extracted on the next pass.
    for ref in _sync_manifest(app_config):

        # Check for stop signal at the beginning of each loop iteration to allow graceful shutdown.
        if app_config.stop_event.is_set():
            break

        try:
            LOG.info("Extracting data %s", ref)
            item = _extract_one(app_config, ref)
            batch.append(item)

            if item.time_error:
                anomalies.append({
                    "type": "time_key",
                    "error": item.time_error,
                    "context": {
                        "file": item.ref,
                        "endpoint": item.metadata.endpoint_name,
                    },
                })

        except ValueError as exc:
            LOG.warning("Processing rejected for %s: %s", ref, exc)
            done.append((ref, {"item_name": _item_name(ref), "state": FAILED, "error": str(exc)}))

        except Exception as exc:
            LOG.exception("Extraction failed for %s", ref)
            done.append((ref, {"item_name": _item_name(ref), "state": FAILED, "error": str(exc)}))

    # Phase 2: filter — order the batch by the time of the data instead of
    # the time of the payload receipt.
    sorted_batch = sort_by_data_time(batch)
    if [item.ref for item in sorted_batch] != [item.ref for item in batch]:
        LOG.info("Batch reordered by data time: %s", [item.ref for item in sorted_batch])

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
        LOG.debug("Held items: %s", [item.ref for item in held_items])

    # Phase 3b: write the ready items to the zarr store in data-time order.
    for item in ready_items:

        if app_config.stop_event.is_set():
            break

        try:
            LOG.info("Storing data %s", item.ref)
            _store_one(item)
            done.append((item.ref, _outcome(item, SUCCESS)))
            LOG.info("Processing succeeded for %s", item.ref)

        except ValueError as exc:
            LOG.warning("Processing rejected for %s: %s", item.ref, exc)
            done.append((item.ref, _outcome(item, FAILED, str(exc))))

        except Exception as exc:
            LOG.exception("Processing failed for %s", item.ref)
            done.append((item.ref, _outcome(item, FAILED, str(exc))))

    return _finish(app_config, done)


def working_loop(app_config: AppConfig, poll_sleep: float = 30.0) -> None:
    LOG.info("Worker loop started")

    while not app_config.stop_event.is_set():
        try:
            progressed = _process_available_files(app_config)
        except Exception:
            LOG.exception("Worker pass failed")
            progressed = False

        if not progressed:
            app_config.stop_event.wait(timeout=poll_sleep)

    LOG.info("Worker loop stopped")


def startup_recover(app_config: AppConfig) -> None:
    # Re-assert the layout instead of trusting the one load_app_config checked:
    # the queue may have been emptied or re-created since, and on S3 this is
    # also where a credential or permission problem surfaces before polling.
    app_config.queue.ensure_layout()

    LOG.info("Recovering: moving failed -> accepted")
    failed = [name for name, state in app_config.manifest.states().items() if state == FAILED]
    app_config.manifest.add([{"item_name": name, "state": ACCEPTED} for name in failed])
    app_config.queue.recover_failed()


def install_signal_handlers(app_config: AppConfig) -> None:
    def _on_term(_signum, _frame) -> None:
        LOG.info("SIGTERM received. Stopping worker…")
        app_config.stop_event.set()
    try:
        signal.signal(signal.SIGTERM, _on_term)
    except Exception:
        LOG.exception("Failed to install SIGTERM handler")
