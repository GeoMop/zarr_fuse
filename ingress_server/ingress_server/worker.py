import json
import signal
import logging
import warnings

import zarr_fuse as zf
import xarray as xr
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from .app_config import AppConfig
from .io import read_df_from_bytes, send_anomaly_email
from .io.time_filter import (
    ExtractedItem,
    format_time_key,
    make_extracted_item,
    partition_by_retention,
    sort_by_data_time,
    time_key_type_conflict,
)
from .local_cache import content_hash
from .manifest import STR_WIDTHS, ManifestEntry, receipt_key, validate_entry
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
    Read a payload with its metadata sidecar and extract the data object out
    of it. The worker pass reads registered items from the manifest instead
    (see `_process_available_files`).

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
        payload = app_config.queue.read_bytes(ref)

    return _extract_payload(app_config, FileRef(str(ref)), metadata, payload)


def _extract_payload(
    app_config: AppConfig,
    ref: FileRef,
    metadata: MetadataModel,
    payload: bytes,
) -> ExtractedItem:
    """Extract the data object out of a payload."""
    schema_path = metadata.resolve_schema_path(app_config.config_dir)
    if not schema_path.exists():
        raise ValueError(f"No schema for endpoint {metadata.endpoint_name}: {schema_path}")

    obj = read_df_from_bytes(
        payload=payload,
        metadata=metadata,
        config_dir=app_config.config_dir,
    )

    return make_extracted_item(ref, metadata, schema_path, obj)


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


def _move_to_failed(app_config: AppConfig, ref: FileRef) -> bool:
    """Park a rejected item in failed/; return whether it left the accepted queue."""
    try:
        app_config.queue.move(ref, FAILED)
        return True
    except Exception:
        LOG.exception("Failed to move %s to failed queue", ref)
        return False


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


def _item_name(ref: FileRef) -> str:
    """Name of a queue item, i.e. its ref without the queue folder."""
    return ref.partition("/")[2]


def _accepted_ref(entry: ManifestEntry) -> FileRef:
    return FileRef(f"{ACCEPTED}/{entry.item_name}")


def _failed(entry: ManifestEntry, exc: Exception) -> ManifestEntry:
    """The entry marked failed, keeping as much of the reason as fits the manifest."""
    width = STR_WIDTHS["error"]
    error = f"{type(exc).__name__}: {exc}"
    if len(error) > width:
        error = error[:width - 3] + "..."
    return entry.with_state(FAILED, error)


def _move_item(app_config: AppConfig, ref: FileRef, state: str) -> bool:
    """
    Move an accepted item to the queue folder of its terminal state and drop
    its local copy; best effort. Only a known terminal state names a folder:
    a damaged manifest row must not move the payload out of every queue.
    """
    if state not in (SUCCESS, FAILED):
        LOG.error("Not moving %s: manifest state %r is not a terminal state", ref, state)
        return False

    try:
        app_config.queue.move(ref, state)
    except Exception:
        LOG.exception("Failed to move %s to the %s queue", ref, state)
        return False

    app_config.cache.evict(_item_name(ref))
    return True


def _park_in_failed(app_config: AppConfig, ref: FileRef) -> bool:
    """Move an item that gets no manifest entry to failed/ and drop its local copy."""
    app_config.cache.evict(_item_name(ref))
    return _move_to_failed(app_config, ref)


def _manifest_metadata(metadata: MetadataModel) -> str:
    """
    Metadata JSON kept in the manifest. When it does not fit the manifest
    variable, the unbounded `dataframe_row` is left out and the worker reads
    it from the sidecar instead (see `_item_metadata`).
    """
    full = metadata.model_dump_json()
    if len(full) <= STR_WIDTHS["metadata"]:
        return full
    return metadata.model_dump_json(exclude={"dataframe_row"})


def _item_metadata(app_config: AppConfig, entry: ManifestEntry) -> MetadataModel:
    """Metadata of a pending item: the manifest copy, or the sidecar if that copy is reduced."""
    data = json.loads(entry.metadata)
    if "dataframe_row" in data:
        return MetadataModel.model_validate(data)
    return _load_metadata(app_config, _accepted_ref(entry))


def _data_time_text(value, ref: FileRef) -> str:
    """Text form of a data time for the manifest, empty if it does not fit."""
    text = format_time_key(value)
    if len(text) > STR_WIDTHS["data_time_min"]:
        LOG.warning("Data time %r of %s does not fit the manifest, left empty", text, ref)
        return ""
    return text


def _new_entry(ref: FileRef, metadata: MetadataModel) -> ManifestEntry:
    """
    Manifest entry of a newly received item, keyed by its receipt second;
    `ManifestStore.register` makes the key unique. Raise ValueError if the
    item does not fit the manifest.
    """
    try:
        key = receipt_key(metadata.received_at)
    except ValueError:
        LOG.warning(
            "Unreadable received_at %r of %s, registered at the current time",
            metadata.received_at,
            ref,
        )
        key = receipt_key(datetime.now(timezone.utc).isoformat())

    entry = ManifestEntry(
        key=key,
        item_name=_item_name(ref),
        source=metadata.endpoint_name,
        state=ACCEPTED,
        sha256=metadata.sha256 or "",
        metadata=_manifest_metadata(metadata),
    )
    validate_entry(entry)
    return entry


def _admit_new_items(
    app_config: AppConfig,
    refs: list[FileRef],
    with_meta: set[FileRef],
) -> tuple[bool, list[ManifestEntry]]:
    """
    Handle the accepted payloads that are not pending in the manifest. New
    items are registered. Items registered before, whose move to the folder
    of their recorded state was interrupted, are moved there now. A payload
    whose metadata cannot be read or does not fit the manifest, and a payload
    without a sidecar unknown to the manifest, are parked in failed/ without
    a manifest entry.

    Return whether a payload left the accepted queue, and the new entries.
    """
    progressed = False
    entries: list[ManifestEntry] = []
    without_meta: list[FileRef] = []

    for ref in refs:
        if app_config.stop_event.is_set():
            break

        if ref not in with_meta:
            without_meta.append(ref)
            continue

        try:
            entries.append(_new_entry(ref, _load_metadata(app_config, ref)))
        except Exception as exc:
            LOG.error("Cannot register %s, moving it to the failed queue: %s", ref, exc)
            progressed |= _park_in_failed(app_config, ref)

    registered, known = app_config.manifest.register(entries)
    for entry in registered:
        LOG.info("Registered %s under %s", entry.item_name, entry.key)

    # Without a sidecar the receipt time is unknown, so the whole manifest is scanned.
    known.update(app_config.manifest.find({_item_name(ref) for ref in without_meta}))
    for ref in without_meta:
        if _item_name(ref) not in known:
            LOG.warning("%s has neither metadata nor a manifest entry, moving it to the failed queue", ref)
            progressed |= _park_in_failed(app_config, ref)

    for name, entry in known.items():
        if entry.state == ACCEPTED:
            continue
        ref = FileRef(f"{ACCEPTED}/{name}")
        LOG.info("Moving %s to its recorded %s queue", ref, entry.state)
        progressed |= _move_item(app_config, ref, entry.state)

    return progressed, registered


def _read_payload(app_config: AppConfig, entry: ManifestEntry) -> bytes:
    """
    Payload of a pending item: the local copy when its content hash matches
    the manifest, the queue storage otherwise. A payload found in another
    queue folder than accepted/ (an interrupted recovery) is moved back first.
    """
    payload = app_config.cache.get(entry.item_name, entry.sha256)
    if payload is not None:
        return payload

    ref = _accepted_ref(entry)
    try:
        payload = app_config.queue.read_bytes(ref)
    except FileNotFoundError:
        found = app_config.queue.find_item(entry.item_name)
        if found is None:
            raise ValueError(f"Payload of {entry.item_name} is in none of the queues")
        LOG.warning("Payload of pending %s found in %s, moving it back", entry.item_name, found)
        app_config.queue.relocate(found, ref)
        payload = app_config.queue.read_bytes(ref)

    if entry.sha256 and content_hash(payload) != entry.sha256:
        raise ValueError(f"Content hash of {ref} does not match the manifest")

    app_config.cache.put(entry.item_name, payload)
    return payload


def _finish(app_config: AppConfig, updates: list[ManifestEntry]) -> bool:
    """
    Record the changed entries, then move the items that reached a terminal
    state to the queue folder of that state and drop their local copies. The
    manifest goes first as the source of truth; a payload left behind by an
    interrupted move is moved by a later pass (see `_admit_new_items`).
    Return whether an item left the accepted state.
    """
    if not updates:
        return False

    try:
        app_config.manifest.record(updates)
    except Exception:
        # `record` may have written some runs of rows already; a later pass
        # moves the payloads of the entries it did record.
        LOG.exception("Failed to record %d manifest update(s)", len(updates))
        return False

    progressed = False
    for entry in updates:
        if entry.state == ACCEPTED:
            continue
        progressed = True
        _move_item(app_config, _accepted_ref(entry), entry.state)

    return progressed


def _process_available_files(app_config: AppConfig) -> bool:
    """
    Run one pass over the queue and report whether at least one item left the
    accepted state. The manifest drives the pass (see manifest.py): the
    accepted payloads without a pending entry are admitted first, then the
    pending manifest entries are processed.

    An item that fails to be recorded as done is not progress: counting it as
    such would make `working_loop` skip its sleep and redo the pass forever.
    Items held back by the retention window are not progress either — they
    are waiting for newer data, not for the CPU.
    """
    # Phase 0: register the newly received items and reconcile the folders.
    refs, with_meta = app_config.queue.scan_accepted()
    pending = app_config.manifest.pending()
    pending_names = {entry.item_name for entry in pending}

    progressed, registered = _admit_new_items(
        app_config,
        [ref for ref in refs if _item_name(ref) not in pending_names],
        with_meta,
    )
    pending = sorted(pending + registered, key=lambda entry: entry.key)

    # Changed manifest entries by item name, recorded at the end of the pass.
    updates: dict[str, ManifestEntry] = {}
    entries: dict[FileRef, ManifestEntry] = {}
    batch: list[ExtractedItem] = []
    anomalies: list[dict] = []

    # Phase 1: extract all pending items; nothing is written to the store yet,
    # so an interrupted batch is safely re-extracted on the next pass.
    for entry in pending:

        # Check for stop signal at the beginning of each loop iteration to allow graceful shutdown.
        if app_config.stop_event.is_set():
            break

        ref = _accepted_ref(entry)
        try:
            LOG.info("Extracting data %s", ref)
            metadata = _item_metadata(app_config, entry)
            item = _extract_payload(app_config, ref, metadata, _read_payload(app_config, entry))

        except ValueError as exc:
            LOG.warning("Processing rejected for %s: %s", ref, exc)
            updates[entry.item_name] = _failed(entry, exc)
            continue

        except Exception as exc:
            LOG.exception("Extraction failed for %s", ref)
            updates[entry.item_name] = _failed(entry, exc)
            continue

        batch.append(item)
        entries[ref] = entry

        data_time = (_data_time_text(item.time_key, ref), _data_time_text(item.time_max, ref))
        if data_time != (entry.data_time_min, entry.data_time_max):
            entries[ref] = replace(entry, data_time_min=data_time[0], data_time_max=data_time[1])
            updates[entry.item_name] = entries[ref]

        if item.time_error:
            anomalies.append({
                "type": "time_key",
                "error": item.time_error,
                "context": {
                    "file": item.ref,
                    "endpoint": item.metadata.endpoint_name,
                },
            })

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
    # relative to the newest data seen in this batch — they stay pending and
    # are re-checked on the next pass, once newer data has actually arrived
    # to age them out.
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

        entry = entries[item.ref]
        try:
            LOG.info("Storing data %s", item.ref)
            _store_one(item)
            updates[entry.item_name] = entry.with_state(SUCCESS)
            LOG.info("Processing succeeded for %s", item.ref)

        except ValueError as exc:
            LOG.warning("Processing rejected for %s: %s", item.ref, exc)
            updates[entry.item_name] = _failed(entry, exc)

        except Exception as exc:
            LOG.exception("Processing failed for %s", item.ref)
            updates[entry.item_name] = _failed(entry, exc)

    # Phase 4: record the outcome, then let the queue folders follow.
    progressed |= _finish(app_config, list(updates.values()))
    return progressed


def _recover_failed(app_config: AppConfig) -> None:
    """
    Retry the failed items: reset their manifest entries to accepted, move
    the failed/ folder back (items without an entry are retried as well), and
    drop the local copies of items no longer in accepted/.

    Runs in the worker thread, not in the server startup: on a large manifest
    it may take longer than the startup probe allows.
    """
    LOG.info("Recovering: moving failed -> accepted")
    try:
        failed = app_config.manifest.failed()
        app_config.manifest.record([entry.with_state(ACCEPTED) for entry in failed])
    except Exception:
        # The folders are still recovered: a registered item moved back to
        # accepted/ while its entry stays failed is moved to failed/ again by
        # the next pass (see `_admit_new_items`).
        LOG.exception("Failed to reset the failed manifest entries")

    # The folders follow the manifest.
    app_config.queue.recover_failed()

    app_config.cache.sweep(keep={_item_name(ref) for ref in app_config.queue.list_accepted()})


def working_loop(app_config: AppConfig, poll_sleep: float = 30.0) -> None:
    LOG.info("Worker loop started")

    try:
        _recover_failed(app_config)
    except Exception:
        LOG.exception("Recovery of the failed items failed")

    while not app_config.stop_event.is_set():
        try:
            progressed = _process_available_files(app_config)
        except Exception:
            # E.g. the queue or the manifest storage being unreachable; the
            # worker thread must survive it and retry after the poll sleep.
            LOG.exception("Worker pass failed")
            progressed = False

        if not progressed:
            app_config.stop_event.wait(timeout=poll_sleep)

    LOG.info("Worker loop stopped")


def startup_check(app_config: AppConfig) -> None:
    """
    Re-assert the queue layout and the manifest store instead of trusting the
    ones load_app_config checked: they may have been emptied or re-created
    since, and on S3 this is also where a credential or permission problem
    surfaces before polling. The failed items are recovered by the worker.
    """
    app_config.queue.ensure_layout()
    app_config.manifest.ensure_store()


def install_signal_handlers(app_config: AppConfig) -> None:
    def _on_term(_signum, _frame) -> None:
        LOG.info("SIGTERM received. Stopping worker…")
        app_config.stop_event.set()
    try:
        signal.signal(signal.SIGTERM, _on_term)
    except Exception:
        LOG.exception("Failed to install SIGTERM handler")
