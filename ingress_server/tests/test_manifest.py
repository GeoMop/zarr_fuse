import copy
import json
import logging
import threading

import numpy as np
import pytest
import yaml
import zarr_fuse as zf

import bukov_fixtures as bukov

from ingress_server import worker
from ingress_server.io.files import save_data
from ingress_server.local_cache import LocalCache, content_hash
from ingress_server.manifest import (
    SCHEMA_PATH,
    STR_WIDTHS,
    ManifestEntry,
    ManifestStore,
    check_schema,
    receipt_key,
)
from ingress_server.models import MetadataModel
from ingress_server.queue_storage import QueueStorage
from ingress_server.worker import _process_available_files, _recover_failed

RECEIVED_AT = "2025-09-19T11:15:24Z"


@pytest.fixture
def queue(tmp_path, monkeypatch) -> QueueStorage:
    monkeypatch.setenv("ZF_STORE_URL", str(tmp_path / "bukov_store.zarr"))
    storage = QueueStorage(str(tmp_path / "queue"))
    storage.ensure_layout()
    return storage


def _entry(name: str, received_at: str = RECEIVED_AT, **kwargs) -> ManifestEntry:
    return ManifestEntry(key=receipt_key(received_at), item_name=name, source="ep", state="accepted", **kwargs)


def _all_entries(manifest: ManifestStore) -> dict[str, ManifestEntry]:
    return manifest.find(set(manifest._dataset()["item_name"].values.tolist()))


def _payload_names(queue: QueueStorage, queue_name: str) -> set[str]:
    return {name for name in queue._item_names(queue_name) if not name.endswith(".meta.json")}


def _sidecar_names(queue: QueueStorage, queue_name: str) -> set[str]:
    return {name for name in queue._item_names(queue_name) if name.endswith(".meta.json")}


def _stage_with_hash(queue: QueueStorage, cache: LocalCache | None = None) -> list[str]:
    names = bukov.stage_items(queue)
    for name in names:
        ref = bukov.item_ref(name)
        payload = queue.read_bytes(ref)
        meta = json.loads(queue.read_meta_text(ref))
        meta["sha256"] = content_hash(payload)
        queue.put_item(ref.partition("/")[2], payload, json.dumps(meta).encode("utf-8"))
        if cache is not None:
            cache.put(ref.partition("/")[2], payload)
    return names


# --- manifest store ---

def test_manifest_yaml_matches_the_code():
    check_schema(zf.schema.deserialize(SCHEMA_PATH))


def test_schema_check_reports_every_mismatch():
    raw = yaml.safe_load(SCHEMA_PATH.read_text())
    broken = copy.deepcopy(raw)
    broken["items"]["VARS"]["state"]["type"] = "str[8]"
    del broken["items"]["VARS"]["error"]
    broken["items"]["COORDS"]["received_at"]["sorted"] = True

    with pytest.raises(ValueError) as error:
        check_schema(zf.schema.deserialize(broken))

    assert "unsorted" in str(error.value)
    assert "'error'" in str(error.value)
    assert "'state'" in str(error.value)


def test_register_makes_keys_unique_and_skips_registered_items(tmp_path):
    manifest = ManifestStore(str(tmp_path / "manifest.zarr"))

    added, known = manifest.register([_entry("a.json"), _entry("b.json")])

    assert known == {}
    base = receipt_key(RECEIVED_AT)
    assert [entry.key for entry in added] == [base, base + np.timedelta64(1, "us")]

    manifest.record([added[0].with_state("success")])
    added, known = manifest.register([_entry("a.json")])

    assert added == []
    assert known["a.json"].state == "success"
    assert [entry.item_name for entry in manifest.pending()] == ["b.json"]


def test_record_keeps_rows_between_the_updated_ones(tmp_path):
    manifest = ManifestStore(str(tmp_path / "manifest.zarr"))
    entries, _ = manifest.register([
        _entry(f"{i}.json", received_at=f"2025-09-19T11:15:2{i}Z", metadata=f'{{"i": {i}}}')
        for i in range(3)
    ])

    manifest.record([entries[2].with_state("success"), entries[0].with_state("failed", "boom")])

    stored = _all_entries(manifest)
    assert stored["0.json"].state == "failed"
    assert stored["0.json"].error == "boom"
    assert stored["1.json"] == entries[1]
    assert stored["2.json"].state == "success"


def test_record_rejects_values_too_long_to_store(tmp_path):
    manifest = ManifestStore(str(tmp_path / "manifest.zarr"))

    with pytest.raises(ValueError, match="metadata"):
        manifest.record([_entry("a.json", metadata="x" * (STR_WIDTHS["metadata"] + 1))])

    assert manifest.pending() == []


def test_manifest_is_not_redirected_by_zf_store_url(tmp_path, monkeypatch):
    monkeypatch.setenv("ZF_STORE_URL", str(tmp_path / "data_store.zarr"))
    manifest = ManifestStore(str(tmp_path / "manifest.zarr"))

    manifest.register([_entry("a.json")])

    assert (tmp_path / "manifest.zarr").is_dir()
    assert not (tmp_path / "data_store.zarr").exists()


def test_manifest_operations_do_not_leak_threads(tmp_path):
    manifest = ManifestStore(str(tmp_path / "manifest.zarr"))
    manifest.register([_entry("a.json")])
    before = threading.active_count()

    for _ in range(10):
        manifest.pending()

    assert threading.active_count() <= before


# --- local cache and receipt ---

def test_local_cache_returns_only_hash_verified_copies(tmp_path):
    cache = LocalCache(tmp_path / "cache")
    cache.put("ep_a.json", b"payload")

    assert cache.get("ep_a.json", content_hash(b"payload")) == b"payload"
    assert cache.get("ep_a.json", content_hash(b"other")) is None
    assert cache.get("ep_a.json", "") is None
    assert cache.get("missing.json", content_hash(b"payload")) is None

    cache.evict("ep_a.json")
    assert cache.get("ep_a.json", content_hash(b"payload")) is None

    disabled = LocalCache(None)
    disabled.put("ep_a.json", b"payload")
    assert disabled.get("ep_a.json", content_hash(b"payload")) is None


def test_unusable_cache_dir_never_raises(tmp_path):
    blocker = tmp_path / "file"
    blocker.write_bytes(b"")
    cache = LocalCache(blocker / "cache")

    cache.put("ep_a.json", b"payload")
    assert cache.get("ep_a.json", content_hash(b"payload")) is None
    cache.evict("ep_a.json")


def test_save_data_records_the_hash_and_caches_the_payload(queue, tmp_path):
    cache = LocalCache(tmp_path / "cache")
    app_config = bukov.app_config(queue, cache=cache)
    metadata = MetadataModel(
        content_type="application/json",
        endpoint_name="ep",
        node_path=None,
        username="test",
        schema_path="schemas/unused.yaml",
        extract_fn=None,
        fn_module=None,
        dataframe_row=None,
    )

    save_data(app_config, metadata, b'[{"a": 1}]')

    (ref,) = queue.list_accepted()
    meta = json.loads(queue.read_meta_text(ref))
    assert meta["sha256"] == content_hash(b'[{"a": 1}]')
    assert cache.get(ref.partition("/")[2], meta["sha256"]) == b'[{"a": 1}]'


# --- worker driven by the manifest ---

def test_worker_records_the_items_and_their_data_time(queue, caplog):
    names = bukov.stage_items(queue)
    app_config = bukov.app_config(queue)

    with caplog.at_level(logging.INFO, logger="ingress_server.worker"):
        assert _process_available_files(app_config)

    assert bukov.stored_refs(caplog) == [
        bukov.item_ref(bukov.OLDEST),
        bukov.item_ref(bukov.MIDDLE),
        bukov.item_ref(bukov.NEWEST),
    ]
    bukov.assert_store_content()

    item_names = {f"{bukov.ENDPOINT}_{name}" for name in names}
    stored = _all_entries(app_config.manifest)
    assert set(stored) == item_names
    assert {entry.state for entry in stored.values()} == {"success"}

    oldest = stored[f"{bukov.ENDPOINT}_{bukov.OLDEST}"]
    assert oldest.data_time_min == "2025-09-17T11:30:00+00:00"
    assert oldest.data_time_max == "2025-09-17T19:00:00+00:00"
    assert MetadataModel.model_validate_json(oldest.metadata).time_like_coord == "date_time"

    assert _payload_names(queue, "accepted") == set()
    assert _payload_names(queue, "success") == item_names
    assert _sidecar_names(queue, "success") == {name + ".meta.json" for name in item_names}


def test_held_items_stay_pending_with_their_data_time(queue):
    bukov.stage_items(queue)
    app_config = bukov.app_config(queue, retention_time=96.0)

    assert not _process_available_files(app_config)

    pending = app_config.manifest.pending()
    assert len(pending) == 3
    assert all(entry.data_time_min for entry in pending)
    assert len(_payload_names(queue, "accepted")) == 3


def test_worker_reads_the_local_copy_instead_of_the_queue(queue, tmp_path, monkeypatch):
    cache = LocalCache(tmp_path / "cache")
    _stage_with_hash(queue, cache)
    app_config = bukov.app_config(queue, cache=cache)

    def no_download(ref):
        raise AssertionError(f"{ref} downloaded despite a valid local copy")

    monkeypatch.setattr(queue, "read_bytes", no_download)

    assert _process_available_files(app_config)

    assert {entry.state for entry in _all_entries(app_config.manifest).values()} == {"success"}
    assert not any(path.is_file() for path in (tmp_path / "cache").rglob("*"))


def test_payload_not_matching_its_hash_fails(queue):
    names = _stage_with_hash(queue)
    name = bukov.item_ref(names[0]).partition("/")[2]
    meta = queue.read_meta_text(f"accepted/{name}")
    queue.put_item(name, b"corrupted", meta.encode("utf-8"))
    app_config = bukov.app_config(queue)

    _process_available_files(app_config)

    entry = _all_entries(app_config.manifest)[name]
    assert entry.state == "failed"
    assert "Content hash" in entry.error
    assert name in _payload_names(queue, "failed")


def test_interrupted_move_is_finished_without_reprocessing(queue, caplog):
    names = bukov.stage_items(queue)
    app_config = bukov.app_config(queue)
    _process_available_files(app_config)

    name = f"{bukov.ENDPOINT}_{names[0]}"
    for suffix in ("", ".meta.json"):
        queue.fs.mv(queue._abs(f"success/{name}{suffix}"), queue._abs(f"accepted/{name}{suffix}"))
    caplog.clear()

    with caplog.at_level(logging.INFO, logger="ingress_server.worker"):
        assert _process_available_files(app_config)

    assert bukov.stored_refs(caplog) == []
    assert _payload_names(queue, "accepted") == set()
    assert name in _payload_names(queue, "success")


def test_payload_without_metadata_is_failed(queue):
    queue._write("accepted/orphan.json", b"[]")
    app_config = bukov.app_config(queue)

    assert _process_available_files(app_config)

    assert _payload_names(queue, "failed") == {"orphan.json"}
    assert app_config.manifest.find({"orphan.json"}) == {}


def test_long_metadata_is_read_from_the_sidecar(queue, monkeypatch):
    names = bukov.stage_items(queue)
    ref = bukov.item_ref(names[0])
    meta = json.loads(queue.read_meta_text(ref))
    meta["dataframe_row"] = {f"column_{i}": "x" * 40 for i in range(100)}
    queue.put_item(ref.partition("/")[2], queue.read_bytes(ref), json.dumps(meta).encode("utf-8"))
    app_config = bukov.app_config(queue)

    seen_rows = []
    original = worker._extract_payload

    def spy(config, item_ref, metadata, payload):
        seen_rows.append(metadata.dataframe_row)
        return original(config, item_ref, metadata, payload)

    monkeypatch.setattr(worker, "_extract_payload", spy)

    assert _process_available_files(app_config)

    entry = _all_entries(app_config.manifest)[ref.partition("/")[2]]
    assert entry.state == "success"
    assert entry.metadata == ""
    assert meta["dataframe_row"] in seen_rows


def test_recovery_requeues_failed_entries(queue):
    names = _stage_with_hash(queue)
    name = bukov.item_ref(names[0]).partition("/")[2]
    meta = queue.read_meta_text(f"accepted/{name}")
    queue.put_item(name, b"corrupted", meta.encode("utf-8"))
    app_config = bukov.app_config(queue)
    _process_available_files(app_config)
    assert _all_entries(app_config.manifest)[name].state == "failed"

    _recover_failed(app_config)

    assert [entry.item_name for entry in app_config.manifest.pending()] == [name]
    assert _all_entries(app_config.manifest)[name].error == ""
    assert name in _payload_names(queue, "accepted")


def test_failed_record_keeps_the_items_pending(queue, monkeypatch):
    bukov.stage_items(queue)
    app_config = bukov.app_config(queue)
    manifest = app_config.manifest
    original = manifest.record
    calls = []

    def flaky_record(entries):
        calls.append(entries)
        if any(entry.state == "success" for entry in entries) and len(calls) <= 2:
            raise OSError("S3 unreachable")
        return original(entries)

    monkeypatch.setattr(manifest, "record", flaky_record)

    assert not _process_available_files(app_config)
    assert len(manifest.pending()) == 3
    assert len(_payload_names(queue, "accepted")) == 3

    assert _process_available_files(app_config)
    assert manifest.pending() == []


def test_working_loop_survives_a_failing_pass(queue, monkeypatch):
    app_config = bukov.app_config(queue)
    calls = []

    def flaky_pass(config):
        calls.append(config)
        if len(calls) == 1:
            raise OSError("storage unreachable")
        config.stop_event.set()
        return True

    monkeypatch.setattr(worker, "_process_available_files", flaky_pass)

    worker.working_loop(app_config, poll_sleep=0.01)

    assert len(calls) == 2
