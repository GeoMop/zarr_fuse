import copy
import json
import logging
import threading

import pytest
import yaml
import zarr_fuse as zf

import bukov_fixtures as bukov

from ingress_server import worker
from ingress_server.io.files import save_data
from ingress_server.local_cache import LocalCache, content_hash
from ingress_server.manifest import SCHEMA_PATH, TIME, Manifest, check_schema
from ingress_server.models import MetadataModel
from ingress_server.queue_storage import QueueStorage
from ingress_server.worker import _process_available_files, startup_recover


@pytest.fixture
def queue(tmp_path, monkeypatch) -> QueueStorage:
    monkeypatch.setenv("ZF_STORE_URL", str(tmp_path / "bukov_store.zarr"))
    storage = QueueStorage(str(tmp_path / "queue"))
    storage.ensure_layout()
    return storage


def _payload_names(queue: QueueStorage, queue_name: str) -> set[str]:
    return {name for name in queue._item_names(queue_name) if not name.endswith(".meta.json")}


def _stage_with_hash(queue: QueueStorage, cache: LocalCache | None = None) -> list[str]:
    names = []
    for name in bukov.stage_items(queue):
        ref = bukov.item_ref(name)
        payload = queue.read_bytes(ref)
        meta = json.loads(queue.read_meta_text(ref))
        meta["sha256"] = content_hash(payload)
        queue.put_item(ref.partition("/")[2], payload, json.dumps(meta).encode("utf-8"))
        if cache:
            cache.put(ref.partition("/")[2], payload)
        names.append(ref.partition("/")[2])
    return names


def test_manifest_yaml_matches_the_code():
    check_schema(zf.schema.deserialize(SCHEMA_PATH))


def test_schema_mismatch_is_reported():
    schema = copy.deepcopy(yaml.safe_load(SCHEMA_PATH.read_text()))
    schema["items"]["VARS"]["state"]["type"] = "str[8]"

    with pytest.raises(ValueError, match="state"):
        check_schema(zf.schema.deserialize(schema))


def test_last_row_is_the_current_state(tmp_path):
    manifest = Manifest(str(tmp_path / "manifest.zarr"))
    manifest.add([{"item_name": "a.json", "state": "accepted"}, {"item_name": "b.json", "state": "accepted"}])
    manifest.add([{"item_name": "a.json", "state": "success"}])

    assert manifest.states() == {"a.json": "success", "b.json": "accepted"}

    times = manifest._node().dataset[TIME].values
    assert len(set(times)) == 3


def test_manifest_is_not_redirected_by_zf_store_url(tmp_path, monkeypatch):
    monkeypatch.setenv("ZF_STORE_URL", str(tmp_path / "data_store.zarr"))
    manifest = Manifest(str(tmp_path / "manifest.zarr"))

    manifest.add([{"item_name": "a.json", "state": "accepted"}])

    assert (tmp_path / "manifest.zarr").is_dir()
    assert not (tmp_path / "data_store.zarr").exists()


def test_manifest_reads_do_not_leak_threads(tmp_path):
    manifest = Manifest(str(tmp_path / "manifest.zarr"))
    manifest.add([{"item_name": "a.json", "state": "accepted"}])
    before = threading.active_count()

    for _ in range(10):
        manifest.states()

    assert threading.active_count() <= before


def test_local_cache_returns_only_matching_copies(tmp_path):
    cache = LocalCache(tmp_path / "cache")
    cache.put("a.json", b"payload")

    assert cache.get("a.json", content_hash(b"payload")) == b"payload"
    assert cache.get("a.json", content_hash(b"other")) is None
    assert cache.get("a.json", None) is None

    cache.evict("a.json")
    assert cache.get("a.json", content_hash(b"payload")) is None


def test_save_data_records_the_hash_and_caches_the_payload(queue, tmp_path):
    cache = LocalCache(tmp_path / "cache")
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

    save_data(bukov.app_config(queue, cache=cache), metadata, b"[]")

    (ref,) = queue.list_accepted()
    sha256 = json.loads(queue.read_meta_text(ref))["sha256"]
    assert sha256 == content_hash(b"[]")
    assert cache.get(ref.partition("/")[2], sha256) == b"[]"


def test_worker_records_the_outcome(queue):
    names = {f"{bukov.ENDPOINT}_{name}" for name in bukov.stage_items(queue)}
    app_config = bukov.app_config(queue)

    assert _process_available_files(app_config)

    assert app_config.manifest.states() == dict.fromkeys(names, "success")
    assert _payload_names(queue, "success") == names
    bukov.assert_store_content()

    ds = app_config.manifest._node().dataset
    last = {str(n): str(t) for n, s, t in zip(ds["item_name"].values, ds["state"].values, ds["data_time"].values)
            if s == "success"}
    assert last[f"{bukov.ENDPOINT}_{bukov.OLDEST}"] == "2025-09-17 11:30:00+00:00"


def test_held_items_stay_accepted(queue):
    bukov.stage_items(queue)
    app_config = bukov.app_config(queue, retention_time=96.0)

    assert not _process_available_files(app_config)

    assert set(app_config.manifest.states().values()) == {"accepted"}
    assert len(_payload_names(queue, "accepted")) == 3


def test_worker_reads_the_local_copy(queue, tmp_path, monkeypatch):
    cache = LocalCache(tmp_path / "cache")
    _stage_with_hash(queue, cache)
    app_config = bukov.app_config(queue, cache=cache)
    monkeypatch.setattr(queue, "read_bytes", lambda ref: pytest.fail(f"{ref} downloaded"))

    assert _process_available_files(app_config)

    assert set(app_config.manifest.states().values()) == {"success"}


def test_payload_not_matching_its_hash_fails(queue):
    name = _stage_with_hash(queue)[0]
    queue.put_item(name, b"corrupted", queue.read_meta_text(f"accepted/{name}").encode("utf-8"))
    app_config = bukov.app_config(queue)

    _process_available_files(app_config)

    assert app_config.manifest.states()[name] == "failed"
    assert name in _payload_names(queue, "failed")


def test_interrupted_move_is_finished_without_reprocessing(queue, caplog):
    name = f"{bukov.ENDPOINT}_{bukov.stage_items(queue)[0]}"
    app_config = bukov.app_config(queue)
    _process_available_files(app_config)
    for suffix in ("", ".meta.json"):
        queue.fs.mv(queue._abs(f"success/{name}{suffix}"), queue._abs(f"accepted/{name}{suffix}"))

    with caplog.at_level(logging.INFO, logger="ingress_server.worker"):
        caplog.clear()
        _process_available_files(app_config)

    assert bukov.stored_refs(caplog) == []
    assert name in _payload_names(queue, "success")


def test_recovery_retries_the_failed_items(queue):
    name = _stage_with_hash(queue)[0]
    queue.put_item(name, b"corrupted", queue.read_meta_text(f"accepted/{name}").encode("utf-8"))
    app_config = bukov.app_config(queue)
    _process_available_files(app_config)

    startup_recover(app_config)

    assert app_config.manifest.states()[name] == "accepted"
    assert name in _payload_names(queue, "accepted")


def test_failed_manifest_write_keeps_the_items_accepted(queue, monkeypatch):
    bukov.stage_items(queue)
    app_config = bukov.app_config(queue)
    original = app_config.manifest.add

    def add(rows):
        if any(row["state"] == "success" for row in rows):
            raise OSError("S3 unreachable")
        original(rows)

    monkeypatch.setattr(app_config.manifest, "add", add)

    with pytest.raises(OSError):
        _process_available_files(app_config)

    assert len(_payload_names(queue, "accepted")) == 3


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
