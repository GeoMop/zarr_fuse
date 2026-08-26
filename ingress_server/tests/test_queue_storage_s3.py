"""
Queue storage tests against a real S3 endpoint (CESNET by default).

They require ZF_S3_ACCESS_KEY / ZF_S3_SECRET_KEY (e.g. via .secrets_env in the
repo root) and skip otherwise. Each test run works under a unique prefix in the
test bucket and cleans up after itself.
"""
import logging
import uuid

import pytest

import bukov_fixtures as bukov

from ingress_server.queue_storage import QueueStorage
from ingress_server.worker import _process_available_files

LOG = logging.getLogger(__name__)


@pytest.fixture
def s3_queue(s3_queue_config):
    url = f"s3://{s3_queue_config['bucket_name']}/ingress-queue-tests/{uuid.uuid4().hex}"
    storage = QueueStorage(url)
    assert storage.url == url
    storage.ensure_layout()

    yield storage

    try:
        storage.fs.rm(storage.root, recursive=True)
    except FileNotFoundError:
        pass


def test_roundtrip_put_list_read_move(s3_queue):
    ref = s3_queue.put_item("bukov_20250919T111523_aaa.json", b"payload", b'{"m": 1}')

    assert ref == "accepted/bukov_20250919T111523_aaa.json"
    # The layout marker and the .meta.json sidecar must not be listed.
    assert s3_queue.list_accepted() == [ref]
    assert s3_queue.read_bytes(ref) == b"payload"
    assert s3_queue.read_meta_text(ref) == '{"m": 1}'

    dest = s3_queue.move(ref, "success")

    assert dest == "success/bukov_20250919T111523_aaa.json"
    assert s3_queue.list_accepted() == []
    assert set(s3_queue._item_names("success")) == {
        "bukov_20250919T111523_aaa.json",
        "bukov_20250919T111523_aaa.json.meta.json",
    }


def test_list_orders_by_item_name(s3_queue):
    late = s3_queue.put_item("ep_b_20250919T2_bbb.json", b"2", b"{}")
    early = s3_queue.put_item("ep_a_20250919T1_aaa.json", b"1", b"{}")

    assert s3_queue.list_accepted() == [early, late]


def test_recover_failed(s3_queue):
    ref_a = s3_queue.put_item("ep_a_a.json", b"a", b"{}")
    ref_b = s3_queue.put_item("ep_b_b.json", b"b", b"{}")
    s3_queue.move(ref_a, "failed")
    s3_queue.move(ref_b, "failed")
    assert s3_queue.list_accepted() == []

    s3_queue.recover_failed()

    assert s3_queue.list_accepted() == [ref_a, ref_b]
    for ref in (ref_a, ref_b):
        assert s3_queue.read_meta_text(ref) == "{}"


def test_fresh_instance_sees_writes(s3_queue):
    ref = s3_queue.put_item("bukov_x.json", b"payload", b"{}")

    fresh = QueueStorage(s3_queue.url)
    assert fresh.list_accepted() == [ref]
    assert fresh.read_bytes(ref) == b"payload"


def test_worker_end_to_end_on_s3_queue(s3_queue, tmp_path, monkeypatch, caplog):
    """
    Full worker pass with the queue in S3: payloads received in the order
    opposite to their data time must be written to the (local) zarr store
    oldest-data-first and end up in the S3 success queue.
    """
    names = bukov.stage_items(s3_queue)
    assert set(names) == {bukov.NEWEST, bukov.MIDDLE, bukov.OLDEST}

    app_config = bukov.app_config(s3_queue)
    monkeypatch.setenv("ZF_STORE_URL", str(tmp_path / "bukov_store.zarr"))

    with caplog.at_level(logging.INFO, logger="ingress_server.worker"):
        progressed = _process_available_files(app_config)

    assert progressed
    assert bukov.stored_refs(caplog) == [
        bukov.item_ref(bukov.OLDEST),
        bukov.item_ref(bukov.MIDDLE),
        bukov.item_ref(bukov.NEWEST),
    ]

    assert s3_queue.list_accepted() == []
    success_names = {
        name for name in s3_queue._item_names("success")
        if not name.endswith(".meta.json")
    }
    assert success_names == {f"{bukov.ENDPOINT}_{name}" for name in names}

    bukov.assert_store_content()
