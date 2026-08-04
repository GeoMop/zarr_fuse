"""
Queue storage tests against a real S3 endpoint (CESNET by default).

They require ZF_S3_ACCESS_KEY / ZF_S3_SECRET_KEY (e.g. via .secrets_env in the
repo root) and skip otherwise. Each test run works under a unique prefix in the
test bucket and cleans up after itself.
"""
import json
import logging
import uuid

from pathlib import Path

import numpy as np
import pytest
import zarr_fuse as zf

from ingress_server.app_config import AppConfig, BaseConfig, SmtpConfig
from ingress_server.queue_storage import QueueStorage, S3QueueStorage
from ingress_server.worker import _process_available_files

LOG = logging.getLogger(__name__)

TESTS_DIR = Path(__file__).parent
BUKOV_DATA_DIR = TESTS_DIR / "data" / "bukov"
BUKOV_SCHEMA = "schemas/bukov_test_schema.yaml"

# Receipt order (sorted file names) is opposite to the data time order.
NEWEST = "20250919T111523_177cbe0317bd.json"
MIDDLE = "20250919T111523_d9a465df946d.json"
OLDEST = "20250919T115137_7b2b4dfe5bf6.json"


@pytest.fixture
def s3_queue(s3_queue_config):
    url = f"s3://{s3_queue_config['bucket_name']}/ingress-queue-tests/{uuid.uuid4().hex}"
    storage = QueueStorage.from_url(url)
    assert isinstance(storage, S3QueueStorage)
    storage.ensure_layout()

    yield storage

    try:
        storage._fs.rm(url[len("s3://"):], recursive=True)
    except FileNotFoundError:
        pass


def test_roundtrip_put_list_read_move(s3_queue):
    key = s3_queue.put_item("bukov", "20250919T111523_aaa.json", b"payload", b'{"m": 1}')

    assert key == "accepted/bukov/20250919T111523_aaa.json"
    # The layout marker and the .meta.json sidecar must not be listed.
    assert s3_queue.list_accepted() == [key]
    assert s3_queue.read_bytes(key) == b"payload"
    assert s3_queue.read_meta_text(key) == '{"m": 1}'

    s3_queue.move(key, "success")

    assert s3_queue.list_accepted() == []
    success_keys = {
        s3_queue._rel(path) for path in s3_queue._fs.find(s3_queue._abs("success"))
    }
    assert success_keys == {
        "success/bukov/20250919T111523_aaa.json",
        "success/bukov/20250919T111523_aaa.json.meta.json",
    }


def test_list_orders_by_basename_across_endpoints(s3_queue):
    late = s3_queue.put_item("ep_b", "20250919T2_bbb.json", b"2", b"{}")
    early = s3_queue.put_item("ep_a", "20250919T1_aaa.json", b"1", b"{}")

    assert s3_queue.list_accepted() == [early, late]


def test_recover_failed(s3_queue):
    key_a = s3_queue.put_item("ep_a", "a.json", b"a", b"{}")
    key_b = s3_queue.put_item("ep_b", "b.json", b"b", b"{}")
    s3_queue.move(key_a, "failed")
    s3_queue.move(key_b, "failed")
    assert s3_queue.list_accepted() == []

    s3_queue.recover_failed()

    assert s3_queue.list_accepted() == [key_a, key_b]
    for key in (key_a, key_b):
        assert s3_queue.read_meta_text(key) == "{}"


def test_fresh_instance_sees_writes(s3_queue):
    key = s3_queue.put_item("bukov", "x.json", b"payload", b"{}")

    fresh = QueueStorage.from_url(s3_queue.url)
    assert fresh.list_accepted() == [key]
    assert fresh.read_bytes(key) == b"payload"


def _stage_bukov_items(storage: QueueStorage) -> list[str]:
    """Upload the bukov fixture payloads into the S3 accepted queue,
    pointing their metadata at the test schema."""
    names = []
    for payload in sorted(BUKOV_DATA_DIR.glob("*.json")):
        if payload.name.endswith(".meta.json"):
            continue

        meta = json.loads(
            (BUKOV_DATA_DIR / (payload.name + ".meta.json")).read_text(encoding="utf-8")
        )
        meta["schema_path"] = BUKOV_SCHEMA
        storage.put_item(
            endpoint_name="bukov",
            name=payload.name,
            payload=payload.read_bytes(),
            meta=json.dumps(meta).encode("utf-8"),
        )
        names.append(payload.name)

    return names


def test_worker_end_to_end_on_s3_queue(s3_queue, tmp_path, monkeypatch, caplog):
    """
    Full worker pass with the queue in S3: payloads received in the order
    opposite to their data time must be written to the (local) zarr store
    oldest-data-first and end up in the S3 success queue.
    """
    names = _stage_bukov_items(s3_queue)
    assert set(names) == {NEWEST, MIDDLE, OLDEST}

    app_config = AppConfig(
        queue=s3_queue,
        config_path=TESTS_DIR / "inputs" / "endpoints_config.yaml",
        config={},
        base=BaseConfig(),
        smtp=SmtpConfig(),
    )
    monkeypatch.setenv("ZF_STORE_URL", str(tmp_path / "bukov_store.zarr"))

    with caplog.at_level(logging.INFO, logger="ingress_server.worker"):
        progressed = _process_available_files(app_config)

    assert progressed

    stored_order = [
        QueueStorage.basename(record.args[0])
        for record in caplog.records
        if record.msg == "Storing data %s"
    ]
    assert stored_order == [OLDEST, MIDDLE, NEWEST]

    assert s3_queue.list_accepted() == []
    success_payloads = {
        QueueStorage.basename(path)
        for path in s3_queue._fs.find(s3_queue._abs("success"))
        if not path.endswith(".meta.json")
    }
    assert success_payloads == set(names)

    ds = zf.open_store(TESTS_DIR / "inputs" / BUKOV_SCHEMA)["bukov"].dataset
    date_time = ds["date_time"].values.astype("datetime64[s]")
    assert np.all(date_time[:-1] <= date_time[1:])
    assert date_time.min() == np.datetime64("2025-09-17T11:30:00")
    assert date_time.max() == np.datetime64("2025-09-19T11:00:00")
