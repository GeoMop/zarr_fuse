import logging
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import polars as pl
import pytest
import zarr_fuse as zf

import bukov_fixtures as bukov

from ingress_server.app_config import BaseConfig
from ingress_server.io.time_filter import (
    ExtractedItem,
    TimeKeyError,
    _normalize_time_value,
    _time_diff,
    make_extracted_item,
    partition_by_retention,
    sort_by_data_time,
    time_key_type_conflict,
)
from ingress_server.models import MetadataModel
from ingress_server import worker
from ingress_server.queue_storage import FileRef, QueueStorage
from ingress_server.worker import _process_available_files

LOG = logging.getLogger(__name__)


def _item(name: str, target_node: str, time_key) -> ExtractedItem:
    metadata = MetadataModel(
        content_type="application/json",
        endpoint_name="bukov",
        node_path=None,
        username="test",
        schema_path=bukov.SCHEMA,
        extract_fn=None,
        fn_module=None,
        dataframe_row=None,
        target_node=target_node,
    )
    return ExtractedItem(
        ref=FileRef(name),
        metadata=metadata,
        schema_path=Path(bukov.SCHEMA),
        obj=pl.DataFrame(),
        time_key=time_key,
    )


def _utc(iso: str) -> datetime:
    """A time key as `make_extracted_item` produces it: aware UTC datetime."""
    return datetime.fromisoformat(iso).replace(tzinfo=timezone.utc)


def test_sort_by_data_time():
    items = [
        _item("a", "n1", _utc("2025-09-19T00:00:00")),
        _item("b", "n1", None),
        _item("c", "n2", _utc("2025-09-01T00:00:00")),
        _item("d", "n1", _utc("2025-09-17T00:00:00")),
        _item("e", "n1", None),
    ]

    ordered = sort_by_data_time(items)

    assert {item.ref for item in ordered} == {"a", "b", "c", "d", "e"}
    n1_order = [
        item.ref for item in ordered
        if item.metadata.target_node == "n1"
    ]
    # Items without a time key keep receipt order and precede the time-ordered ones.
    assert n1_order == ["b", "e", "d", "a"]


def test_normalize_time_value_reports_uninterpretable_values():
    """A value that cannot become a time key must say why instead of silently
    turning into None, which would demote the item to receipt order unnoticed."""
    assert _normalize_time_value("2025-09-19T00:00:00") == _utc("2025-09-19T00:00:00")
    assert _normalize_time_value(3) == 3.0

    for bad in [None, "not-a-date", np.datetime64("NaT", "s"), object()]:
        with pytest.raises(TimeKeyError):
            _normalize_time_value(bad)


def test_time_diff_rejects_mixed_time_key_types():
    """Diffing a datetime key against a numeric one is a source configuration
    error, and must be reported as such rather than as a bare float() failure."""
    assert _time_diff(_utc("2025-09-19T06:00:00"), _utc("2025-09-19T00:00:00")) == 6.0
    assert _time_diff(7.0, 2.5) == 4.5

    with pytest.raises(TimeKeyError, match="different types"):
        _time_diff(_utc("2025-09-19T00:00:00"), 1.0)


def test_mixed_time_key_types_are_reported_and_filtered_per_type():
    """A batch mixing datetime and numeric keys is reported, and each type is
    still held against its own newest item instead of crashing the sort."""
    items = [
        _item("dt_old", "n1", _utc("2025-09-17T00:00:00")),
        _item("dt_new", "n1", _utc("2025-09-19T00:00:00")),
        _item("num_old", "n2", 0.0),
        _item("num_new", "n2", 100.0),
    ]

    conflict = time_key_type_conflict(items)
    assert conflict is not None
    assert "datetime" in conflict and "numeric" in conflict

    ready, held = partition_by_retention(sort_by_data_time(items), retention_time=24.0)

    assert {item.ref for item in ready} == {"dt_old", "num_old"}
    assert {item.ref for item in held} == {"dt_new", "num_new"}


def test_unreadable_time_coord_is_recorded_on_the_item():
    """A configured time_like_coord missing from the payload keeps the item
    processable (receipt order) but records the reason for notification."""
    metadata = MetadataModel(
        content_type="application/json",
        endpoint_name="bukov",
        node_path=None,
        username="test",
        schema_path=bukov.SCHEMA,
        extract_fn=None,
        fn_module=None,
        time_like_coord="date_time",
        dataframe_row=None,
        target_node="n1",
    )

    item = make_extracted_item(
        FileRef("no_time.json"),
        metadata,
        Path(bukov.SCHEMA),
        pl.DataFrame({"temp": [1.0]}),
    )

    assert item.time_key is None
    assert item.time_error is not None
    assert "date_time" in item.time_error


def test_anomaly_is_emailed_only_once(tmp_path, monkeypatch):
    """The worker re-examines held items on every poll, so a persisting anomaly
    must notify once instead of mailing the same report every cycle."""
    app_config = bukov.app_config(QueueStorage(str(tmp_path / "queue")))
    sent: list[list[dict]] = []
    monkeypatch.setattr(
        worker,
        "send_anomaly_email",
        lambda *, smtp_config, anomalies: sent.append(anomalies),
    )

    anomaly = {"type": "time_key", "error": "boom", "context": {"file": "a.json"}}
    other = {"type": "time_key", "error": "boom", "context": {"file": "b.json"}}

    worker._notify_anomalies(app_config, [anomaly])
    worker._notify_anomalies(app_config, [anomaly])
    worker._notify_anomalies(app_config, [anomaly, other])

    assert sent == [[anomaly], [other]]


def test_worker_stores_batch_in_data_time_order(tmp_path, monkeypatch, caplog):
    """
    Payloads whose receipt order is opposite to their data time order must be
    written to the store oldest-data-first, so no backdated payload falls
    below the store minimum and gets dropped.
    """
    queue_dir = tmp_path / "queue"
    storage = QueueStorage(str(queue_dir))
    storage.ensure_layout()

    names = bukov.stage_items(storage)
    assert set(names) == {bukov.NEWEST, bukov.MIDDLE, bukov.OLDEST}

    # retention_time=0 disables holding: this test verifies data-time
    # ordering, not the (separately tested) retention mechanism.
    app_config = bukov.app_config(storage, base=BaseConfig(retention_time=0.0))
    monkeypatch.setenv("ZF_STORE_URL", str(tmp_path / "bukov_store.zarr"))

    with caplog.at_level(logging.INFO, logger="ingress_server.worker"):
        progressed = _process_available_files(app_config)

    assert progressed
    assert bukov.stored_refs(caplog) == [
        bukov.item_ref(bukov.OLDEST),
        bukov.item_ref(bukov.MIDDLE),
        bukov.item_ref(bukov.NEWEST),
    ]

    success_names = {
        path.name for path in (queue_dir / "success").rglob("*.json")
        if not path.name.endswith(".meta.json")
    }
    assert success_names == {f"{bukov.ENDPOINT}_{name}" for name in names}
    assert not list((queue_dir / "failed").rglob("*.json"))

    bukov.assert_store_content()

    # The backdated payload's data must be present with real values; without
    # data-time ordering it would fall below the store minimum and be dropped.
    ds = zf.open_store(bukov.TESTS_DIR / "inputs" / bukov.SCHEMA)[bukov.ENDPOINT].dataset
    day_17 = ds["rock_temp"].sel(date_time=slice("2025-09-17", "2025-09-18"))
    assert np.isfinite(day_17.values).any()
