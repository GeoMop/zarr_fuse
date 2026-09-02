import json
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import polars as pl
import pytest
import zarr_fuse as zf

from ingress_server.app_config import AppConfig, BaseConfig, SmtpConfig
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
from ingress_server.worker import _process_available_files

LOG = logging.getLogger(__name__)

TESTS_DIR = Path(__file__).parent
BUKOV_DATA_DIR = TESTS_DIR / "data" / "bukov"
BUKOV_SCHEMA = "schemas/bukov_test_schema.yaml"

# Receipt order (sorted file names) is exactly opposite to the data time order:
NEWEST = "20250919T111523_177cbe0317bd.json"   # 2025-09-18T19:30 -> 2025-09-19T11:00
MIDDLE = "20250919T111523_d9a465df946d.json"   # 2025-09-18T11:30 -> 2025-09-18T19:00
OLDEST = "20250919T115137_7b2b4dfe5bf6.json"   # 2025-09-17T11:30 -> 2025-09-17T19:00


def _item(name: str, target_node: str, time_key) -> ExtractedItem:
    metadata = MetadataModel(
        content_type="application/json",
        endpoint_name="bukov",
        node_path=None,
        username="test",
        schema_path=BUKOV_SCHEMA,
        extract_fn=None,
        fn_module=None,
        dataframe_row=None,
        target_node=target_node,
    )
    return ExtractedItem(
        data_path=Path(name),
        metadata=metadata,
        schema_path=Path(BUKOV_SCHEMA),
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

    assert {item.data_path.name for item in ordered} == {"a", "b", "c", "d", "e"}
    n1_order = [
        item.data_path.name for item in ordered
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

    assert {item.data_path.name for item in ready} == {"dt_old", "num_old"}
    assert {item.data_path.name for item in held} == {"dt_new", "num_new"}


def test_unreadable_time_coord_is_recorded_on_the_item():
    """A configured time_like_coord missing from the payload keeps the item
    processable (receipt order) but records the reason for notification."""
    metadata = MetadataModel(
        content_type="application/json",
        endpoint_name="bukov",
        node_path=None,
        username="test",
        schema_path=BUKOV_SCHEMA,
        extract_fn=None,
        fn_module=None,
        time_like_coord="date_time",
        dataframe_row=None,
        target_node="n1",
    )

    item = make_extracted_item(
        Path("no_time.json"),
        metadata,
        Path(BUKOV_SCHEMA),
        pl.DataFrame({"temp": [1.0]}),
    )

    assert item.time_key is None
    assert item.time_error is not None
    assert "date_time" in item.time_error


def test_anomaly_is_emailed_only_once(tmp_path, monkeypatch):
    """The worker re-examines held items on every poll, so a persisting anomaly
    must notify once instead of mailing the same report every cycle."""
    app_config = AppConfig(
        queue_dir=tmp_path / "queue",
        config_path=tmp_path / "unused_config.yaml",
        config={},
        base=BaseConfig(),
        smtp=SmtpConfig(),
    )
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


def _stage_bukov_queue(queue_dir: Path) -> list[str]:
    """Copy bukov payloads into the accepted queue, pointing at the test schema."""
    accepted = queue_dir / "accepted" / "bukov"
    accepted.mkdir(parents=True)

    names = []
    for payload in sorted(BUKOV_DATA_DIR.glob("*.json")):
        if payload.name.endswith(".meta.json"):
            continue

        shutil.copy2(payload, accepted / payload.name)
        meta_name = payload.name + ".meta.json"
        meta = json.loads((BUKOV_DATA_DIR / meta_name).read_text(encoding="utf-8"))
        meta["schema_path"] = BUKOV_SCHEMA
        meta["time_like_coord"] = "date_time"
        (accepted / meta_name).write_text(json.dumps(meta), encoding="utf-8")
        names.append(payload.name)

    return names


def test_worker_stores_batch_in_data_time_order(tmp_path, monkeypatch, caplog):
    """
    Payloads whose receipt order is opposite to their data time order must be
    written to the store oldest-data-first, so no backdated payload falls
    below the store minimum and gets dropped.
    """
    queue_dir = tmp_path / "queue"
    names = _stage_bukov_queue(queue_dir)
    assert set(names) == {NEWEST, MIDDLE, OLDEST}

    app_config = AppConfig(
        queue_dir=queue_dir,
        config_path=TESTS_DIR / "inputs" / "endpoints_config.yaml",
        config={},
        # retention_time=0 disables holding: this test verifies data-time
        # ordering, not the (separately tested) retention mechanism.
        base=BaseConfig(retention_time=0.0),
        smtp=SmtpConfig(),
    )
    monkeypatch.setenv("ZF_STORE_URL", str(tmp_path / "bukov_store.zarr"))

    with caplog.at_level(logging.INFO, logger="ingress_server.worker"):
        progressed = _process_available_files(app_config)

    assert progressed

    stored_order = [
        Path(record.args[0]).name
        for record in caplog.records
        if record.msg == "Storing data %s"
    ]
    assert stored_order == [OLDEST, MIDDLE, NEWEST]

    success_files = {
        p.name for p in (queue_dir / "success").rglob("*.json")
        if not p.name.endswith(".meta.json")
    }
    assert success_files == set(names)
    assert not list((queue_dir / "failed").rglob("*"))

    ds = zf.open_store(TESTS_DIR / "inputs" / BUKOV_SCHEMA)["bukov"].dataset
    date_time = ds["date_time"].values.astype("datetime64[s]")
    assert np.all(date_time[:-1] <= date_time[1:])
    assert date_time.min() == np.datetime64("2025-09-17T11:30:00")
    assert date_time.max() == np.datetime64("2025-09-19T11:00:00")

    # The backdated payload's data must be present with real values; without
    # data-time ordering it would fall below the store minimum and be dropped.
    day_17 = ds["rock_temp"].sel(date_time=slice("2025-09-17", "2025-09-18"))
    assert np.isfinite(day_17.values).any()
