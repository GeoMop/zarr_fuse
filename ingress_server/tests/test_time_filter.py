import json
import logging
import shutil
from pathlib import Path

import numpy as np
import polars as pl
import zarr_fuse as zf

from ingress_server.app_config import AppConfig, BaseConfig, SmtpConfig
from ingress_server.io.time_filter import ExtractedItem, sort_by_data_time
from ingress_server.models import MetadataModel
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


def test_sort_by_data_time():
    items = [
        _item("a", "n1", "2025-09-19T00:00:00+00:00"),
        _item("b", "n1", None),
        _item("c", "n2", "2025-09-01T00:00:00+00:00"),
        _item("d", "n1", "2025-09-17T00:00:00+00:00"),
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
        base=BaseConfig(),
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
