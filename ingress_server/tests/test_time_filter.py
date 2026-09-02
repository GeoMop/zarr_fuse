import logging
from pathlib import Path

import numpy as np
import polars as pl
import zarr_fuse as zf

import bukov_fixtures as bukov

from ingress_server.io.time_filter import ExtractedItem, sort_by_data_time
from ingress_server.models import MetadataModel
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


def test_sort_by_data_time():
    items = [
        _item("a", "n1", "2025-09-19T00:00:00+00:00"),
        _item("b", "n1", None),
        _item("c", "n2", "2025-09-01T00:00:00+00:00"),
        _item("d", "n1", "2025-09-17T00:00:00+00:00"),
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

    app_config = bukov.app_config(storage)
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
