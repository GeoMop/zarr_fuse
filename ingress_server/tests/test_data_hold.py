import json
import logging

from pathlib import Path

import zarr_fuse as zf

from ingress_server.app_config import AppConfig, BaseConfig, SmtpConfig
from ingress_server.models import MetadataModel
from ingress_server.queue_storage import QueueStorage
from ingress_server.worker import _process_available_files

LOG = logging.getLogger(__name__)

ENDPOINT = "hold_test"
RETENTION_HOURS = 4.0

OLD_NAME = "old.json"
FRESH_NAME = "fresh.json"
NEWER_NAME = "newer.json"

OLD_DATE_TIME = "2025-07-17T00:00:00+00:00"
FRESH_DATE_TIME = "2025-07-17T05:00:00+00:00"    # 5h after OLD
NEWER_DATE_TIME = "2025-07-17T10:00:00+00:00"    # 5h after FRESH


def _write_schema(tmp_path: Path, monkeypatch) -> Path:
    schema_path = tmp_path / "hold_schema.yaml"
    store_path = tmp_path / "hold_store.zarr"
    schema_path.write_text(
        f"""
{ENDPOINT}:
    VARS:
        temperature:
            unit: "degC"
            df_col: "temp"
            coords: ["date_time"]
    COORDS:
        date_time:
            unit: {{ tick: "s", tz: "UTC" }}
            source_unit: {{ tick: "s", tz: "UTC" }}
            df_col: "date_time"
            chunk_size: 64
    ATTRS:
        STORE_URL: "{store_path}"
""",
        encoding="utf-8",
    )
    # ZF_STORE_URL (e.g. from a local .env) takes precedence over the
    # schema's ATTRS.STORE_URL in zarr_fuse, so it must be cleared/pinned
    # here or the test would silently hit whatever store that env points to.
    monkeypatch.setenv("ZF_STORE_URL", str(store_path))
    return schema_path


def _stage_item(
    storage: QueueStorage, schema_path: Path, name: str, date_time: str, temp: float
) -> None:
    payload = json.dumps([{"date_time": date_time, "temp": temp}]).encode("utf-8")

    metadata = MetadataModel(
        content_type="application/json",
        endpoint_name=ENDPOINT,
        node_path=None,
        username="test",
        schema_path=str(schema_path),
        extract_fn=None,
        fn_module=None,
        time_like_coord="date_time",
        dataframe_row=None,
        target_node=ENDPOINT,
    )
    storage.put_item(
        name=name,
        payload=payload,
        meta=metadata.model_dump_json().encode("utf-8"),
    )


def _app_config(tmp_path: Path) -> AppConfig:
    storage = QueueStorage(str(tmp_path / "queue"))
    storage.ensure_layout()
    return AppConfig(
        queue=storage,
        config_path=tmp_path / "unused_config.yaml",
        config={},
        base=BaseConfig(retention_time=RETENTION_HOURS),
        smtp=SmtpConfig(),
    )


def _payload_names(app_config: AppConfig, queue_name: str) -> set[str]:
    return {
        name for name in app_config.queue._item_names(queue_name)
        if not name.endswith(".meta.json")
    }


def _accepted_names(app_config: AppConfig) -> set[str]:
    return _payload_names(app_config, "accepted")


def _success_names(app_config: AppConfig) -> set[str]:
    return _payload_names(app_config, "success")


def test_batch_relative_retention_splits_ready_and_held(tmp_path, monkeypatch):
    """
    "Now" is the newest time_key in the current batch (2025-07-17T05:00, the
    fresh item), not the wall clock. The old item is 5h older than that,
    which exceeds the 4h retention window, so it is stored; the fresh item
    IS the batch maximum (0h diff) so it is always held.
    """
    schema_path = _write_schema(tmp_path, monkeypatch)
    app_config = _app_config(tmp_path)
    _stage_item(app_config.queue, schema_path, OLD_NAME, OLD_DATE_TIME, temp=1.0)
    _stage_item(app_config.queue, schema_path, FRESH_NAME, FRESH_DATE_TIME, temp=2.0)

    progressed = _process_available_files(app_config)

    assert progressed
    assert _accepted_names(app_config) == {FRESH_NAME}
    assert _success_names(app_config) == {OLD_NAME}


def test_held_item_alone_does_not_report_progress(tmp_path, monkeypatch):
    """
    A single item is always its own batch maximum (0h diff from itself), so
    it is always held regardless of retention_time. This pass must report no
    progress, otherwise the worker loop would busy-spin instead of sleeping
    between checks.
    """
    schema_path = _write_schema(tmp_path, monkeypatch)
    app_config = _app_config(tmp_path)
    _stage_item(app_config.queue, schema_path, FRESH_NAME, FRESH_DATE_TIME, temp=2.0)

    progressed = _process_available_files(app_config)

    assert not progressed
    assert _accepted_names(app_config) == {FRESH_NAME}


def test_held_item_is_released_once_newer_data_arrives(tmp_path, monkeypatch):
    """
    A held item is not released by merely re-polling with no new data (there
    is nothing newer to make it "age"); it becomes ready once a newer item
    raises the batch maximum far enough past it.
    """
    schema_path = _write_schema(tmp_path, monkeypatch)
    app_config = _app_config(tmp_path)
    _stage_item(app_config.queue, schema_path, OLD_NAME, OLD_DATE_TIME, temp=1.0)
    _stage_item(app_config.queue, schema_path, FRESH_NAME, FRESH_DATE_TIME, temp=2.0)

    _process_available_files(app_config)
    assert _accepted_names(app_config) == {FRESH_NAME}

    # Re-poll with no new data: nothing changes, no progress.
    progressed = _process_available_files(app_config)
    assert not progressed
    assert _accepted_names(app_config) == {FRESH_NAME}

    # A newer item arrives, pushing the batch maximum forward.
    _stage_item(app_config.queue, schema_path, NEWER_NAME, NEWER_DATE_TIME, temp=3.0)
    progressed = _process_available_files(app_config)

    assert progressed
    assert _accepted_names(app_config) == {NEWER_NAME}
    assert _success_names(app_config) == {OLD_NAME, FRESH_NAME}

    ds = zf.open_store(schema_path)[ENDPOINT].dataset
    date_time = ds["date_time"].values.astype("datetime64[s]")
    assert date_time.min() == np_datetime64(OLD_DATE_TIME)
    assert date_time.max() == np_datetime64(FRESH_DATE_TIME)


def np_datetime64(iso: str):
    import numpy as np
    return np.datetime64(iso.replace("+00:00", ""))
