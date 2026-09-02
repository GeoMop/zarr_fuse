"""Shared fixture data for the worker tests driven by the bukov payloads."""
import json

from pathlib import Path

import numpy as np
import zarr_fuse as zf

from ingress_server.app_config import AppConfig, BaseConfig, SmtpConfig
from ingress_server.queue_storage import FileRef, QueueStorage

TESTS_DIR = Path(__file__).parent
DATA_DIR = TESTS_DIR / "data" / "bukov"
CONFIG_PATH = TESTS_DIR / "inputs" / "endpoints_config.yaml"
SCHEMA = "schemas/bukov_test_schema.yaml"

ENDPOINT = "bukov"

# Receipt order (sorted item names) is exactly opposite to the data time order:
NEWEST = "20250919T111523_177cbe0317bd.json"   # 2025-09-18T19:30 -> 2025-09-19T11:00
MIDDLE = "20250919T111523_d9a465df946d.json"   # 2025-09-18T11:30 -> 2025-09-18T19:00
OLDEST = "20250919T115137_7b2b4dfe5bf6.json"   # 2025-09-17T11:30 -> 2025-09-17T19:00


def item_ref(payload_name: str) -> FileRef:
    """Ref of a staged payload in the accepted queue."""
    return FileRef(f"accepted/{ENDPOINT}_{payload_name}")


def stage_items(storage: QueueStorage) -> list[str]:
    """
    Put the bukov payloads into the accepted queue, with their metadata
    pointing at the test schema. Return the payload names.
    """
    names = []
    for payload in sorted(DATA_DIR.glob("*.json")):
        if payload.name.endswith(".meta.json"):
            continue

        meta = json.loads(
            (DATA_DIR / (payload.name + ".meta.json")).read_text(encoding="utf-8")
        )
        meta["schema_path"] = SCHEMA
        meta["time_like_coord"] = "date_time"
        storage.put_item(
            name=f"{ENDPOINT}_{payload.name}",
            payload=payload.read_bytes(),
            meta=json.dumps(meta).encode("utf-8"),
        )
        names.append(payload.name)

    return names


def app_config(storage: QueueStorage, base: BaseConfig | None = None) -> AppConfig:
    return AppConfig(
        queue=storage,
        config_path=CONFIG_PATH,
        config={},
        base=base or BaseConfig(),
        smtp=SmtpConfig(),
    )


def stored_refs(caplog) -> list[str]:
    """Refs of the items written to the store, in the order of the writes."""
    return [
        record.args[0]
        for record in caplog.records
        if record.msg == "Storing data %s"
    ]


def assert_store_content() -> None:
    """The whole time range of the fixture payloads is stored, sorted."""
    ds = zf.open_store(TESTS_DIR / "inputs" / SCHEMA)[ENDPOINT].dataset

    date_time = ds["date_time"].values.astype("datetime64[s]")
    assert np.all(date_time[:-1] <= date_time[1:])
    assert date_time.min() == np.datetime64("2025-09-17T11:30:00")
    assert date_time.max() == np.datetime64("2025-09-19T11:00:00")
