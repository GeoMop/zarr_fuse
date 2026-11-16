"""Manifest of the ingress queue (#113): an append-only log of the item states, written by the worker."""
import logging

from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import polars as pl
import zarr_fuse as zf

from zarr_fuse import zarr_storage

LOG = logging.getLogger(__name__)

SCHEMA_PATH = Path(__file__).with_name("manifest.yaml")
NODE = "items"
TIME = "time"
COLUMNS = {
    "item_name": 128,
    "source": 64,
    "state": 16,
    "sha256": 64,
    "data_time": 32,
    "error": 256,
}


def default_manifest_url(queue_url: str) -> str:
    return f"{queue_url.rstrip('/')}/manifest.zarr"


def check_schema(schema: zf.schema.NodeSchema) -> None:
    ds = schema.groups[NODE].ds
    coords = list(ds.COORDS)
    columns = {name: str(var.dtype) for name, var in ds.VARS.items()}
    expected = {name: f"<U{width}" for name, width in COLUMNS.items()}
    if coords != [TIME] or ds.COORDS[TIME].sorted or columns != expected:
        raise ValueError(
            f"{SCHEMA_PATH} must have the unsorted coordinate {TIME!r} and the variables {expected}, "
            f"it has the coordinates {coords} and the variables {columns}"
        )


class Manifest:
    def __init__(self, url: str):
        self.url = url
        self._schema = zf.schema.deserialize(SCHEMA_PATH)
        check_schema(self._schema)

    def _node(self) -> zf.Node:
        # zf.open_store would take the URL from ZF_STORE_URL, which points to the data store.
        options = {**zarr_storage._zarr_fuse_options(None), "STORE_URL": self.url}
        store = zarr_storage._zarr_store_open(options)
        # The default zarr-fuse logger starts a thread per store that never stops.
        return zf.Node("", store, new_schema=self._schema, mode="a", logger=LOG)[NODE]

    def states(self) -> dict[str, str]:
        """Current state of every item, i.e. the state in its last row."""
        ds = self._node().dataset
        if TIME not in ds.coords:
            return {}
        order = np.argsort(ds[TIME].values)
        return dict(zip(ds["item_name"].values[order].tolist(), ds["state"].values[order].tolist()))

    def add(self, rows: list[dict]) -> None:
        if not rows:
            return

        node = self._node()
        start = np.datetime64(datetime.now(timezone.utc).replace(tzinfo=None), "us")
        if TIME in node.dataset.coords:
            last = node.dataset[TIME].values.max().astype("datetime64[us]")
            start = max(start, last + np.timedelta64(1, "us"))

        df = pl.DataFrame(
            [{name: str(row.get(name, ""))[:width] for name, width in COLUMNS.items()} for row in rows],
            schema={name: pl.String for name in COLUMNS},
        ).with_columns(
            pl.Series(TIME, start + np.arange(len(rows)).astype("timedelta64[us]")).dt.replace_time_zone("UTC")
        )
        node.update(df)
