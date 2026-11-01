"""Manifest of the ingress queue (#113): one entry per received payload, written by the worker only."""
import logging

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import polars as pl
import xarray as xr
import zarr_fuse as zf

from zarr_fuse import zarr_storage

from .queue_storage import ACCEPTED, FAILED

LOG = logging.getLogger(__name__)

SCHEMA_PATH = Path(__file__).with_name("manifest.yaml")
NODE = "items"
KEY = "received_at"
KEY_DTYPE = "datetime64[us]"
STR_WIDTHS = {
    "item_name": 128,
    "source": 64,
    "state": 16,
    "sha256": 64,
    "data_time_min": 32,
    "data_time_max": 32,
    "error": 256,
    "metadata": 2048,
}
TIME_VARS = ("updated_at",)


def check_schema(schema: zf.schema.NodeSchema) -> None:
    """Raise ValueError if the schema does not define exactly the variables the manifest writes."""
    if NODE not in schema.groups:
        raise ValueError(f"{SCHEMA_PATH}: missing node {NODE!r}")
    ds = schema.groups[NODE].ds

    errors = []
    if list(ds.COORDS) != [KEY]:
        errors.append(f"coordinates {list(ds.COORDS)}, expected [{KEY!r}]")
    elif ds.COORDS[KEY].dtype != np.dtype(KEY_DTYPE) or ds.COORDS[KEY].sorted:
        # A sorted coordinate makes zarr-fuse drop keys older than the newest one.
        errors.append(f"{KEY!r} must be an unsorted {KEY_DTYPE} coordinate")

    expected = {name: np.dtype(f"<U{width}") for name, width in STR_WIDTHS.items()}
    expected.update({name: np.dtype(KEY_DTYPE) for name in TIME_VARS})
    for name in sorted(set(expected) | set(ds.VARS)):
        if name not in ds.VARS:
            errors.append(f"missing variable {name!r}")
        elif name not in expected:
            errors.append(f"unexpected variable {name!r}")
        elif ds.VARS[name].dtype != expected[name]:
            errors.append(f"variable {name!r} is {ds.VARS[name].dtype}, expected {expected[name]}")

    if errors:
        raise ValueError(f"{SCHEMA_PATH}: " + "; ".join(errors))


@dataclass(frozen=True)
class ManifestEntry:
    key: np.datetime64
    item_name: str
    source: str
    state: str
    sha256: str = ""
    data_time_min: str = ""
    data_time_max: str = ""
    error: str = ""
    metadata: str = ""

    def with_state(self, state: str, error: str = "") -> "ManifestEntry":
        return replace(self, state=state, error=error)


def default_manifest_url(queue_url: str) -> str:
    return f"{queue_url.rstrip('/')}/manifest.zarr"


def receipt_key(received_at: str) -> np.datetime64:
    dt = datetime.fromisoformat(received_at)
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return np.datetime64(dt, "us")


def validate_entry(entry: ManifestEntry) -> None:
    """Raise ValueError for a value longer than its variable; zarr-fuse would truncate it."""
    for name, width in STR_WIDTHS.items():
        value = getattr(entry, name)
        if len(value) > width:
            raise ValueError(
                f"Manifest value {name!r} of {entry.item_name!r} has "
                f"{len(value)} characters, at most {width} fit"
            )


class ManifestStore:
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

    def _dataset(self) -> xr.Dataset:
        return self._node().dataset

    def ensure_store(self) -> None:
        self._node()

    @staticmethod
    def _keys(ds: xr.Dataset) -> np.ndarray:
        if KEY not in ds.coords:
            return np.array([], dtype=KEY_DTYPE)
        return ds[KEY].values.astype(KEY_DTYPE)

    @staticmethod
    def _rows(ds: xr.Dataset, index: np.ndarray) -> list[ManifestEntry]:
        if len(index) == 0:
            return []

        rows = ds.isel({KEY: index})[list(STR_WIDTHS)].compute()
        keys = rows[KEY].values.astype(KEY_DTYPE)
        return [
            ManifestEntry(key=keys[i], **{name: str(rows[name].values[i]) for name in STR_WIDTHS})
            for i in range(len(keys))
        ]

    def _entries_in_state(self, state: str) -> list[ManifestEntry]:
        ds = self._dataset()
        if KEY not in ds.coords:
            return []
        entries = self._rows(ds, np.nonzero(ds["state"].values == state)[0])
        return sorted(entries, key=lambda entry: entry.key)

    def pending(self) -> list[ManifestEntry]:
        return self._entries_in_state(ACCEPTED)

    def failed(self) -> list[ManifestEntry]:
        return self._entries_in_state(FAILED)

    def find(self, item_names: set[str]) -> dict[str, ManifestEntry]:
        ds = self._dataset()
        if not item_names or KEY not in ds.coords:
            return {}
        index = np.nonzero(np.isin(ds["item_name"].values, list(item_names)))[0]
        return {entry.item_name: entry for entry in self._rows(ds, index)}

    def record(self, entries: list[ManifestEntry]) -> None:
        """Write the entries, overwriting the stored ones with the same key."""
        if not entries:
            return
        for entry in entries:
            validate_entry(entry)

        positions = {key: i for i, key in enumerate(self._keys(self._dataset()).tolist())}
        new = [entry for entry in entries if entry.key.tolist() not in positions]
        existing = sorted(
            ((positions[entry.key.tolist()], entry) for entry in entries if entry.key.tolist() in positions),
            key=lambda pair: pair[0],
        )

        # zarr-fuse corrupts the rows between non-adjacent updated rows of an unsorted
        # coordinate, so the stored rows are written in runs of consecutive positions.
        run, last = [], None
        for position, entry in existing:
            if run and position != last + 1:
                self._write(run)
                run = []
            run.append(entry)
            last = position
        self._write(run)
        self._write(new)

    def _write(self, entries: list[ManifestEntry]) -> None:
        if not entries:
            return

        now = np.datetime64(datetime.now(timezone.utc).replace(tzinfo=None), "us")
        df = pl.DataFrame(
            {
                KEY: np.array([entry.key for entry in entries], dtype=KEY_DTYPE),
                "updated_at": np.full(len(entries), now, dtype=KEY_DTYPE),
                **{name: [getattr(entry, name) for entry in entries] for name in STR_WIDTHS},
            },
            schema_overrides={name: pl.String for name in STR_WIDTHS},
        ).with_columns(pl.col(KEY, "updated_at").dt.replace_time_zone("UTC"))

        self._node().update(df)

    def register(
        self, entries: list[ManifestEntry]
    ) -> tuple[list[ManifestEntry], dict[str, ManifestEntry]]:
        """
        Add the items that are not in the manifest yet.

        The key of an entry is its receipt time, which has a one-second resolution;
        items received in the same second get keys one microsecond apart. An item
        already stored under its receipt second is not added again.

        Return the added entries, and the stored entries of the items that were
        already there, by item name.
        """
        if not entries:
            return [], {}

        ds = self._dataset()
        keys = self._keys(ds)
        used = set(keys.tolist())

        added: list[ManifestEntry] = []
        known: dict[str, ManifestEntry] = {}
        for entry in entries:
            second = (keys >= entry.key) & (keys < entry.key + np.timedelta64(1, "s"))
            stored = {row.item_name: row for row in self._rows(ds, np.nonzero(second)[0])}
            if entry.item_name in stored:
                known[entry.item_name] = stored[entry.item_name]
                continue

            key = entry.key
            while key.tolist() in used:
                key = key + np.timedelta64(1, "us")
            used.add(key.tolist())
            added.append(replace(entry, key=key))

        self.record(added)
        return added, known
