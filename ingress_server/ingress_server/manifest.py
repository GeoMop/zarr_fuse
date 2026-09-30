"""
Manifest of the ingress queue (#113): a zarr-fuse store with one entry per
received payload, the source of truth for the item state. Written by the
worker only.
"""
import os
import logging

from dataclasses import dataclass, field, replace
from datetime import datetime, timezone

import numpy as np
import polars as pl
import xarray as xr
import yaml
import zarr
import zarr_fuse as zf

from zarr_fuse import zarr_storage

from .queue_storage import ACCEPTED, FAILED, S3_SCHEME

LOG = logging.getLogger(__name__)

NODE = "items"
KEY = "received_at"
KEY_DTYPE = "datetime64[us]"
KEY_TICK = np.timedelta64(1, "us")
KEY_WINDOW = np.timedelta64(1, "s")
MAX_GAP_FILL = 64

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

ARRAYS = {KEY, "updated_at", *STR_WIDTHS}

SCHEMA_YAML = f"""
ATTRS:
  description: "Ingress queue manifest, one entry per received payload."
{NODE}:
  COORDS:
    {KEY}:
      description: "Payload receipt time; microsecond offsets keep the keys unique."
      unit: {{tick: "us", tz: "UTC"}}
      sorted: false
      chunk_size: 256
  VARS:
    item_name:
      description: "Payload object name within its queue folder."
      type: "str[{STR_WIDTHS['item_name']}]"
      coords: {KEY}
    source:
      description: "Name of the endpoint or active scrapper that received the payload."
      type: "str[{STR_WIDTHS['source']}]"
      coords: {KEY}
    state:
      description: "Processing state: accepted, success or failed."
      type: "str[{STR_WIDTHS['state']}]"
      coords: {KEY}
    sha256:
      description: "SHA-256 hex digest of the payload, computed at receipt."
      type: "str[{STR_WIDTHS['sha256']}]"
      coords: {KEY}
    data_time_min:
      description: "Minimum of the time_like_coord values of the payload, empty when unknown."
      type: "str[{STR_WIDTHS['data_time_min']}]"
      coords: {KEY}
    data_time_max:
      description: "Maximum of the time_like_coord values of the payload, empty when unknown."
      type: "str[{STR_WIDTHS['data_time_max']}]"
      coords: {KEY}
    updated_at:
      description: "Time of the last change of the entry."
      unit: {{tick: "us", tz: "UTC"}}
      coords: {KEY}
    error:
      description: "Reason of the failure of a failed item, empty otherwise."
      type: "str[{STR_WIDTHS['error']}]"
      coords: {KEY}
    metadata:
      description: "JSON of the item metadata (MetadataModel)."
      type: "str[{STR_WIDTHS['metadata']}]"
      coords: {KEY}
"""


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
    updated_at: np.datetime64 | None = field(default=None, compare=False)

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


def _utc_now() -> np.datetime64:
    return np.datetime64(datetime.now(timezone.utc).replace(tzinfo=None), "us")


def _store_options(url: str) -> dict[str, str]:
    if url.startswith("file://"):
        url = url[len("file://"):]

    options = {"STORE_URL": url}
    if url.startswith(S3_SCHEME):
        for option in ("S3_ACCESS_KEY", "S3_SECRET_KEY", "S3_ENDPOINT_URL", "S3_OPTIONS"):
            value = os.getenv(f"ZF_{option}")
            if value:
                options[option] = value
    return options


class ManifestStore:
    def __init__(self, url: str):
        self.url = url
        self._options = _store_options(url)
        self._schema = zf.schema.deserialize(yaml.safe_load(SCHEMA_YAML))

    def _open_node(self) -> zf.Node:
        # Not zf.open_store: ZF_STORE_URL would redirect the manifest into the data store.
        store = zarr_storage._zarr_store_open(self._options)
        # The default zarr-fuse logger starts a thread per store that never ends.
        node = zf.Node("", store, new_schema=self._schema, mode="a", logger=LOG)[NODE]
        ds = node.dataset
        if KEY in ds.coords and not ARRAYS <= set(ds.variables):
            raise ValueError(f"Manifest {self.url} lacks the arrays {sorted(ARRAYS - set(ds.variables))}")
        return node

    def _node(self) -> zf.Node:
        try:
            return self._open_node()
        except Exception:
            if not self._repair():
                raise
            return self._open_node()

    def _repair(self) -> bool:
        """Fix the arrays an interrupted write left inconsistent; return whether anything changed."""
        store = zarr_storage._zarr_store_open(self._options)
        try:
            group = zarr.open_group(store, path=NODE, mode="r+")
        except Exception:
            return False

        arrays = dict(group.arrays())
        if arrays and not ARRAYS <= set(arrays):
            LOG.error(
                "Manifest %s lacks the arrays %s after an interrupted creation, recreating it",
                self.url,
                sorted(ARRAYS - set(arrays)),
            )
            del zarr.open_group(store, mode="r+")[NODE]
            return True

        lengths = {name: array.shape[0] for name, array in arrays.items() if name in ARRAYS}
        if len(set(lengths.values())) <= 1:
            return False

        length = min(lengths.values())
        LOG.error(
            "Manifest %s has arrays of different lengths %s after an interrupted write, "
            "cutting them to %d rows",
            self.url,
            lengths,
            length,
        )
        for name, array_length in lengths.items():
            if array_length > length:
                arrays[name].resize((length,))
        return True

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

        rows = ds.isel({KEY: index})[[*STR_WIDTHS, "updated_at"]].compute()
        keys = rows[KEY].values.astype(KEY_DTYPE)
        updated_at = rows["updated_at"].values.astype(KEY_DTYPE)
        columns = {name: rows[name].values for name in STR_WIDTHS}

        return [
            ManifestEntry(
                key=keys[i],
                updated_at=updated_at[i],
                **{name: str(columns[name][i]) for name in STR_WIDTHS},
            )
            for i in range(len(keys))
        ]

    def _entries_in_state(self, state: str) -> list[ManifestEntry]:
        ds = self._dataset()
        if len(self._keys(ds)) == 0:
            return []

        index = np.nonzero(ds["state"].values == state)[0]
        entries = self._rows(ds, index)
        return sorted(entries, key=lambda entry: entry.key)

    def pending(self) -> list[ManifestEntry]:
        return self._entries_in_state(ACCEPTED)

    def failed(self) -> list[ManifestEntry]:
        return self._entries_in_state(FAILED)

    def find(self, item_names: set[str]) -> dict[str, ManifestEntry]:
        """Entries of the given item names; scans the whole item_name column."""
        if not item_names:
            return {}

        ds = self._dataset()
        if len(self._keys(ds)) == 0:
            return {}

        wanted = list(item_names)
        index = []
        offset = 0
        for block in ds["item_name"].data.blocks:
            names = np.asarray(block.compute())
            index.extend(offset + np.nonzero(np.isin(names, wanted))[0])
            offset += len(names)

        return {entry.item_name: entry for entry in self._rows(ds, np.array(index, dtype=int))}

    def record(self, entries: list[ManifestEntry]) -> None:
        if not entries:
            return

        for entry in entries:
            validate_entry(entry)

        by_key = {entry.key.astype(KEY_DTYPE).tolist(): entry for entry in entries}
        ds = self._dataset()
        positions = {key: i for i, key in enumerate(self._keys(ds).tolist())}

        new = [entry for key, entry in by_key.items() if key not in positions]
        existing = sorted(
            (positions[key], entry) for key, entry in by_key.items() if key in positions
        )

        # zarr-fuse corrupts the rows between non-adjacent updated rows of an unsorted
        # coordinate, so every write covers consecutive rows; short gaps are refilled.
        runs: list[list[tuple[int, ManifestEntry]]] = []
        for position, entry in existing:
            if runs and position - runs[-1][-1][0] - 1 <= MAX_GAP_FILL:
                runs[-1].append((position, entry))
            else:
                runs.append([(position, entry)])

        for run in runs:
            first, last = run[0][0], run[-1][0]
            updated = {position for position, _ in run}
            gap = np.array([p for p in range(first, last + 1) if p not in updated], dtype=int)
            self._write([entry for _, entry in run], unchanged=self._rows(ds, gap))
        self._write(new)

    def _write(self, entries: list[ManifestEntry], unchanged: list[ManifestEntry] = ()) -> None:
        rows = [*entries, *unchanged]
        if not rows:
            return

        now = _utc_now()
        updated_at = [now] * len(entries) + [
            entry.updated_at if entry.updated_at is not None else now for entry in unchanged
        ]
        df = pl.DataFrame(
            {
                KEY: np.array([entry.key for entry in rows], dtype=KEY_DTYPE),
                "updated_at": np.array(updated_at, dtype=KEY_DTYPE),
                **{name: [getattr(entry, name) for entry in rows] for name in STR_WIDTHS},
            },
            schema_overrides={name: pl.String for name in STR_WIDTHS},
        ).with_columns(pl.col(KEY, "updated_at").dt.replace_time_zone("UTC"))

        self._node().update(df)

    def register(
        self, entries: list[ManifestEntry]
    ) -> tuple[list[ManifestEntry], dict[str, ManifestEntry]]:
        """
        Record the new items under a free key within their receipt second.
        Return them and the stored entries of the items registered before.
        """
        if not entries:
            return [], {}

        ds = self._dataset()
        keys = self._keys(ds)
        used = set(keys.tolist())

        registered: list[ManifestEntry] = []
        known: dict[str, ManifestEntry] = {}
        for entry in entries:
            base = entry.key.astype(KEY_DTYPE)
            window = np.nonzero((keys >= base) & (keys < base + KEY_WINDOW))[0]
            stored = {row.item_name: row for row in self._rows(ds, window)}
            if entry.item_name in stored:
                known[entry.item_name] = stored[entry.item_name]
                continue

            key = base
            while key.tolist() in used:
                key = key + KEY_TICK
            used.add(key.tolist())
            registered.append(replace(entry, key=key))

        self.record(registered)
        return registered, known
