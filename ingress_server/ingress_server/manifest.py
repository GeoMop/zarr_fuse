"""
Manifest of the ingress queue: an auxiliary zarr-fuse store keeping one entry
per received payload (issue #113).

Source of truth
---------------
The manifest is the source of truth for the processing state of a queue item.
The `accepted` / `success` / `failed` queue folders only mirror that state for
convenience: a payload is moved after its new state is recorded, and a payload
found in a folder that disagrees with its entry is moved by the worker later.

Single writer
-------------
zarr-fuse does not support concurrent writers, so only the worker thread
writes the manifest, and only one ingress server may run at a time (the helm
chart uses the Recreate strategy). The receipt path (passive endpoints, active
scrappers) stores the payload with its metadata sidecar in `accepted/`; the
worker registers every accepted payload that has no manifest entry yet.

The sidecar stays next to its payload. The manifest keeps the item metadata
without the unbounded `dataframe_row` when the full JSON does not fit its
variable; the sidecar is then read for it. The sidecars also keep the queue
readable by an ingress server without the manifest.

Keys
----
Entries are keyed by the receipt time. `MetadataModel.received_at` has a
one-second resolution, so `register` adds the smallest free microsecond offset
to keep the keys unique; being the only writer, it can do so safely.

zarr-fuse constraints behind the schema
---------------------------------------
- The key coordinate is unsorted: a sorted coordinate silently drops keys
  older than the newest stored one.
- A datetime variable must not contain NaT, reading it back fails. The data
  time is unknown before extraction and may also be numeric, so it is stored
  as a string, empty when unknown.
- A discrete string range fails when writing, so `state` is a plain string.
- Overwriting non-adjacent rows of an unsorted coordinate corrupts the rows
  between them, so `record` writes runs of consecutive rows.
- zarr-fuse writes the arrays one by one, so an interrupted write can leave
  them inconsistent; `_repair` fixes that when the store is opened.
- `zf.open_store` lets the ZF_STORE_URL environment variable override the
  store URL, which would put the manifest into the data store. The manifest
  store is therefore built from explicit options.

NOTE: this module must not import app_config or io.* (app_config imports it).
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
# Longest run of unchanged rows `record` rewrites to join two runs of changed rows.
MAX_GAP_FILL = 64

# Width of the string variables; `record` rejects longer values instead of
# letting zarr-fuse truncate them.
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

# Arrays of the manifest node, the key coordinate included.
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
    """One manifest row, i.e. the state of a single queue item."""

    key: np.datetime64
    item_name: str
    source: str
    state: str
    sha256: str = ""
    data_time_min: str = ""
    data_time_max: str = ""
    error: str = ""
    metadata: str = ""
    # Time of the last change, as read from the store; `record` sets it anew.
    updated_at: np.datetime64 | None = field(default=None, compare=False)

    def with_state(self, state: str, error: str = "") -> "ManifestEntry":
        return replace(self, state=state, error=error)


def default_manifest_url(queue_url: str) -> str:
    """Manifest location next to the queue folders: ``<queue root>/manifest.zarr``."""
    return f"{queue_url.rstrip('/')}/manifest.zarr"


def receipt_key(received_at: str) -> np.datetime64:
    """Manifest key base of an item from its ISO `received_at` timestamp."""
    dt = datetime.fromisoformat(received_at)
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return np.datetime64(dt, "us")


def validate_entry(entry: ManifestEntry) -> None:
    """Raise ValueError if a value is too long for its variable; zarr-fuse would truncate it."""
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
    """
    zarr-fuse store options of the manifest. Credentials come from the same
    ZF_S3_* variables the queue and the data stores use, but the URL is
    never taken from ZF_STORE_URL, see the module docstring.
    """
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
    """
    Access to the manifest store. The store is opened anew for every
    operation, so that each worker pass sees its current content.
    """

    def __init__(self, url: str):
        self.url = url
        self._options = _store_options(url)
        self._schema = zf.schema.deserialize(yaml.safe_load(SCHEMA_YAML))

    def _open_node(self) -> zf.Node:
        # A private zarr-fuse function: the public `zf.open_store` would let
        # ZF_STORE_URL redirect the manifest into the data store.
        store = zarr_storage._zarr_store_open(self._options)
        # An explicit logger: the default zarr-fuse store logger starts an
        # event loop thread per store object and never closes it.
        node = zf.Node("", store, new_schema=self._schema, mode="a", logger=LOG)[NODE]
        # Surface an inconsistent store here, see `_repair`: arrays of different
        # lengths fail to open, missing arrays are just absent from the dataset.
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
        """
        Repair the damage an interrupted write leaves behind; return whether
        anything was repaired. zarr-fuse writes the arrays one by one:

        - An interrupted append leaves arrays of different lengths. They are
          cut to the shortest one; the rows cut off belong to a registration
          that did not complete, so their items get registered again.
        - An interrupted first write leaves some arrays missing. Only that
          first registration was in the store, so the node is dropped and
          recreated empty.
        """
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
        """Create the store if missing; fail fast on a misconfigured backend."""
        self._node()

    # --- reads ---

    @staticmethod
    def _keys(ds: xr.Dataset) -> np.ndarray:
        if KEY not in ds.coords:
            return np.array([], dtype=KEY_DTYPE)
        return ds[KEY].values.astype(KEY_DTYPE)

    @staticmethod
    def _rows(ds: xr.Dataset, index: np.ndarray) -> list[ManifestEntry]:
        """Entries of the given row positions, reading only their chunks."""
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
        """Entries still waiting for processing, in the receipt order."""
        return self._entries_in_state(ACCEPTED)

    def failed(self) -> list[ManifestEntry]:
        return self._entries_in_state(FAILED)

    def find(self, item_names: set[str]) -> dict[str, ManifestEntry]:
        """
        Entries of the given item names. Scans the whole item_name column, one
        chunk at a time; meant for the rare payloads without a sidecar, whose
        receipt time is unknown. Items with a sidecar are looked up by
        `register`, within their receipt second.
        """
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

    # --- writes ---

    def record(self, entries: list[ManifestEntry]) -> None:
        """
        Write complete entries, adding new keys and overwriting existing ones.
        Raise ValueError on a value too long for its variable, before writing
        anything (see `validate_entry`).
        """
        if not entries:
            return

        for entry in entries:
            validate_entry(entry)

        # Keep the last entry of a repeated key.
        by_key = {entry.key.astype(KEY_DTYPE).tolist(): entry for entry in entries}
        ds = self._dataset()
        positions = {key: i for i, key in enumerate(self._keys(ds).tolist())}

        new = [entry for key, entry in by_key.items() if key not in positions]
        existing = sorted(
            (positions[key], entry) for key, entry in by_key.items() if key in positions
        )

        # zarr-fuse overwrites existing rows of an unsorted coordinate as one
        # region from the first to the last of them, and corrupts the rows in
        # between that are not part of the update (strings become 'nan').
        # Every write therefore covers consecutive rows only. Each write costs
        # a full zarr-fuse update, so short gaps are filled with the unchanged
        # rows read back from the store, making one run out of several.
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
        """
        One zarr-fuse update of the changed `entries`, stamped with the current
        time, and of the `unchanged` entries, which keep their own time.
        """
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
        Record new items. The `key` of a given entry is its receipt second,
        the registered entry gets the smallest free microsecond within it.

        An item already present within its receipt second is not recorded
        again: its stored entry is returned instead, so that the caller can
        tell a new item from one registered before (e.g. one whose move to its
        state folder was interrupted).

        Return the registered entries and the stored entries by item name.
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
