import logging

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import xarray as xr

from ..data_types import DataObject
from ..models import MetadataModel

LOG = logging.getLogger(__name__)


class TimeKeyError(ValueError):
    """A time_like_coord value could not be turned into a usable time key."""


@dataclass(eq=False)
class ExtractedItem:
    """An extracted payload waiting to be written to the zarr store."""

    data_path: Path
    metadata: MetadataModel
    schema_path: Path
    obj: DataObject
    time_key: Any = None
    time_error: str | None = None


def _normalize_time_value(value: Any) -> Any:
    """
    Normalize a raw time_like_coord value into an aware UTC datetime or a
    plain float (for a numeric time-like index). Keeping both representations
    lets datetime and numeric values be diffed uniformly via `_time_diff`.
    Raises TimeKeyError if the value cannot be interpreted.
    """
    if value is None:
        raise TimeKeyError("time value is None")

    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value)
        except ValueError as exc:
            raise TimeKeyError(f"unparseable time string {value!r}") from exc
    elif isinstance(value, np.datetime64):
        if np.isnat(value):
            raise TimeKeyError("time value is NaT")
        dt = value.astype("datetime64[us]").item()
    elif isinstance(value, datetime):
        dt = value
    elif hasattr(value, "to_pydatetime"):
        dt = value.to_pydatetime()
    elif isinstance(value, (int, float)):
        return float(value)
    else:
        raise TimeKeyError(
            f"unsupported time value type {type(value).__name__}: {value!r}"
        )

    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def _time_kind(value: Any) -> str:
    """Which of the two `_normalize_time_value` representations this key is.
    Only keys of the same kind may be compared or diffed."""
    return "datetime" if isinstance(value, datetime) else "numeric"


def _time_diff(a: Any, b: Any) -> float:
    """(a - b) as a float: hours for datetimes, raw units for a numeric
    time-like index. Raises TimeKeyError if the two keys are of different
    kinds, which means two sources disagree on how time is represented."""
    if _time_kind(a) != _time_kind(b):
        raise TimeKeyError(
            f"cannot compare time keys of different types: "
            f"{type(a).__name__}({a!r}) and {type(b).__name__}({b!r})"
        )

    if isinstance(a, datetime):
        return (a - b).total_seconds() / 3600.0
    return float(a) - float(b)


def _time_value(obj: DataObject, column: str) -> Any:
    """Read the representative (minimum) value of `column` from the
    extracted object, normalized for sorting/retention comparisons."""
    if isinstance(obj, xr.Dataset):
        if column not in obj.coords:
            raise TimeKeyError(f"coordinate {column!r} not in the extracted dataset")
        if obj.coords[column].size == 0:
            raise TimeKeyError(f"coordinate {column!r} is empty")
        raw = obj.coords[column].values.min()
    else:
        if column not in obj.columns:
            raise TimeKeyError(f"column {column!r} not in the extracted dataframe")
        if obj.height == 0:
            raise TimeKeyError(f"column {column!r} is empty")
        raw = obj.get_column(column).min()

    return _normalize_time_value(raw)


def make_extracted_item(
    data_path: Path,
    metadata: MetadataModel,
    schema_path: Path,
    obj: DataObject,
) -> ExtractedItem:
    item = ExtractedItem(
        data_path=data_path,
        metadata=metadata,
        schema_path=schema_path,
        obj=obj,
    )

    if metadata.time_like_coord:
        try:
            item.time_key = _time_value(obj, metadata.time_like_coord)
        except Exception as exc:
            item.time_error = f"time_like_coord={metadata.time_like_coord!r}: {exc}"
            LOG.error(
                "Failed to read time_like_coord=%r for %s, it will be stored in receipt order",
                metadata.time_like_coord,
                data_path,
                exc_info=True,
            )

    return item


def sort_by_data_time(items: list[ExtractedItem]) -> list[ExtractedItem]:
    """
    Order a batch of extracted items by their time_like_coord value, so that
    writes reach the zarr store in the order of the data time instead of the
    payload receipt time — even across items destined for different schema
    nodes (a single source can feed several nodes). Items without a
    detectable time keep their receipt order and precede the time-ordered
    ones (the sort is stable).
    """
    # The kind rank keeps datetime and numeric keys in separate comparison
    # groups, so a batch mixing both still sorts instead of raising.
    kind_rank = {"datetime": 0, "numeric": 1}

    def sort_key(item: ExtractedItem):
        if item.time_key is None:
            return (0, 0, 0.0)
        return (1, kind_rank[_time_kind(item.time_key)], item.time_key)

    return sorted(items, key=sort_key)


def time_key_type_conflict(items: list[ExtractedItem]) -> str | None:
    """
    Describe a batch whose time keys are not all of the same kind, or None if
    they are. Mixing datetime and numeric time keys means two sources disagree
    on how time is represented; each kind is then sorted and held on its own,
    which is atypical enough to report.
    """
    kinds: dict[str, list[str]] = {}
    for item in items:
        if item.time_key is not None:
            kinds.setdefault(_time_kind(item.time_key), []).append(item.data_path.name)

    if len(kinds) < 2:
        return None

    return "batch mixes time key types: " + ", ".join(
        f"{kind} ({', '.join(names)})" for kind, names in sorted(kinds.items())
    )


def partition_by_retention(
    items: list[ExtractedItem], retention_time: float
) -> tuple[list[ExtractedItem], list[ExtractedItem]]:
    """
    Split a data-time-sorted batch into items ready to store and items to
    hold back. "Now" is defined as the maximum time_key seen in the batch —
    not the wall clock — so an item is held only until enough newer data has
    actually arrived; the freshest item of a batch is therefore always held.
    Items without a detectable time are always ready. A batch mixing datetime
    and numeric time keys gets one "now" per kind (see
    `time_key_type_conflict`). retention_time <= 0 disables holding entirely.
    """
    if retention_time <= 0:
        return list(items), []

    current_time: dict[str, Any] = {}
    for item in items:
        if item.time_key is not None:
            kind = _time_kind(item.time_key)
            current_time[kind] = max(item.time_key, current_time.get(kind, item.time_key))

    ready: list[ExtractedItem] = []
    held: list[ExtractedItem] = []
    for item in items:
        if item.time_key is None:
            ready.append(item)
            continue

        newest = current_time[_time_kind(item.time_key)]
        if _time_diff(newest, item.time_key) > retention_time:
            ready.append(item)
        else:
            held.append(item)

    return ready, held
