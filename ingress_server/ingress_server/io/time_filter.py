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


@dataclass(eq=False)
class ExtractedItem:
    """An extracted payload waiting to be written to the zarr store."""

    data_path: Path
    metadata: MetadataModel
    schema_path: Path
    obj: DataObject
    time_key: Any = None


def _normalize_time_value(value: Any) -> Any:
    """
    Normalize a raw time_like_coord value into an aware UTC datetime, a plain
    float (for a numeric time-like index), or None if it cannot be
    interpreted. Keeping both representations lets datetime and numeric
    values be diffed uniformly via `_time_diff`.
    """
    if value is None:
        return None

    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value)
        except ValueError:
            return None
    elif isinstance(value, np.datetime64):
        if np.isnat(value):
            return None
        dt = value.astype("datetime64[us]").item()
    elif isinstance(value, datetime):
        dt = value
    elif hasattr(value, "to_pydatetime"):
        dt = value.to_pydatetime()
    elif isinstance(value, (int, float)):
        return float(value)
    else:
        return None

    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def _time_diff(a: Any, b: Any) -> float:
    """(a - b) as a float: hours for datetimes, raw units for a numeric
    time-like index."""
    if isinstance(a, datetime) and isinstance(b, datetime):
        return (a - b).total_seconds() / 3600.0
    return float(a) - float(b)


def _time_value(obj: DataObject, column: str) -> Any:
    """Read the representative (minimum) value of `column` from the
    extracted object, normalized for sorting/retention comparisons."""
    if isinstance(obj, xr.Dataset):
        if column not in obj.coords or obj.coords[column].size == 0:
            return None
        raw = obj.coords[column].values.min()
    else:
        if column not in obj.columns or obj.height == 0:
            return None
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
        except Exception:
            LOG.warning(
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
    def sort_key(item: ExtractedItem):
        return (0, 0) if item.time_key is None else (1, item.time_key)

    return sorted(items, key=sort_key)


def partition_by_retention(
    items: list[ExtractedItem], retention_time: float
) -> tuple[list[ExtractedItem], list[ExtractedItem]]:
    """
    Split a data-time-sorted batch into items ready to store and items to
    hold back. "Now" is defined as the maximum time_key seen in the batch —
    not the wall clock — so an item is held only until enough newer data has
    actually arrived; the freshest item of a batch is therefore always held.
    Items without a detectable time are always ready. retention_time <= 0
    disables holding entirely.
    """
    if retention_time <= 0:
        return list(items), []

    dated = [item.time_key for item in items if item.time_key is not None]
    if not dated:
        return list(items), []

    current_time = max(dated)

    ready: list[ExtractedItem] = []
    held: list[ExtractedItem] = []
    for item in items:
        if item.time_key is None or _time_diff(current_time, item.time_key) > retention_time:
            ready.append(item)
        else:
            held.append(item)

    return ready, held
