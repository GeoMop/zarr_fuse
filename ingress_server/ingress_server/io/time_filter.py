import logging

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import xarray as xr
import zarr_fuse as zf
from zarr_fuse.units import DateTimeUnit

from ..data_types import DataObject
from ..models import MetadataModel
from ..queue_storage import FileRef

LOG = logging.getLogger(__name__)


@dataclass(eq=False)
class ExtractedItem:
    """An extracted payload waiting to be written to the zarr store.

    `ref` is the queue item ref ("accepted/<name>"), or a local file path for
    items processed outside the queue (deprecated, see `worker._extract_one`).
    """

    ref: FileRef
    metadata: MetadataModel
    schema_path: Path
    obj: DataObject
    time_key: Any = None

    @property
    def store_key(self) -> str:
        return "::".join((
            str(self.schema_path),
            self.metadata.target_node or "",
            self.metadata.node_path or "",
        ))


def _resolve_node_schema(
    schema_path: Path,
    metadata: MetadataModel,
    schema_cache: dict | None,
) -> "zf.schema.NodeSchema":
    key = str(schema_path)
    if schema_cache is not None and key in schema_cache:
        node = schema_cache[key]
    else:
        node = zf.schema.deserialize(schema_path)
        if schema_cache is not None:
            schema_cache[key] = node

    # Walk the schema tree the same way worker._resolve_target walks the store nodes.
    for path_value in (metadata.target_node, metadata.node_path):
        if not path_value:
            continue

        for part in path_value.strip("/").split("/"):
            if part:
                node = node.groups[part]

    return node


def _find_time_coord(node_schema: "zf.schema.NodeSchema"):
    time_coords = [
        coord
        for coord in node_schema.ds.COORDS.values()
        if isinstance(coord.unit, DateTimeUnit)
    ]
    if len(time_coords) != 1:
        return None
    return time_coords[0]


def _min_time_value(obj: DataObject, coord) -> Any:
    if isinstance(obj, xr.Dataset):
        if coord.name not in obj.coords or obj.coords[coord.name].size == 0:
            return None
        return obj.coords[coord.name].values.min()

    column = coord.df_col or coord.name
    if column not in obj.columns or obj.height == 0:
        return None
    return obj.get_column(column).min()


def make_extracted_item(
    ref: FileRef,
    metadata: MetadataModel,
    schema_path: Path,
    obj: DataObject,
    schema_cache: dict | None = None,
) -> ExtractedItem:
    item = ExtractedItem(
        ref=ref,
        metadata=metadata,
        schema_path=schema_path,
        obj=obj,
    )

    try:
        node_schema = _resolve_node_schema(schema_path, metadata, schema_cache)
        coord = _find_time_coord(node_schema)
        if coord is not None:
            item.time_key = _min_time_value(obj, coord)
    except Exception:
        LOG.warning(
            "Failed to determine data time for %s, it will be stored in receipt order",
            ref,
            exc_info=True,
        )

    return item


def sort_by_data_time(items: list[ExtractedItem]) -> list[ExtractedItem]:
    """
    Order a batch of extracted items by the minimal value of their time
    coordinate, so that writes reach the zarr store in the order of the data
    time instead of the payload receipt time.

    Items are grouped by their target store/node; the data-time ordering is
    applied within each group only. Items without a detectable time keep
    their receipt order and precede the time-ordered ones of the same group
    (the sort is stable).
    """
    def sort_key(item: ExtractedItem):
        if item.time_key is None:
            return (item.store_key, 0, 0)
        return (item.store_key, 1, item.time_key)

    return sorted(items, key=sort_key)
