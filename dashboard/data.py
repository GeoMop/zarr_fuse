from __future__ import annotations

import logging
from bisect import bisect_left, bisect_right
from collections import OrderedDict
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import datetime
from operator import index
from os import getenv
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd
import xarray as xr
from zarr.abc.store import Store
from zarr.storage import ZipStore

import zarr_fuse as zf
from dashboard.config import (
    SchemaFieldsConfig,
    find_view_file,
    load_view_config,
    load_views,
    read_variable_metadata,
    resolve_schema_fields,
)
from dashboard.perf_summary import count, stage
from dashboard.timeseries_overview import overview_path, write_overview
from zarr_fuse.zarr_storage import _zarr_fuse_options, _zarr_store_open

logger = logging.getLogger(__name__)


@dataclass
class DashboardData:
    view_name: str
    group_path: str
    display_variable: str
    client: "LocalClient"


@dataclass
class ViewHandle:
    name: str
    schema_path: Path
    store_uri: str


class _TimeseriesWindowCache:
    """Group metadata and recent materialized windows for one cache generation."""

    MAX_WINDOWS = 32

    def __init__(self) -> None:
        self.lock = Lock()
        self.groups: dict[
            tuple[str, str], tuple[xr.Dataset, SchemaFieldsConfig, pd.DatetimeIndex]
        ] = {}
        self.overviews: dict[tuple[str, str, str], xr.Dataset | None] = {}
        self.windows: OrderedDict[tuple, xr.DataArray] = OrderedDict()


def _copy_timeseries_window(window: xr.DataArray) -> xr.DataArray:
    copied = window.copy(deep=True)
    # Rebuild indexed coordinate variables from the copied indexes. xarray's
    # deep copy can leave those variables backed by the original index buffers.
    coordinates = dict(copied.coords.variables)
    for coordinate_index, variables in copied.xindexes.group_by_index():
        coordinates.update(coordinate_index.create_variables(variables))
    return xr.DataArray(
        copied.variable,
        coords=xr.Coordinates(coordinates, indexes=copied.xindexes),
        name=copied.name,
    )


class LocalClient:
    def __init__(self, views_path: Path):
        self.views_path = Path(views_path).expanduser().resolve()
        self.base_dir = self.views_path.parent.parent
        self._nodes: Dict[str, Any] = {}
        self._map_data_cache: Dict[str, Any] = {}
        self._timeseries_window_cache = _TimeseriesWindowCache()

    def clear_cache(self) -> None:
        """Replace the active caches so in-flight requests cannot repopulate them.

        Requests capture their cache at entry and can finish writing to that
        detached cache after a clear.
        """
        self._map_data_cache = {}
        self._timeseries_window_cache = _TimeseriesWindowCache()
        print("[cache] Cleared all caches")

    def get_views(self) -> Dict[str, Any]:
        views = load_views(self.views_path)
        return {name: asdict(view) for name, view in views.items()}

    def get_view(self, view_name: str) -> Dict[str, Any]:
        view = load_view_config(self.views_path, view_name)
        return asdict(view)

    def _view_config(self, view_name: Optional[str]):
        if view_name is None:
            raise ValueError("view_name is required")
        return load_view_config(self.views_path, view_name)

    def _view_handle(self, view_name: Optional[str]) -> ViewHandle:
        view = self._view_config(view_name)
        schema_path = Path(view.schema.file)
        if not schema_path.is_absolute():
            schema_path = self.base_dir / schema_path

        return ViewHandle(
            name=view.name,
            schema_path=schema_path,
            store_uri=view.source.uri,
        )

    def _open_node(self, view_name: Optional[str]) -> Any:
        handle = self._view_handle(view_name)
        if handle.name in self._nodes:
            count("store.cache_hit")
            return self._nodes[handle.name]

        with stage("store.open"):
            schema = zf.schema.deserialize(handle.schema_path)
            schema.ds.ATTRS["STORE_URL"] = handle.store_uri
            node = zf.open_store(schema, MODE="r")
        self._nodes[handle.name] = node
        return node

    def _get_group(self, view_name: Optional[str], group_path: str):
        node = self._open_node(view_name)
        if not group_path or group_path == "/":
            return node

        path_parts = [p for p in group_path.strip("/").split("/") if p]
        for part in path_parts:
            if part not in node.children:
                raise KeyError(f"Group '{group_path}' not found")
            node = node.children[part]
        return node

    def get_structure(self, view_name: Optional[str]) -> Dict[str, Any]:
        node = self._open_node(view_name)

        def build(n):
            return {
                "name": n.name or "root",
                "path": n.group_path or "/",
                "children": [build(child) for child in n.children.values()],
            }

        return build(node)

    def get_variables(self, view_name: Optional[str], group_path: str) -> Dict[str, str]:
        node = self._get_group(view_name, group_path)
        ds = node.dataset

        # Exclude coordinate variables - only show actual data variables
        coord_names = set()
        for coord in ds.coords:
            coord_names.add(coord)
        for var_name in ds.coords:
            coord_names.add(var_name)

        variables = {}
        for var_name in ds.data_vars:
            # Skip coordinate-like variables
            if var_name.lower() in ("latitude", "longitude", "lat", "lon", "x", "y"):
                continue
            
            var = ds[var_name]
            unit = ""
            if hasattr(var, "units") and var.units:
                unit = str(var.units)
            elif hasattr(var, "attrs") and var.attrs.get("units"):
                unit = str(var.attrs.get("units"))
            variables[var_name] = unit

        return variables

    def get_variable_metadata(
        self,
        view_name: Optional[str],
        group_path: str,
        variable_name: str,
    ) -> Optional[Dict[str, Any]]:
        view = self._view_config(view_name)
        schema_path = Path(view.schema.file)
        if not schema_path.is_absolute():
            schema_path = self.base_dir / schema_path

        return read_variable_metadata(
            schema_path,
            variable_name=variable_name,
            group_path=group_path,
        )

    def get_map_data(
        self,
        view_name: str | None,
        group_path: str,
        variable: str | None = None,
        time_index: int = 0,
        depth_index: int = 0,
    ) -> dict[str, Any]:
        with stage("get_map_data"):
            return self._get_map_data(view_name, group_path, variable, time_index, depth_index)

    def _get_map_data(
        self,
        view_name: str | None,
        group_path: str,
        variable: str | None = None,
        time_index: int = 0,
        depth_index: int = 0,
    ) -> dict[str, Any]:
        # Capture the active cache once; a concurrent clear_cache() replaces the
        # attribute, so this request must keep using the dictionary it started
        # with for both lookup and insertion.
        cache = self._map_data_cache
        cache_key = f"{view_name}:{group_path}:{variable}:{time_index}:{depth_index}"
        if cache_key in cache:
            cached = cache[cache_key]
            print(f"[cache] get_map_data: cache hit for {variable}")
            count("map.cache_hit")
            return cached
            
        view = self._view_config(view_name)
        fields = resolve_schema_fields(view.schema, group_path)

        variable = variable or view.defaults.display_variable
        lat_field = fields.lat if fields else None
        lon_field = fields.lon if fields else None
        time_field = fields.time if fields else None
        depth_field = fields.vertical if fields else None
        entity_field = fields.entity if fields else None

        if not variable:
            return {"status": "error", "reason": "No default display variable configured"}

        if not lat_field or not lon_field:
            return {"status": "error", "reason": "lat/lon mapping is not configured"}

        with stage("map.read"):
            node = self._get_group(view_name, group_path)
            ds = node.dataset
            if variable not in ds:
                return {"status": "error", "reason": f"Variable '{variable}' not found"}

            lat = ds.get(lat_field)
            lon = ds.get(lon_field)
            entity_var = ds.get(entity_field) if entity_field else None
            if lat is None or lon is None:
                return {"status": "error", "reason": f"{lat_field}/{lon_field} not found"}

            data_var_full = ds[variable]

        def _isel_if_has_dim(array, dim_name: Optional[str], index: int):
            if dim_name and dim_name in array.dims:
                return array.isel({dim_name: index})
            return array

        def _slice_map_arrays(t_idx: int, d_idx: int):
            def _coords_for_map(coord_var, time_idx: int):
                if coord_var is None:
                    return None, None, 0

                dims = tuple(coord_var.dims)
                raw = np.array(coord_var.values)

                # If entity_field present we expect per-entity coords
                if entity_field and entity_field in dims:
                    time_ax = dims.index(time_field) if (time_field and time_field in dims) else None
                    ent_ax = dims.index(entity_field)

                    if time_ax is None:
                        coords = np.array(coord_var.values)
                        coords_1d = coords.reshape(-1)
                        return coords_1d, coords.dtype, coords_1d.size

                    arr = np.array(coord_var.values)
                    if arr.ndim == 1:
                        coords_1d = arr.ravel()
                        return coords_1d, arr.dtype, coords_1d.size

                    if time_ax != 0 or ent_ax != 1:
                        arr = np.moveaxis(arr, (time_ax, ent_ax), (0, 1))

                    T, E = arr.shape[0], arr.shape[1]

                    try:
                        valid_mask = np.isfinite(arr.astype(float))
                    except Exception:
                        valid_mask = ~pd.isna(arr)

                    t_idx_clamped = max(0, min(int(time_idx), T - 1))
                    slice_vals = arr[t_idx_clamped, :]

                    filled = np.array(slice_vals, dtype=float)
                    nan_mask = ~valid_mask[t_idx_clamped, :]
                    if nan_mask.any():
                        for j in np.flatnonzero(nan_mask):
                            found = False
                            for offset in range(1, max(T - t_idx_clamped, t_idx_clamped) + 1):
                                for cand in (t_idx_clamped - offset, t_idx_clamped + offset):
                                    if 0 <= cand < T and valid_mask[cand, j]:
                                        try:
                                            filled[j] = float(arr[cand, j])
                                            found = True
                                        except Exception:
                                            filled[j] = np.nan
                                        break
                                if found:
                                    break
                            if not found:
                                filled[j] = np.nan

                    return filled.ravel(), arr.dtype, E

                flat = raw.ravel()
                return flat, raw.dtype, flat.size

            # selected data slice for logging and value extraction
            data_sel = _isel_if_has_dim(_isel_if_has_dim(data_var_full, time_field, t_idx), depth_field, d_idx)

            # build 1D coords per entity (or flattened fallback)
            lat_1d, lat_dtype, lat_count = _coords_for_map(lat, t_idx)
            lon_1d, lon_dtype, lon_count = _coords_for_map(lon, t_idx)
            if entity_var is not None:
                ent_1d_raw, ent_dtype, ent_count = _coords_for_map(entity_var, t_idx)
            else:
                ent_1d_raw, ent_dtype, ent_count = (None, None, 0)

            # Logging dims and shapes
            print(f"[map][debug] variable='{variable}' original_dims={data_var_full.dims} original_shape={data_var_full.shape}")
            print(f"[map][debug] selected_slice_dims={data_sel.dims} selected_slice_shape={data_sel.shape}")
            print(f"[map][debug] lat_dims={getattr(lat, 'dims', None)} lat_shape={getattr(lat, 'shape', None)}")
            print(f"[map][debug] lon_dims={getattr(lon, 'dims', None)} lon_shape={getattr(lon, 'shape', None)}")

            values_arr = np.array(data_sel.values)

            # If data_sel has an entity dimension, try to reduce to one value per entity
            if entity_field and entity_field in data_sel.dims:
                dims = tuple(data_sel.dims)
                ent_ax = dims.index(entity_field)
                if ent_ax != values_arr.ndim - 1:
                    values_arr = np.moveaxis(values_arr, ent_ax, -1)
                if values_arr.ndim > 1:
                    values_1d = values_arr.reshape(-1, values_arr.shape[-1])
                    vals = np.full(values_1d.shape[1], np.nan)
                    for j in range(values_1d.shape[1]):
                        col = values_1d[:, j]
                        try:
                            finite_idx = np.flatnonzero(np.isfinite(col))
                            if finite_idx.size:
                                vals[j] = float(col[finite_idx[0]])
                            else:
                                vals[j] = np.nan
                        except Exception:
                            valid = (~pd.isna(col)) & (col != None)
                            idx = np.flatnonzero(valid)
                            vals[j] = col[idx[0]] if idx.size else None
                    values_1d_final = vals
                else:
                    values_1d_final = values_arr.astype(float).ravel()
            else:
                try:
                    values_1d_final = np.array(values_arr, dtype=float).ravel()
                except Exception:
                    values_1d_final = np.array(values_arr).ravel()

            entity_count = ent_count or lat_count or lon_count or values_1d_final.size

            def _ensure_len(arr, count):
                if arr is None:
                    return np.full(count, np.nan)
                a = np.array(arr).ravel()
                if a.size == count:
                    return a
                if a.size < count:
                    out = np.full(count, np.nan)
                    out[: a.size] = a
                    return out
                return a[:count]

            lats_local = _ensure_len(lat_1d, entity_count)
            lons_local = _ensure_len(lon_1d, entity_count)
            values_local = _ensure_len(values_1d_final, entity_count)

            coord_valid_mask = np.isfinite(lats_local) & np.isfinite(lons_local)
            entity_indices = np.arange(entity_count)
            try:
                value_valid_mask = np.isfinite(values_local)
            except Exception:
                value_valid_mask = ~pd.isna(values_local)

            total = int(entity_count)
            valid_coord_count = int(np.sum(coord_valid_mask))
            finite_value_count = int(np.sum(value_valid_mask & coord_valid_mask))
            missing_coord_count = int(np.sum(~coord_valid_mask))
            missing_value_count = int(np.sum(coord_valid_mask & ~value_valid_mask))
            displayed_markers = valid_coord_count

            print(
                f"[map] Location counts - total entities: {total}, valid coordinate markers: {valid_coord_count}, finite selected values: {finite_value_count}, missing selected values: {missing_value_count}, displayed markers: {displayed_markers}"
            )

            if valid_coord_count > 0:
                keep_mask = coord_valid_mask
                lats_local = lats_local[keep_mask]
                lons_local = lons_local[keep_mask]
                values_local = values_local[keep_mask]
                entities_local = ent_1d_raw[keep_mask] if ent_1d_raw is not None else None
                entity_indices = entity_indices[keep_mask]
                value_valid_mask = value_valid_mask[keep_mask]
            else:
                entities_local = ent_1d_raw
                value_valid_mask = np.array([], dtype=bool)

            marker_meta = []
            for idx in range(len(lats_local)):
                entity_value = entities_local[idx] if entities_local is not None and idx < len(entities_local) else None
                value_value = values_local[idx] if idx < len(values_local) else np.nan
                has_value = bool(np.isfinite(value_value)) if np.isscalar(value_value) else False
                marker_meta.append(
                    {
                        "entity_index": int(entity_indices[idx]),
                        "site_id": None if entity_value is None else str(entity_value),
                        "value": None if not has_value else float(value_value),
                        "has_value": has_value,
                    }
                )

            return lats_local, lons_local, values_local, valid_coord_count, entities_local, marker_meta, value_valid_mask

        selected_time_index = time_index
        selected_depth_index = depth_index
        with stage("map.convert"):
            sliced = _slice_map_arrays(selected_time_index, selected_depth_index)

            if sliced is None:
                return {
                    "status": "error",
                    "reason": "Coordinate/value lengths do not match for selected map slice",
                }

            lats, lons, values, valid_count, entities, marker_meta, value_valid_mask = sliced

            if valid_count == 0:
                return {
                    "status": "error",
                    "reason": "No valid lat/lon points for selected map slice",
                }

            result = {
                "status": "success",
                "lat": _to_json_floats(lats),
                "lon": _to_json_floats(lons),
                "values": _to_json_floats(values),
                "entities": entities.tolist() if entities is not None else None,
                "marker_meta": marker_meta,
                "has_value": value_valid_mask.tolist() if hasattr(value_valid_mask, "tolist") else [],
                "variable": variable,
                "time_index": selected_time_index,
                "depth_index": selected_depth_index,
            }
        
        # Cache the result in the dictionary captured at request start.
        cache[cache_key] = result
        return result

    def _timeseries_group(
        self,
        cache: _TimeseriesWindowCache,
        view_name: str,
        group_path: str,
    ) -> tuple[xr.Dataset, SchemaFieldsConfig, pd.DatetimeIndex]:
        group_key = (view_name, group_path)
        with stage("timeseries_window.metadata"):
            with cache.lock:
                group = cache.groups.get(group_key)
            if group is None:
                view = self._view_config(view_name)
                fields = resolve_schema_fields(view.schema, group_path)
                if fields is None:
                    raise ValueError(
                        f"No schema field mapping for group {group_path!r}"
                    )
                ds = self._get_group(view_name, group_path).dataset
                time_index = ds.indexes.get(fields.time)
                if not isinstance(time_index, pd.DatetimeIndex):
                    raise ValueError(
                        f"{fields.time!r} must be a decoded datetime dimension coordinate"
                    )
                if time_index.hasnans or not time_index.is_monotonic_increasing:
                    raise ValueError(
                        f"{fields.time!r} must be ascending and contain no NaT"
                    )
                with cache.lock:
                    group = cache.groups.setdefault(group_key, (ds, fields, time_index))
            else:
                count("timeseries_window.metadata_hit")
        return group

    def _overview(self, cache, view_name, group_path, array) -> xr.Dataset | None:
        directory = getenv("ZF_TIMESERIES_OVERVIEW_DIR")
        if not directory:
            return None
        key = (view_name, group_path, array.name)
        with cache.lock:
            if key in cache.overviews:
                return cache.overviews[key]
        path = overview_path(
            Path(directory),
            getenv("ZF_STORE_URL", self._view_handle(view_name).store_uri),
            group_path,
            array,
        )
        summary = None
        if path.is_dir():
            try:
                candidate = xr.open_zarr(path, chunks=None, consolidated=False)
                if {
                    "available",
                    "start",
                    "length",
                    "timestamp",
                    "value",
                } <= candidate.data_vars.keys():
                    summary = candidate
            except (OSError, ValueError, KeyError):
                logger.warning(
                    "Cannot open time-series overview %s", path, exc_info=True
                )
        with cache.lock:
            return cache.overviews.setdefault(key, summary)

    def get_timeseries_site_metadata(
        self,
        view_name: str,
        group_path: str,
        *,
        variable: str,
        entity_index: int | None,
        lat: float = 0.0,
        lon: float = 0.0,
    ) -> dict[str, Any]:
        """Describe a site and history availability without reading measurement values."""
        cache = self._timeseries_window_cache
        group_path = group_path.strip("/")
        ds, fields, times = self._timeseries_group(cache, view_name, group_path)
        array = ds[variable]
        if entity_index is None:

            def coordinate(name):
                field = ds[name]
                return field.isel(
                    {dim: 0 for dim in field.dims if dim != fields.entity}
                ).values

            distances = (coordinate(fields.lat) - lat) ** 2 + (
                coordinate(fields.lon) - lon
            ) ** 2
            entity_index = int(np.nanargmin(distances))
        if not 0 <= entity_index < array.sizes[fields.entity]:
            raise IndexError(f"Invalid entity index {entity_index} for {variable!r}")
        single_trace = fields.vertical not in array.dims
        # Depth 0 is the selection-table position of a depthless measurement.
        depths = [0.0] if single_trace else ds[fields.vertical].values
        summary = self._overview(cache, view_name, group_path, array)
        availability = None
        if summary is not None:
            try:
                known = summary.available.isel(entity_position=entity_index).values
                if len(known) == len(depths):
                    availability = known
            except (OSError, ValueError, KeyError, IndexError):
                logger.warning(
                    "Cannot load overview availability for %s", variable, exc_info=True
                )
        name = ds[fields.entity].isel({fields.entity: entity_index}).values.item()
        if times.tz is not None:
            times = times.tz_convert("UTC").tz_localize(None)
        return {
            "status": "success",
            "borehole_index": entity_index,
            "borehole_name": str(name),
            "depths": depths,
            "times": times[[0, -1]] if len(times) else times,
            "availability": availability,
            "single_trace": single_trace,
        }

    def get_timeseries_overview_data(
        self,
        view_name: str,
        group_path: str,
        *,
        variable: str,
        entity_index: int,
        depth_indices: tuple[int, ...],
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> dict[int, tuple[pd.DatetimeIndex, np.ndarray]] | None:
        """Return persisted traces or None if no matching source snapshot exists."""
        cache = self._timeseries_window_cache
        group_path = group_path.strip("/")
        ds, _, _ = self._timeseries_group(cache, view_name, group_path)
        summary = self._overview(cache, view_name, group_path, ds[variable])
        if summary is None:
            return None
        spans = summary[["start", "length"]].isel(
            entity_position=entity_index, depth_position=list(depth_indices)
        )
        result = {}
        for depth, offset, size in zip(
            depth_indices, spans.start.values, spans.length.values
        ):
            points = summary.isel(point=slice(offset, offset + size))
            times = pd.DatetimeIndex(points.timestamp.values)
            visible = slice(bisect_left(times, start), bisect_right(times, end))
            result[depth] = (times[visible], points.value.isel(point=visible).values)
        return result

    def _overview_source(self, handle: ViewHandle) -> tuple[str, Store]:
        """Resolve the configured source without the write-capable Node initializer."""
        schema = zf.schema.deserialize(handle.schema_path)
        schema.ds.ATTRS["STORE_URL"] = handle.store_uri
        options = _zarr_fuse_options(schema)
        uri = options["STORE_URL"]
        if uri.startswith("zip://"):
            return uri, ZipStore(uri.removeprefix("zip://"), mode="r")
        return uri, _zarr_store_open(options).with_read_only(True)

    def generate_timeseries_overview(
        self,
        view_name: str,
        group_path: str,
        variable: str,
        directory: Path,
        *,
        bins: int = 750,
    ) -> Path:
        """Preprocess one variable into a new sidecar, leaving the source store read-only."""
        group_path = group_path.strip("/")
        fields = resolve_schema_fields(self._view_config(view_name).schema, group_path)
        if fields is None:
            raise ValueError(f"No schema field mapping for group {group_path!r}")
        uri, store = self._overview_source(self._view_handle(view_name))
        with store, xr.open_zarr(store, group=group_path, consolidated=False) as ds:
            array = ds[variable]
            destination = overview_path(directory, uri, group_path, array)
            write_overview(
                array,
                time_dim=fields.time,
                entity_dim=fields.entity,
                depth_dim=fields.vertical,
                destination=destination,
                bins=bins,
            )
        return destination

    def get_timeseries_window_data(
        self,
        view_name: str,
        group_path: str,
        *,
        variable: str,
        entity_index: int,
        start: str | datetime | np.datetime64,
        end: str | datetime | np.datetime64,
        depth_indices: Sequence[int] | None = None,
    ) -> xr.DataArray:
        """Read one entity over the closed interval ``[start, end]``.

        Bounds are UTC, including naive inputs. All samples at either endpoint
        are included, including duplicate timestamps. The stored time index
        must be decoded, ascending, and free of NaT.

        Return an eager DataArray in the variable's original dimension order
        with the entity dimension selected as a scalar. Depth indices are
        zero-based and returned in the requested order. Use None for all depths
        or for a variable without a depth dimension. Empty intervals retain
        their time dimension and dtype. Missing measurements are preserved.
        """
        cache = self._timeseries_window_cache
        group_path = group_path.strip("/")
        group_key = (view_name, group_path)
        start, end = (pd.Timestamp(bound) for bound in (start, end))
        if pd.isna(start) or pd.isna(end):
            raise ValueError("Window bounds cannot be NaT")
        start, end = (
            bound.tz_convert("UTC").tz_localize(None) if bound.tzinfo else bound
            for bound in (start, end)
        )
        if start > end:
            raise ValueError("Window start must be at or before end")

        entity_index = index(entity_index)
        depths = None if depth_indices is None else tuple(map(index, depth_indices))
        key = (*group_key, variable, entity_index, depths, start, end)
        with stage("timeseries_window.cache"):
            with cache.lock:
                result = cache.windows.get(key)
                if result is not None:
                    cache.windows.move_to_end(key)
            if result is not None:
                count("timeseries_window.cache_hit")
                return _copy_timeseries_window(result)

        ds, fields, time_index = self._timeseries_group(cache, view_name, group_path)

        with stage("timeseries_window.select"):
            array = ds[variable]
            if fields.time not in array.dims or fields.entity not in array.dims:
                raise ValueError(
                    f"{variable!r} must have the mapped time and entity dimensions"
                )
            if not 0 <= entity_index < array.sizes[fields.entity]:
                raise IndexError(
                    f"Invalid entity index {entity_index} for {variable!r}"
                )
            if time_index.tz is not None:
                start, end = (bound.tz_localize("UTC") for bound in (start, end))
            # Compare timestamps without casting bounds to the index's unit.
            selectors: dict[str, slice | int | list[int]] = {
                fields.time: slice(
                    bisect_left(time_index, start),
                    bisect_right(time_index, end),
                ),
                fields.entity: entity_index,
            }
            if depths is not None:
                depth_dim = fields.vertical
                if depth_dim is None or depth_dim not in array.dims:
                    raise ValueError(f"{variable!r} has no mapped depth dimension")
                if any(
                    depth < 0 or depth >= array.sizes[depth_dim] for depth in depths
                ):
                    raise IndexError(f"Invalid depth indices {depths} for {variable!r}")
                selectors[depth_dim] = list(depths)
            selected = array.isel(selectors)

        with stage("timeseries_window.read"):
            result = selected.load()
        with cache.lock:
            cache.windows[key] = result
            cache.windows.move_to_end(key)
            if len(cache.windows) > cache.MAX_WINDOWS:
                cache.windows.popitem(last=False)
        return _copy_timeseries_window(result)


def _to_json_floats(values: Any) -> list:
    arr = np.array(values, dtype=float).ravel()
    arr = np.where(np.isfinite(arr), arr, np.nan)
    out = []
    for value in arr.tolist():
        if value != value or value in (float("inf"), float("-inf")):
            out.append(None)
        else:
            out.append(float(value))
    return out


def load_data(
    source: str,
    *,
    view_name: str,
    views_path: Optional[Path] = None,
    group_path: Optional[str] = None,
    display_variable: Optional[str] = None,
    **kwargs,
) -> DashboardData:
    if source not in {"local", "direct", "zarr_fuse"}:
        raise NotImplementedError("Only local zarr_fuse data sources are supported.")

    if kwargs:
        unexpected = ", ".join(sorted(kwargs.keys()))
        raise TypeError(f"Unexpected keyword argument(s): {unexpected}")

    resolved_views_path = (
        Path(views_path).expanduser().resolve()
        if views_path is not None
        else find_view_file()
    )

    if not resolved_views_path.exists():
        raise FileNotFoundError(
            f"Views file not found: {resolved_views_path}. "
            "Set ZF_VIEW_PATH environment variable to point to your zf_view.yaml"
        )

    client = LocalClient(resolved_views_path)

    view = load_view_config(resolved_views_path, view_name)
    resolved_group_path = group_path or view.defaults.group_path
    if not resolved_group_path:
        raise ValueError("group_path is required (set defaults.group_path or pass group_path explicitly)")

    resolved_display_variable = display_variable or view.defaults.display_variable or ""

    return DashboardData(
        view_name=view_name,
        group_path=resolved_group_path,
        display_variable=resolved_display_variable,
        client=client,
    )
