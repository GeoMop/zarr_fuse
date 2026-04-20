from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
import sys
import time
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd
import zarr_fuse as zf
import os


def _timer_log(message: str, duration: float) -> None:
    print(f"[timing] {message}: {duration:.3f}s")


# Set CONFIG_ROOT to the dashboard directory
CONFIG_ROOT = Path(__file__).resolve().parent

# Import config directly (no sys.path manipulation needed with proper package structure)
from dashboard.config import get_endpoint_config, load_endpoints, resolve_schema_fields


@dataclass
class DashboardData:
    endpoint_name: str
    group_path: str
    client: "LocalClient"


@dataclass
class EndpointHandle:
    name: str
    schema_path: Path
    store_uri: str


class LocalClient:
    def __init__(self, endpoints_path: Path):
        self.endpoints_path = Path(endpoints_path)
        self.base_dir = self.endpoints_path.parent.parent
        self._nodes: Dict[str, Any] = {}

    def get_endpoints(self) -> Dict[str, Any]:
        endpoints = load_endpoints(self.endpoints_path)
        return {name: asdict(endpoint) for name, endpoint in endpoints.items()}

    def get_endpoint(self, endpoint_name: str) -> Dict[str, Any]:
        endpoint = get_endpoint_config(self.endpoints_path, endpoint_name)
        return asdict(endpoint)

    def _endpoint_config(self, endpoint_name: Optional[str]):
        if endpoint_name is None:
            raise ValueError("endpoint_name is required")
        return get_endpoint_config(self.endpoints_path, endpoint_name)

    def _endpoint_handle(self, endpoint_name: Optional[str]) -> EndpointHandle:
        endpoint = self._endpoint_config(endpoint_name)
        schema_path = Path(endpoint.schema.file)
        if not schema_path.is_absolute():
            schema_path = self.base_dir / schema_path

        return EndpointHandle(
            name=endpoint.name,
            schema_path=schema_path,
            store_uri=endpoint.source.uri,
        )

    def _open_node(self, endpoint_name: Optional[str]) -> Any:
        handle = self._endpoint_handle(endpoint_name)
        if handle.name in self._nodes:
            return self._nodes[handle.name]

        schema = zf.schema.deserialize(handle.schema_path)
        schema.ds.ATTRS["STORE_URL"] = handle.store_uri
        node = zf.open_store(schema, MODE="r")
        self._nodes[handle.name] = node
        return node

    def _get_group(self, endpoint_name: Optional[str], group_path: str):
        node = self._open_node(endpoint_name)
        if not group_path or group_path == "/":
            return node

        path_parts = [p for p in group_path.strip("/").split("/") if p]
        for part in path_parts:
            if part not in node.children:
                raise KeyError(f"Group '{group_path}' not found")
            node = node.children[part]
        return node

    def get_structure(self, endpoint_name: Optional[str]) -> Dict[str, Any]:
        node = self._open_node(endpoint_name)

        def build(n):
            return {
                "name": n.name or "root",
                "path": n.group_path or "/",
                "children": [build(child) for child in n.children.values()],
            }

        return build(node)

    def get_map_data(
        self,
        endpoint_name: Optional[str],
        group_path: str,
        variable: Optional[str] = None,
        time_index: int = 0,
        depth_index: int = 0,
    ) -> Dict[str, Any]:
        start = time.perf_counter()
        endpoint = self._endpoint_config(endpoint_name)
        fields = resolve_schema_fields(endpoint.schema, group_path)

        variable = variable or endpoint.defaults.display_variable
        lat_field = fields.lat
        lon_field = fields.lon
        time_field = fields.time
        depth_field = fields.vertical

        if not variable:
            _timer_log("get_map_data failed", time.perf_counter() - start)
            return {"status": "error", "reason": "No default display variable configured"}

        if not lat_field or not lon_field:
            _timer_log("get_map_data failed", time.perf_counter() - start)
            return {"status": "error", "reason": "lat/lon mapping is not configured"}

        node = self._get_group(endpoint_name, group_path)
        ds = node.dataset
        if variable not in ds:
            _timer_log("get_map_data failed", time.perf_counter() - start)
            return {"status": "error", "reason": f"Variable '{variable}' not found"}

        lat = ds.get(lat_field)
        lon = ds.get(lon_field)
        if lat is None or lon is None:
            _timer_log("get_map_data failed", time.perf_counter() - start)
            return {"status": "error", "reason": f"{lat_field}/{lon_field} not found"}

        data_var_full = ds[variable]

        def _isel_if_has_dim(array, dim_name: Optional[str], index: int):
            if dim_name and dim_name in array.dims:
                return array.isel({dim_name: index})
            return array

        def _candidate_indices(size: int, preferred: int) -> list[int]:
            if size <= 1:
                return [0]
            preferred_idx = preferred if 0 <= preferred < size else 0
            return [preferred_idx] + [i for i in range(size) if i != preferred_idx]

        def _slice_map_arrays(t_idx: int, d_idx: int):
            lat_sel = _isel_if_has_dim(_isel_if_has_dim(lat, time_field, t_idx), depth_field, d_idx)
            lon_sel = _isel_if_has_dim(_isel_if_has_dim(lon, time_field, t_idx), depth_field, d_idx)
            data_sel = _isel_if_has_dim(_isel_if_has_dim(data_var_full, time_field, t_idx), depth_field, d_idx)

            lats_local = np.array(lat_sel.values, dtype=float).ravel()
            lons_local = np.array(lon_sel.values, dtype=float).ravel()
            values_local = np.array(data_sel.values, dtype=float).ravel()
            values_local = np.where(np.isfinite(values_local), values_local, np.nan)

            if len(lats_local) != len(lons_local) or len(lats_local) != len(values_local):
                return None

            valid_count = int(np.sum(np.isfinite(lats_local) & np.isfinite(lons_local) & np.isfinite(values_local)))
            return lats_local, lons_local, values_local, valid_count

        selected_time_index = time_index
        selected_depth_index = depth_index
        sliced = _slice_map_arrays(selected_time_index, selected_depth_index)

        if sliced is None:
            _timer_log("get_map_data failed", time.perf_counter() - start)
            return {
                "status": "error",
                "reason": (
                    "Coordinate/value lengths do not match "
                    "for selected map slice"
                ),
            }

        lats, lons, values, valid_count = sliced

        if valid_count == 0:
            time_size = int(data_var_full.sizes.get(time_field, 1)) if time_field and time_field in data_var_full.dims else 1
            depth_size = int(data_var_full.sizes.get(depth_field, 1)) if depth_field and depth_field in data_var_full.dims else 1

            # Try other slices only when the selected one has no usable points.
            for t_idx in _candidate_indices(time_size, selected_time_index):
                for d_idx in _candidate_indices(depth_size, selected_depth_index):
                    if t_idx == selected_time_index and d_idx == selected_depth_index:
                        continue
                    candidate = _slice_map_arrays(t_idx, d_idx)
                    if candidate is None:
                        continue
                    cand_lats, cand_lons, cand_values, cand_valid = candidate
                    if cand_valid > 0:
                        lats, lons, values = cand_lats, cand_lons, cand_values
                        selected_time_index = t_idx
                        selected_depth_index = d_idx
                        break
                else:
                    continue
                break

        result = {
            "status": "success",
            "lat": _to_json_floats(lats),
            "lon": _to_json_floats(lons),
            "values": _to_json_floats(values),
            "variable": variable,
            "time_index": selected_time_index,
            "depth_index": selected_depth_index,
        }
        _timer_log("get_map_data", time.perf_counter() - start)
        return result

    def get_timeseries_data(
        self,
        endpoint_name: Optional[str],
        group_path: str,
        lat: float,
        lon: float,
        variable: Optional[str] = None,
    ) -> Dict[str, Any]:
        start = time.perf_counter()
        endpoint = self._endpoint_config(endpoint_name)
        fields = resolve_schema_fields(endpoint.schema, group_path)

        variable = variable or endpoint.defaults.display_variable
        lat_field = fields.lat
        lon_field = fields.lon
        time_field = fields.time
        depth_field = fields.vertical
        entity_field = fields.entity

        if not variable:
            _timer_log("get_timeseries_data failed", time.perf_counter() - start)
            return {"status": "error", "reason": "No default display variable configured"}

        if not lat_field or not lon_field or not time_field:
            _timer_log("get_timeseries_data failed", time.perf_counter() - start)
            return {
                "status": "error",
                "reason": "lat/lon/time mapping is not fully configured for this group",
            }

        node = self._get_group(endpoint_name, group_path)
        ds = node.dataset
        if variable not in ds:
            _timer_log("get_timeseries_data failed", time.perf_counter() - start)
            return {"status": "error", "reason": f"Variable '{variable}' not found"}

        lat_var = ds.get(lat_field)
        lon_var = ds.get(lon_field)
        if lat_var is None or lon_var is None:
            _timer_log("get_timeseries_data failed", time.perf_counter() - start)
            return {"status": "error", "reason": f"{lat_field}/{lon_field} not found"}

        if time_field and time_field in lat_var.dims:
            lat_var = lat_var.isel({time_field: 0})
        if time_field and time_field in lon_var.dims:
            lon_var = lon_var.isel({time_field: 0})
        if depth_field and depth_field in lat_var.dims:
            lat_var = lat_var.isel({depth_field: 0})
        if depth_field and depth_field in lon_var.dims:
            lon_var = lon_var.isel({depth_field: 0})

        lats = np.array(lat_var.values, dtype=float).ravel()
        lons = np.array(lon_var.values, dtype=float).ravel()
        dist = (lats - lat) ** 2 + (lons - lon) ** 2
        idx = int(np.nanargmin(dist))

        data_var = ds[variable]
        if entity_field and entity_field in data_var.dims:
            data_var = data_var.isel({entity_field: idx})

        times = ds[time_field]
        time_values = pd.to_datetime(times.values).astype(str).tolist()

        values = np.array(data_var.values, dtype=float)
        if values.ndim == 1:
            series = [_to_json_floats(values)]
        else:
            series = [_to_json_floats(values[:, i]) for i in range(values.shape[1])]

        if depth_field and depth_field in ds:
            depths = ds[depth_field]
            depth_values = np.array(depths.values, dtype=float).tolist()
        else:
            depth_values = [np.nan] * max(len(series), 1)

        result = {
            "status": "success",
            "times": time_values,
            "depths": _to_json_floats(depth_values),
            "series": series,
            "variable": variable,
            "borehole_index": idx,
        }
        _timer_log("get_timeseries_data", time.perf_counter() - start)
        return result


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

def load_data(source: str, **kwargs) -> DashboardData:
    if source not in {"local", "direct", "zarr_fuse"}:
        raise NotImplementedError("Only local zarr_fuse data sources are supported.")

    # Support ENDPOINTS_PATH environment variable for external project configuration
    endpoints_path = kwargs.pop(
        "endpoints_path",
        os.getenv(
            "ENDPOINTS_PATH",
            str(CONFIG_ROOT.parent / "app" / "databuk" / "config" / "endpoints.yaml")
        ),
    )
    endpoints_path = Path(endpoints_path)
    
    endpoint_name = kwargs.pop("endpoint_name")
    
    if not endpoints_path.exists():
        raise FileNotFoundError(
            f"Endpoints file not found: {endpoints_path}. "
            "Set ENDPOINTS_PATH environment variable to point to your endpoints.yaml"
        )
    
    client = LocalClient(endpoints_path)

    endpoint = get_endpoint_config(endpoints_path, endpoint_name)
    group_path = kwargs.pop("group_path", None) or endpoint.defaults.group_path
    if not group_path:
        raise ValueError("group_path is required (set defaults.group_path or pass group_path explicitly)")

    return DashboardData(
        endpoint_name=endpoint_name,
        group_path=group_path,
        client=client,
    )