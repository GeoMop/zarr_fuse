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

        data_var = ds[variable]
        if time_field and time_field in data_var.dims:
            data_var = data_var.isel({time_field: time_index})
        if depth_field and depth_field in data_var.dims:
            data_var = data_var.isel({depth_field: depth_index})

        values = np.array(data_var.values).astype(float).ravel()
        values = np.where(np.isfinite(values), values, np.nan)
        result = {
            "status": "success",
            "lat": _to_json_floats(lat.values),
            "lon": _to_json_floats(lon.values),
            "values": _to_json_floats(values),
            "variable": variable,
            "time_index": time_index,
            "depth_index": depth_index,
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