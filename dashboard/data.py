from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sys
import time
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd
import zarr_fuse as zf

def _timer_log(message: str, duration: float) -> None:
    print(f"[timing] {message}: {duration:.3f}s")

CONFIG_ROOT = Path(__file__).resolve().parents[1] / "app" / "databuk" / "dashboard" / "holoviz_dashboard"
if str(CONFIG_ROOT) not in sys.path:
    sys.path.insert(0, str(CONFIG_ROOT))

from config.dashboard_config import get_endpoint_config, load_endpoints


@dataclass
class DashboardData:
    endpoint_name: str
    group_path: str
    client: "LocalClient"


@dataclass
class EndpointHandle:
    name: str
    schema_path: Path
    store_url: str


class LocalClient:
    def __init__(self, endpoints_path: Path):
        self.endpoints_path = Path(endpoints_path)
        self.base_dir = self.endpoints_path.parent.parent
        self._nodes: Dict[str, Any] = {}

    def get_endpoints(self) -> Dict[str, Any]:
        endpoints = load_endpoints(self.endpoints_path)
        return {name: endpoint.__dict__ for name, endpoint in endpoints.items()}

    def get_endpoint(self, endpoint_name: str) -> Dict[str, Any]:
        endpoint = get_endpoint_config(self.endpoints_path, endpoint_name)
        return endpoint.__dict__

    def _endpoint_handle(self, endpoint_name: Optional[str]) -> EndpointHandle:
        endpoints = load_endpoints(self.endpoints_path)
        if endpoint_name is None:
            endpoint_name = next(iter(endpoints.keys()))
        endpoint = get_endpoint_config(self.endpoints_path, endpoint_name)
        schema_path = Path(endpoint.schema_file)
        if not schema_path.is_absolute():
            schema_path = self.base_dir / schema_path
        return EndpointHandle(endpoint_name, schema_path, endpoint.store_url)

    def _open_node(self, endpoint_name: Optional[str]) -> Any:
        handle = self._endpoint_handle(endpoint_name)
        if handle.name in self._nodes:
            return self._nodes[handle.name]
        schema = zf.schema.deserialize(handle.schema_path)
        schema.ds.ATTRS["STORE_URL"] = handle.store_url
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
        variable: str = "rock_temp",
        time_index: int = 0,
        depth_index: int = 0,
    ) -> Dict[str, Any]:
        start = time.perf_counter()
        node = self._get_group(endpoint_name, group_path)
        ds = node.dataset
        if variable not in ds:
            _timer_log("get_map_data failed", time.perf_counter() - start)
            return {"status": "error", "reason": f"Variable '{variable}' not found"}

        lat = ds.get("latitude")
        lon = ds.get("longitude")
        if lat is None or lon is None:
            _timer_log("get_map_data failed", time.perf_counter() - start)
            return {"status": "error", "reason": "latitude/longitude not found"}

        data_var = ds[variable]
        if "date_time" in data_var.dims:
            data_var = data_var.isel(date_time=time_index)
        if "depth" in data_var.dims:
            data_var = data_var.isel(depth=depth_index)

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
        variable: str = "rock_temp",
    ) -> Dict[str, Any]:
        start = time.perf_counter()
        node = self._get_group(endpoint_name, group_path)
        ds = node.dataset
        if variable not in ds:
            _timer_log("get_timeseries_data failed", time.perf_counter() - start)
            return {"status": "error", "reason": f"Variable '{variable}' not found"}

        lat_var = ds.get("latitude")
        lon_var = ds.get("longitude")
        if lat_var is None or lon_var is None:
            _timer_log("get_timeseries_data failed", time.perf_counter() - start)
            return {"status": "error", "reason": "latitude/longitude not found"}

        lats = np.array(lat_var.values, dtype=float).ravel()
        lons = np.array(lon_var.values, dtype=float).ravel()
        dist = (lats - lat) ** 2 + (lons - lon) ** 2
        idx = int(np.nanargmin(dist))

        data_var = ds[variable]
        if "borehole" in data_var.dims:
            data_var = data_var.isel(borehole=idx)

        times = ds.get("date_time")
        time_values = []
        if times is not None:
            time_values = pd.to_datetime(times.values).astype(str).tolist()

        depths = ds.get("depth")
        depth_values = []
        if depths is not None:
            depth_values = np.array(depths.values, dtype=float).tolist()

        values = np.array(data_var.values, dtype=float)
        if values.ndim == 1:
            series = [_to_json_floats(values)]
        else:
            series = [_to_json_floats(values[:, i]) for i in range(values.shape[1])]
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

    endpoints_path = kwargs.pop(
        "endpoints_path",
        CONFIG_ROOT / "config" / "endpoints.yaml",
    )
    endpoint_name = kwargs.pop("endpoint_name")
    group_path = kwargs.pop("group_path", "bukov")
    client = LocalClient(endpoints_path)
    return DashboardData(
        endpoint_name=endpoint_name,
        group_path=group_path,
        client=client,
    )
