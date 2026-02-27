from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

import requests


@dataclass
class BackendData:
    api_url: str
    endpoint_name: str
    group_path: str
    client: BackendClient


class BackendClient:
    def __init__(self, api_url: str):
        self.api_url = api_url.rstrip("/")

    def _unwrap_figure(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if isinstance(payload, dict) and "figure" in payload:
            return payload["figure"]
        return payload

    def get_endpoints(self) -> Dict[str, Any]:
        resp = requests.get(f"{self.api_url}/config/endpoints", timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        return payload.get("endpoints", payload)

    def get_endpoint(self, endpoint_name: str) -> Dict[str, Any]:
        resp = requests.get(
            f"{self.api_url}/config/endpoints/{endpoint_name}", timeout=30
        )
        resp.raise_for_status()
        payload = resp.json()
        return payload.get("endpoint", payload)

    def get_structure(self, endpoint_name: str) -> Dict[str, Any]:
        resp = requests.get(
            f"{self.api_url}/s3/structure",
            params={"endpoint": endpoint_name},
            timeout=60,
        )
        resp.raise_for_status()
        payload = resp.json()
        return payload.get("structure", payload)

    def get_map_data(
        self,
        endpoint_name: str,
        group_path: str,
        variable: str = "rock_temp",
        time_index: int = 0,
        depth_index: int = 0,
    ) -> Dict[str, Any]:
        payload = {
            "plot_type": "map",
            "endpoint": endpoint_name,
            "node_path": group_path,
            "selection": {
                "variable": variable,
                "time_index": time_index,
                "depth_index": depth_index,
            },
        }
        resp = requests.post(f"{self.api_url}/s3/plot", json=payload, timeout=60)
        resp.raise_for_status()
        return self._unwrap_figure(resp.json())

    def get_timeseries_data(
        self,
        endpoint_name: str,
        group_path: str,
        lat: float,
        lon: float,
        variable: str = "rock_temp",
    ) -> Dict[str, Any]:
        payload = {
            "plot_type": "timeseries",
            "endpoint": endpoint_name,
            "node_path": group_path,
            "selection": {
                "variable": variable,
                "lat_point": lat,
                "lon_point": lon,
            },
        }
        resp = requests.post(f"{self.api_url}/s3/plot", json=payload, timeout=60)
        resp.raise_for_status()
        return self._unwrap_figure(resp.json())


def load_data(source: str, **kwargs) -> BackendData:
    if source != "api":
        raise NotImplementedError("Only API data source is supported.")

    api_url = kwargs.pop("api_url")
    endpoint_name = kwargs.pop("endpoint_name")
    group_path = kwargs.pop("group_path", "bukov")
    client = BackendClient(api_url)
    return BackendData(
        api_url=api_url,
        endpoint_name=endpoint_name,
        group_path=group_path,
        client=client,
    )
