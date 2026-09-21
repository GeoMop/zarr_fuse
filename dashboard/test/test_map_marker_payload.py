from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pytest
import xarray as xr

from dashboard.config import DefaultsConfig, EndpointConfig, SchemaConfig, SchemaFieldsConfig, SourceConfig
from dashboard.data import LocalClient


def _make_endpoint(display_variable: str = "temp") -> EndpointConfig:
    return EndpointConfig(
        name="demo",
        reload_interval=0,
        description="",
        version="0",
        source=SourceConfig(type="s3", store_type="zarr", uri="s3://demo"),
        schema=SchemaConfig(
            file="schema.yaml",
            fields=SchemaFieldsConfig(lat="lat", lon="lon", time="date_time", vertical="depth_level", entity="site_id"),
        ),
        defaults=DefaultsConfig(display_variable=display_variable),
    )


def _make_client(dataset: xr.Dataset, display_variable: str = "temp") -> LocalClient:
    client = LocalClient.__new__(LocalClient)
    client.endpoints_path = None
    client.base_dir = None
    client._nodes = {}
    client._map_data_cache = {}
    client._timeseries_cache = {}
    endpoint = _make_endpoint(display_variable=display_variable)
    client._endpoint_config = lambda endpoint_name: endpoint
    client._get_group = lambda endpoint_name, group_path: SimpleNamespace(dataset=dataset)
    return client


def _assert_marker_payload(result: dict, expected_len: int) -> None:
    assert result["status"] == "success"
    assert len(result["lat"]) == expected_len
    assert len(result["lon"]) == expected_len
    assert len(result["values"]) == expected_len
    assert len(result["marker_meta"]) == expected_len
    assert len(result["has_value"]) == expected_len


def test_static_coordinates_keep_nan_values_visible() -> None:
    dataset = xr.Dataset(
        data_vars={"temp": ("site_id", [1.5, np.nan])},
        coords={
            "site_id": ["a", "b"],
            "lat": ("site_id", [50.0, 51.0]),
            "lon": ("site_id", [14.0, 15.0]),
        },
    )
    client = _make_client(dataset)

    result = client.get_map_data("demo", "/", variable="temp", time_index=0, depth_index=0)

    _assert_marker_payload(result, 2)
    assert result["lat"] == [50.0, 51.0]
    assert result["lon"] == [14.0, 15.0]
    assert result["has_value"] == [True, False]
    assert result["marker_meta"][1]["has_value"] is False
    assert result["marker_meta"][1]["value"] is None


@pytest.mark.parametrize("dims", [("date_time", "site_id"), ("site_id", "date_time")])
def test_time_dependent_coordinates_fill_missing_slice_from_nearest_timestamp(dims: tuple[str, str]) -> None:
    coord_shape = (2, 2)
    lat_values = np.array([[50.0, np.nan], [50.2, 51.2]])
    lon_values = np.array([[14.0, np.nan], [14.2, 15.2]])
    if dims == ("site_id", "date_time"):
        lat_values = lat_values.T
        lon_values = lon_values.T

    dataset = xr.Dataset(
        data_vars={"temp": (("date_time", "site_id"), [[1.0, np.nan], [2.0, 3.0]])},
        coords={
            "date_time": ["2024-01-01", "2024-01-02"],
            "site_id": ["a", "b"],
            "lat": (dims, lat_values),
            "lon": (dims, lon_values),
        },
    )
    client = _make_client(dataset)

    result = client.get_map_data("demo", "/", variable="temp", time_index=0, depth_index=0)

    _assert_marker_payload(result, 2)
    assert np.isfinite(result["lat"][1])
    assert np.isfinite(result["lon"][1])
    assert result["has_value"] == [True, False]
    assert result["marker_meta"][1]["site_id"] == "b"
    assert result["marker_meta"][1]["has_value"] is False


def test_reversed_dim_order_with_missing_value_keeps_coordinates() -> None:
    lat_values = np.array([[50.0, np.nan], [50.4, 51.4]]).T
    lon_values = np.array([[14.0, np.nan], [14.4, 15.4]]).T
    dataset = xr.Dataset(
        data_vars={"temp": (("date_time", "site_id"), [[1.0, np.nan], [2.0, 3.0]])},
        coords={
            "date_time": ["2024-01-01", "2024-01-02"],
            "site_id": ["a", "b"],
            "lat": (("site_id", "date_time"), lat_values),
            "lon": (("site_id", "date_time"), lon_values),
        },
    )
    client = _make_client(dataset)

    result = client.get_map_data("demo", "/", variable="temp", time_index=0, depth_index=0)

    _assert_marker_payload(result, 2)
    assert np.isfinite(result["lat"][1])
    assert np.isfinite(result["lon"][1])
    assert result["values"][1] is None
    assert result["marker_meta"][1]["has_value"] is False
