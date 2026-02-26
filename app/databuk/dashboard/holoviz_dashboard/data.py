from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import zarr_fuse as zf

from config.dashboard_config import get_endpoint_config


@dataclass
class BukovData:
    group: object
    map_df: pd.DataFrame
    overlay_bounds: tuple[float, float, float, float]
    lats: np.ndarray
    lons: np.ndarray
    depth_arr: np.ndarray
    date_time_index: pd.DatetimeIndex


def to_datetime_index(values, units: str | None = None) -> pd.DatetimeIndex:
    arr = np.array(values)
    if np.issubdtype(arr.dtype, np.datetime64):
        return pd.to_datetime(arr)

    if units and "since" in units:
        unit_part, origin_part = units.split("since", 1)
        unit_part = unit_part.strip().lower()
        origin_part = origin_part.strip()
        unit_map = {
            "seconds": "s",
            "second": "s",
            "minutes": "m",
            "minute": "m",
            "hours": "h",
            "hour": "h",
            "days": "D",
            "day": "D",
        }
        unit_code = unit_map.get(unit_part, "s")
        return pd.to_datetime(arr, unit=unit_code, origin=origin_part, utc=True).tz_convert(None)

    if np.issubdtype(arr.dtype, np.integer) or np.issubdtype(arr.dtype, np.floating):
        return pd.to_datetime(arr, unit="s", utc=True).tz_convert(None)

    return pd.to_datetime(arr, errors="coerce")


def get_overlay_bounds_from_coords(
    lats: np.ndarray,
    lons: np.ndarray,
    pad_ratio: float = 0.05,
) -> tuple[float, float, float, float]:
    lat_min, lat_max = np.nanmin(lats), np.nanmax(lats)
    lon_min, lon_max = np.nanmin(lons), np.nanmax(lons)
    lat_pad = (lat_max - lat_min) * pad_ratio if lat_max > lat_min else 0.01
    lon_pad = (lon_max - lon_min) * pad_ratio if lon_max > lon_min else 0.01
    return (lon_min - lon_pad, lat_min - lat_pad, lon_max + lon_pad, lat_max + lat_pad)


def _dashboard_root() -> Path:
    return Path(__file__).resolve().parent


def _default_schema_path() -> Path:
    return _dashboard_root() / "schemas" / "bukov_schema.yaml"


def load_bukov_node(
    data_root: Path | None = None,
    schema_path: Path | None = None,
    store_url: str | None = None,
    s3_endpoint_url: str | None = None,
    mode: str = "r",
):
    schema_path = schema_path or _default_schema_path()
    schema = zf.schema.deserialize(schema_path)
    if store_url:
        schema.ds.ATTRS["STORE_URL"] = store_url
    elif data_root is not None:
        schema.ds.ATTRS["STORE_URL"] = str(data_root)

    if s3_endpoint_url:
        schema.ds.ATTRS["S3_ENDPOINT_URL"] = s3_endpoint_url

    return zf.open_store(schema, MODE=mode)


def load_bukov_group(
    data_root: Path | None,
    group_name: str = "bukov",
    schema_path: Path | None = None,
    store_url: str | None = None,
    s3_endpoint_url: str | None = None,
    mode: str = "r",
):
    root = load_bukov_node(
        data_root,
        schema_path=schema_path,
        store_url=store_url,
        s3_endpoint_url=s3_endpoint_url,
        mode=mode,
    )
    target = root[group_name] if group_name in root.children else root
    return target.dataset


def load_bukov_map_data(
    group,
    var_name: str = "rock_temp",
    time_index: int = 0,
    depth_index: int = 0,
):
    lats = np.array(group["latitude"][:], dtype=float)
    lons = np.array(group["longitude"][:], dtype=float)

    if var_name not in group:
        raise KeyError(f"Variable '{var_name}' not found in Bukov group")

    values = np.array(group[var_name][time_index, :, depth_index], dtype=float)

    map_df = pd.DataFrame({
        "lon": lons,
        "lat": lats,
        "value": values,
    })
    overlay_bounds = get_overlay_bounds_from_coords(lats, lons)
    return map_df, overlay_bounds, lats, lons


def load_bukov_data(
    data_root: Path | None,
    group_name: str = "bukov",
    schema_path: Path | None = None,
    store_url: str | None = None,
    s3_endpoint_url: str | None = None,
    mode: str = "r",
) -> BukovData:
    group = load_bukov_group(
        data_root,
        group_name=group_name,
        schema_path=schema_path,
        store_url=store_url,
        s3_endpoint_url=s3_endpoint_url,
        mode=mode,
    )
    map_df, overlay_bounds, lats_arr, lons_arr = load_bukov_map_data(group)
    depth_arr = np.array(group["depth"][:], dtype=float)
    date_time_units = group["date_time"].attrs.get("units")
    date_time_values = group["date_time"][:]
    date_time_index = to_datetime_index(date_time_values, units=date_time_units)

    return BukovData(
        group=group,
        map_df=map_df,
        overlay_bounds=overlay_bounds,
        lats=lats_arr,
        lons=lons_arr,
        depth_arr=depth_arr,
        date_time_index=date_time_index,
    )


def load_data(source: str, **kwargs) -> BukovData:
    if source == "local":
        return load_bukov_data(**kwargs)

    if source == "s3":
        endpoints_path = kwargs.pop("endpoints_path")
        endpoint_name = kwargs.pop("endpoint_name", None)
        endpoint = get_endpoint_config(endpoints_path, endpoint_name)
        schema_path = Path(endpoints_path).parent / endpoint.schema_file
        return load_bukov_data(
            data_root=None,
            schema_path=schema_path,
            store_url=endpoint.store_url,
            mode=kwargs.pop("mode", "r"),
            **kwargs,
        )

    raise NotImplementedError(f"Unknown data source '{source}'")
