import geoviews as gv
import numpy as np
import pandas as pd

from geoviews import tile_sources as gvts


def _overlay_bounds_from_coords(lats: np.ndarray, lons: np.ndarray, pad_ratio: float = 0.05):
    lat_min, lat_max = np.nanmin(lats), np.nanmax(lats)
    lon_min, lon_max = np.nanmin(lons), np.nanmax(lons)
    lat_pad = (lat_max - lat_min) * pad_ratio if lat_max > lat_min else 0.01
    lon_pad = (lon_max - lon_min) * pad_ratio if lon_max > lon_min else 0.01
    return (lon_min - lon_pad, lat_min - lat_pad, lon_max + lon_pad, lat_max + lat_pad)


def build_map_view(data, tap_stream):
    base_map = gvts.OSM()
    fig = data.client.get_map_data(
        data.endpoint_name,
        group_path=data.group_path,
        variable="rock_temp",
        time_index=0,
        depth_index=0,
    )
    if fig.get("status") == "error":
        raise ValueError(fig.get("reason", "Failed to load map data"))

    lats = np.array(fig.get("lat", []), dtype=float)
    lons = np.array(fig.get("lon", []), dtype=float)
    values = np.array(fig.get("values", []), dtype=float)
    map_df = pd.DataFrame({"lon": lons, "lat": lats, "value": values})
    if len(lats) == 0 or len(lons) == 0:
        overlay_bounds = (-1.0, -1.0, 1.0, 1.0)
    else:
        overlay_bounds = _overlay_bounds_from_coords(lats, lons)

    overlay = gv.Rectangles([overlay_bounds]).opts(
        alpha=0.2,
        color="orange",
        line_width=2,
        line_color="red",
    )

    map_points = gv.Points(map_df, kdims=["lon", "lat"], vdims=["value"]).opts(
        color="value",
        cmap="viridis",
        size=10,
        alpha=0.8,
        line_color="white",
        line_width=1.5,
        tools=["hover", "tap"],
        colorbar=True,
        responsive=True,
        title="Geographic Data View",
    )

    tap_stream.source = map_points
    map_state = {"lats": lats, "lons": lons}
    return base_map * overlay * map_points, map_state
