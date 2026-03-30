import logging
import os
import time

import cartopy.crs as ccrs
import geoviews as gv 
import numpy as np
import pandas as pd

from geoviews import tile_sources as gvts

logger = logging.getLogger(__name__)


def _load_bukov_overlay():
    if os.getenv("HV_OVERLAY_ENABLED", "1").strip().lower() in {"0", "false", "no"}:
        logger.info("Overlay disabled via HV_OVERLAY_ENABLED.")
        return None

    tile_url = os.getenv("HV_OVERLAY_TILE_URL", "/tiles/{Z}/{X}/{Y}.png").strip()
    if not tile_url:
        logger.info("No overlay tile URL configured.")
        return None

    logger.info("Using tiled overlay: %s", tile_url)
    return gv.WMTS(tile_url).opts(alpha=0.9)


def build_map_view(data, tap_stream):
    start = time.perf_counter()
    base_map = gvts.OSM()
    overlay_layer = _load_bukov_overlay() if data.endpoint_name == "bukov_endpoint" else None

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
    map_points = gv.Points(
        map_df, kdims=["lon", "lat"], vdims=["value"], crs=ccrs.PlateCarree()
    ).opts(
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

    if overlay_layer is not None:
        result = base_map * overlay_layer * map_points, map_state
        print(f"[timing] build_map_view: {time.perf_counter() - start:.3f}s")
        return result

    result = base_map * map_points, map_state
    print(f"[timing] build_map_view: {time.perf_counter() - start:.3f}s")
    return result