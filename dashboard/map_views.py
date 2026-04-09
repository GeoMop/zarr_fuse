import logging
import os
import time

import cartopy.crs as ccrs
import geoviews as gv
import numpy as np
import pandas as pd

from geoviews import tile_sources as gvts

logger = logging.getLogger(__name__)


def _load_overlay(endpoint_config):
    if os.getenv("HV_OVERLAY_ENABLED", "1").strip().lower() in {"0", "false", "no"}:
        logger.info("Overlay disabled via HV_OVERLAY_ENABLED.")
        return None

    visualization_config = endpoint_config.get("visualization", {}) or {}
    overlay_config = visualization_config.get("overlay", {}) or {}

    if not overlay_config.get("enabled", False):
        logger.info("Overlay disabled in endpoint config.")
        return None

    tile_url = overlay_config.get("tile_url") or os.getenv("HV_OVERLAY_TILE_URL", "").strip()
    if not tile_url:
        logger.info("No overlay tile URL configured.")
        return None

    logger.info("Using tiled overlay: %s", tile_url)
    return gv.WMTS(tile_url).opts(alpha=0.9)


def build_map_view(data, tap_stream):
    start = time.perf_counter()
    base_map = gvts.OSM()

    endpoint_config = data.client.get_endpoint(data.endpoint_name)
    defaults_config = endpoint_config.get("defaults", {}) or {}
    visualization_config = endpoint_config.get("visualization", {}) or {}
    map_config = visualization_config.get("map", {}) or {}

    overlay_layer = _load_overlay(endpoint_config)

    default_display_variable = defaults_config.get("display_variable")
    if not default_display_variable:
        raise ValueError(f"No default display variable configured for endpoint '{data.endpoint_name}'")

    fig = data.client.get_map_data(
        data.endpoint_name,
        group_path=data.group_path,
        variable=default_display_variable,
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
        cmap=map_config.get("cmap", "viridis"),
        size=map_config.get("point_size", 10),
        alpha=map_config.get("alpha", 0.8),
        line_color="white",
        line_width=1.5,
        tools=["hover", "tap"],
        colorbar=True,
        responsive=True,
        title=map_config.get("title", "Geographic Data View"),
    )

    tap_stream.source = map_points
    map_state = {
        "lats": lats,
        "lons": lons,
        "center_lat": map_config.get("center_lat"),
        "center_lon": map_config.get("center_lon"),
        "zoom": map_config.get("zoom"),
    }

    if overlay_layer is not None:
        result = base_map * overlay_layer * map_points, map_state
        print(f"[timing] build_map_view: {time.perf_counter() - start:.3f}s")
        return result

    result = base_map * map_points, map_state
    print(f"[timing] build_map_view: {time.perf_counter() - start:.3f}s")
    return result