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

    visualization_config = endpoint_config["visualization"]
    overlay_config = visualization_config["overlay"]

    if not overlay_config["enabled"]:
        logger.info("Overlay disabled in endpoint config.")
        return None

    tile_url = overlay_config["tile_url"] or os.getenv("HV_OVERLAY_TILE_URL", "").strip()
    if not tile_url:
        logger.info("No overlay tile URL configured.")
        return None

    logger.info("Using tiled overlay: %s", tile_url)
    return gv.WMTS(tile_url).opts(alpha=0.9)


def build_map_view(data, tap_stream):
    start = time.perf_counter()
    base_map = gvts.OSM()

    endpoint_config = data.client.get_endpoint(data.endpoint_name)
    defaults_config = endpoint_config["defaults"]
    visualization_config = endpoint_config["visualization"]
    map_config = visualization_config["map"]

    overlay_layer = _load_overlay(endpoint_config)

    default_display_variable = defaults_config["display_variable"]
    if not default_display_variable:
        raise ValueError(f"No default display variable configured for endpoint '{data.endpoint_name}'")

    fig = data.client.get_map_data(
        data.endpoint_name,
        group_path=data.group_path,
        variable=default_display_variable,
        time_index=0,
        depth_index=0,
    )
    if fig["status"] == "error":
        raise ValueError(fig["reason"])

    lats = np.array(fig["lat"], dtype=float)
    lons = np.array(fig["lon"], dtype=float)
    values = np.array(fig["values"], dtype=float)

    map_df = pd.DataFrame({"lon": lons, "lat": lats, "value": values})
    map_points = gv.Points(
        map_df, kdims=["lon", "lat"], vdims=["value"], crs=ccrs.PlateCarree()
    ).opts(
        color="value",
        cmap=map_config["cmap"],
        size=map_config["point_size"],
        alpha=map_config["alpha"],
        line_color="white",
        line_width=1.5,
        tools=["hover", "tap"],
        colorbar=True,
        responsive=True,
        title=map_config["title"],
    )

    tap_stream.source = map_points
    map_state = {
        "lats": lats,
        "lons": lons,
        "center_lat": map_config["center_lat"],
        "center_lon": map_config["center_lon"],
        "zoom": map_config["zoom"],
    }

    if overlay_layer is not None:
        result = base_map * overlay_layer * map_points, map_state
        print(f"[timing] build_map_view: {time.perf_counter() - start:.3f}s")
        return result

    result = base_map * map_points, map_state
    print(f"[timing] build_map_view: {time.perf_counter() - start:.3f}s")
    return result