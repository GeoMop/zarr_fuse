import json
import logging
import os
from pathlib import Path

import cartopy.crs as ccrs
import geoviews as gv
import numpy as np
import pandas as pd
from PIL import Image

from geoviews import tile_sources as gvts

logger = logging.getLogger(__name__)


def _load_bukov_overlay():
    if os.getenv("HV_OVERLAY_ENABLED", "1").strip().lower() in {"0", "false", "no"}:
        logger.info("Overlay disabled via HV_OVERLAY_ENABLED.")
        return None

    base_dir = Path(__file__).resolve().parent / "config" / "bukov_endpoint"
    georef_path = Path(os.getenv("HV_OVERLAY_GEOREF", base_dir / "bukov_georef.json"))
    image_path = Path(os.getenv("HV_OVERLAY_IMAGE", base_dir / "12p_final.png"))

    if not georef_path.is_file() or not image_path.is_file():
        logger.info("Overlay files not found: %s , %s", georef_path, image_path)
        return None

    try:
        with georef_path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)

        image = Image.open(image_path).convert("RGBA")
        width, height = image.size
        orig_width, orig_height = width, height

        max_pixels = int(os.getenv("HV_OVERLAY_MAX_PIXELS", "25000000"))
        pixels = width * height
        if pixels > max_pixels:
            scale = (max_pixels / float(pixels)) ** 0.5
            new_width = max(1, int(width * scale))
            new_height = max(1, int(height * scale))
            logger.warning(
                "Overlay image is large (%s px). Downscaling to %sx%s.",
                pixels,
                new_width,
                new_height,
            )
            image = image.resize((new_width, new_height), Image.LANCZOS)
            width, height = image.size

        src_pts = []
        dst_pts = []
        for pt in data.get("points", []):
            if pt.get("enable", True):
                src_pts.append([pt["sourceX"], abs(pt["sourceY"])])
                dst_pts.append([pt["mapX"], pt["mapY"]])

        if len(src_pts) < 3:
            logger.warning("Overlay georef does not contain enough points.")
            return None

        src = np.array(src_pts, dtype=float)
        dst = np.array(dst_pts, dtype=float)
        A = np.c_[src, np.ones(len(src))]
        transform_matrix, _, _, _ = np.linalg.lstsq(A, dst, rcond=None)

        corners_px = np.array(
            [
                [0, 0, 1],
                [orig_width, 0, 1],
                [orig_width, orig_height, 1],
                [0, orig_height, 1],
            ]
        )
        corners_geo = corners_px @ transform_matrix
        lons = corners_geo[:, 0]
        lats = corners_geo[:, 1]

        valid = np.isfinite(lons) & np.isfinite(lats)
        if not np.any(valid):
            logger.warning("Overlay georef produced invalid bounds.")
            return None

        bounds = (
            float(np.nanmin(lons[valid])),
            float(np.nanmin(lats[valid])),
            float(np.nanmax(lons[valid])),
            float(np.nanmax(lats[valid])),
        )

        return gv.RGB(np.asarray(image), bounds=bounds, crs=ccrs.PlateCarree()).opts(alpha=0.9)
    except Exception:
        logger.exception("Failed to load overlay image.")
        return None
def build_map_view(data, tap_stream):
    base_map = gvts.OSM()
    overlay_image = _load_bukov_overlay() if data.endpoint_name == "bukov_endpoint" else None
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
    if overlay_image is not None:
        return base_map * overlay_image * map_points, map_state
    return base_map * map_points, map_state
