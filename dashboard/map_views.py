import logging
import math
import os
from urllib.parse import urlencode

import cartopy.crs as ccrs
import geoviews as gv
import holoviews as hv
import numpy as np
import pandas as pd
from geoviews import tile_sources as gvts
from holoviews.streams import RangeXY

from dashboard.config import find_view_file, overlay_enabled
from dashboard.profiling import time_stage

try:
    _vpath = find_view_file()
except FileNotFoundError:
    _vpath = None

logger = logging.getLogger(__name__)


def _lonlat_to_web_mercator(lon: float, lat: float) -> tuple[float, float]:
    # Clamp latitude to Web Mercator's valid range.
    lat = max(min(lat, 85.05112878), -85.05112878)
    r = 6378137.0
    x = r * math.radians(lon)
    y = r * math.log(math.tan(math.pi / 4.0 + math.radians(lat) / 2.0))
    return x, y


def _zoom_to_span_meters(zoom: int) -> float:
    world_width_m = 40075016.68557849
    return world_width_m / (2 ** zoom)


def _data_bounds(lats: np.ndarray, lons: np.ndarray, padding: float = 0.05):
    """Return ``(min_lat, max_lat, min_lon, max_lon)`` of all points with padding.

    Returns ``None`` when there are no valid points.
    """
    if len(lats) == 0:
        return None
    min_lat, max_lat = float(lats.min()), float(lats.max())
    min_lon, max_lon = float(lons.min()), float(lons.max())
    r_lat = max_lat - min_lat or 0.01
    r_lon = max_lon - min_lon or 0.01
    return (
        min_lat - r_lat * padding,
        max_lat + r_lat * padding,
        min_lon - r_lon * padding,
        max_lon + r_lon * padding,
    )


WORLD_XLIM = (-20037508.34, 20037508.34)
WORLD_YLIM = (-20037508.34, 20037508.34)


def _cluster_points(x_range, y_range, df, lon_field, lat_field, entity_field, eps_factor=0.05, buffer_factor=0.1):
    """
    Cluster borehole points based on current view extent.

    Returns ``(clustered_df, member_lists, radii)`` where ``clustered_df`` has
    ``lon, lat, merged_count, label`` columns, ``member_lists[i]`` holds the
    entity indices (or positional row indices when no ``entity_index`` column
    exists) that make up cluster row ``i``, and ``radii[i]`` is the max
    centroid-to-member distance in degrees (0.0 for single-member/empty).
    """
    def _members(subset):
        if "entity_index" in subset.columns:
            return [int(v) for v in subset["entity_index"]]
        return list(subset.index)

    if x_range is None or y_range is None or len(df) == 0:
        clustered = pd.DataFrame({
            lon_field: df[lon_field] if len(df) > 0 else [],
            lat_field: df[lat_field] if len(df) > 0 else [],
            "merged_count": [1] * len(df) if len(df) > 0 else [],
            "label": df[entity_field] if len(df) > 0 else [],
        })
        members = [_members(df.iloc[[i]]) for i in range(len(df))]
        return clustered, members, [0.0] * len(df)

    # Calculate cluster distance (eps) as eps_factor of view width in degrees
    view_width = x_range[1] - x_range[0]
    eps = view_width * eps_factor

    # Filter to points within or near current view (with buffer)
    buffer = view_width * buffer_factor
    mask = (
        (df[lon_field] >= x_range[0] - buffer) & (df[lon_field] <= x_range[1] + buffer) &
        (df[lat_field] >= y_range[0] - buffer) & (df[lat_field] <= y_range[1] + buffer)
    )
    visible_df = df[mask].copy()
    if len(visible_df) == 0:
        return pd.DataFrame({lon_field: [], lat_field: [], "merged_count": [], "label": []}), [], []

    # Grid-based clustering: round coordinates to eps grid
    visible_df["grid_lon"] = (visible_df[lon_field] / eps).round() * eps
    visible_df["grid_lat"] = (visible_df[lat_field] / eps).round() * eps

    # Group by grid cell
    grouped = visible_df.groupby(["grid_lon", "grid_lat"])
    clustered_rows = []
    member_lists = []
    radii = []
    for (glon, glat), group in grouped:
        merged_count = len(group)
        # Use centroid of the group for the clustered point
        center_lon = group[lon_field].mean()
        center_lat = group[lat_field].mean()
        # Max centroid-to-member distance (degrees)
        radius = float(np.max(
            np.sqrt((group[lon_field] - center_lon) ** 2 + (group[lat_field] - center_lat) ** 2)
        )) if len(group) > 0 else 0.0
        # Use first entity label as representative
        label = group[entity_field].iloc[0] if entity_field in group.columns else ""
        clustered_rows.append({
            lon_field: center_lon,
            lat_field: center_lat,
            "merged_count": merged_count,
            "label": label,
        })
        member_lists.append(_members(group))
        radii.append(radius)

    return pd.DataFrame(clustered_rows), member_lists, radii


def _load_overlay(view_config):
    if not overlay_enabled(_vpath, view_config.get("name")):
        logger.info("Overlay disabled via config or HV_OVERLAY_ENABLED=0.")
        return None

    visualization_config = view_config["visualization"]
    overlay_config = visualization_config["overlay"]

    tile_url = overlay_config["tile_url"] or os.getenv("HV_OVERLAY_TILE_URL", "").strip()
    if not tile_url:
        logger.info("No overlay tile URL configured.")
        return None

    if tile_url.startswith("/tiles/") and view_config.get("name"):
        separator = "&" if "?" in tile_url else "?"
        tile_url = f"{tile_url}{separator}{urlencode({'view': view_config['name']})}"

    logger.info("Using tiled overlay: %s", tile_url)
    return gv.WMTS(tile_url).opts(alpha=0.9)


def fetch_map_payload(client, view_name: str, group_path: str, variable: str) -> dict:
    """Read the map slice without constructing or updating any UI objects."""
    if not variable:
        return {"status": "error", "reason": "No variable selected"}
    return client.get_map_data(
        view_name, group_path=group_path, variable=variable, time_index=0, depth_index=0,
    )


def build_map_view(data, tap_stream, *, map_payload: dict | None = None):
    """Build the map on the document thread from a supplied or freshly read slice."""
    _stage = time_stage("build_map_view")
    base_map = gvts.OSM()

    view_config = data.client.get_view(data.view_name)
    defaults_config = view_config["defaults"]
    visualization_config = view_config["visualization"]
    map_config = visualization_config["map"]
    schema_config = view_config["schema"]
    schema_display = view_config["schema_display"]

    lat_field = "lat"
    lon_field = "lon"
    entity_field = schema_display.get("entity_name") or "entity"

    if data.group_path:
        schema_dict = view_config["schema"]
        group_fields = schema_dict.get("group_fields", {})
        normalized = "/".join(part for part in (data.group_path or "").strip("/").split("/") if part)
        path = normalized
        fields = None
        while True:
            if path in group_fields:
                fields = group_fields[path]
                break
            if not path:
                break
            path = path.rsplit("/", 1)[0] if "/" in path else ""
        if fields is None:
            fields = schema_dict.get("fields", {})
        if fields:
            if fields.get("lat"):
                lat_field = fields.get("lat")
            if fields.get("lon"):
                lon_field = fields.get("lon")

    overlay_layer = _load_overlay(view_config)

    # Step 4: DynamicMap callback - defined early so it's available in all code paths
    def _make_points_callback(data_obj, config, lon_f, lat_f, ent_f, title):
        # Read clustering config
        cluster_enabled = config.get("cluster_enabled", True)
        # Ensure proper boolean (handle both YAML bool and potential string)
        if isinstance(cluster_enabled, str):
            cluster_enabled = cluster_enabled.lower() in ("true", "1", "yes")
        eps_factor = config.get("cluster_eps_factor",0.05)
        buffer_factor = config.get("cluster_buffer_factor", 0.1)
        cluster_size_scale = config.get("cluster_size_scale", 3.0)

        def callback(x_range, y_range):
            # Always read the latest map DataFrame from the data object
            df = getattr(data_obj, 'current_map_df', pd.DataFrame())
            if len(df) == 0:
                empty_clustered = pd.DataFrame({lon_f: [], lat_f: [], "merged_count": [], "label": []})
                data_obj.current_clusters = []
                return gv.Points(
                    empty_clustered, kdims=[lon_f, lat_f], vdims=["label", "merged_count"], crs=ccrs.PlateCarree()
                ).opts(
                    color="navy", size=config["point_size"],
                    line_color="white", line_width=1.5,
                    tools=["hover"], responsive=True, title=title,
                )
            if cluster_enabled:
                clustered, members, radii = _cluster_points(
                    x_range, y_range, df, lon_f, lat_f, ent_f, eps_factor, buffer_factor
                )
            else:
                # No clustering - pass through all points
                clustered = df.assign(merged_count=1, label=df[ent_f] if ent_f in df.columns else "")
                if "entity_index" in df.columns:
                    members = [[int(v)] for v in df["entity_index"]]
                else:
                    members = [[i] for i in range(len(df))]
                radii = [0.0] * len(df)
            data_obj.current_clusters = [
                {"lon": float(row[lon_f]), "lat": float(row[lat_f]),
                 "entity_indices": members[i], "radius": radii[i]}
                for i, row in clustered.iterrows()
            ]
            if len(clustered) == 0:
                empty_clustered = pd.DataFrame({lon_f: [], lat_f: [], "merged_count": [], "label": []})
                return gv.Points(
                    empty_clustered, kdims=[lon_f, lat_f], vdims=["label", "merged_count"], crs=ccrs.PlateCarree()
                ).opts(
                    color="navy", size=config["point_size"],
                    line_color="white", line_width=1.5,
                    tools=["hover"], responsive=True, title=title,
                )
            return gv.Points(
                clustered, kdims=[lon_f, lat_f], vdims=["label", "merged_count"], crs=ccrs.PlateCarree()
            ).opts(
                color="navy",
                size=hv.dim("merged_count") * cluster_size_scale + config["point_size"],
                line_color="white",
                line_width=1.5,
                tools=["hover", "tap"],
                hover_tooltips=[
                    ("Label", "@{label}"),
                    ("Merged Count", "@{merged_count}"),
                    (lat_f, f"@{{{lat_f}}}"),
                    (lon_f, f"@{{{lon_f}}}"),
                ],
                nonselection_alpha=1.0,
                selection_color="#ff0000",
                selection_line_color="white",
                selection_line_width=3,
                responsive=True,
                title=title,
            )
        return callback

    default_display_variable = data.display_variable
    fig = map_payload
    if fig is None:
        fig = fetch_map_payload(data.client, data.view_name, data.group_path, default_display_variable)

    if fig.get("status") == "error":
        reason = fig.get("reason", "No map data available")
        logger.warning("Map data unavailable for group '%s': %s", data.group_path, reason)
        lats = np.array([], dtype=float)
        lons = np.array([], dtype=float)
        values = np.array([], dtype=float)
        marker_meta = []
        entities = None
        map_title = f"{map_config['title']} - {reason}"
        data_error_reason = reason
    else:
        marker_meta = fig.get("marker_meta") or []
        lats = np.array(fig["lat"], dtype=float)
        lons = np.array(fig["lon"], dtype=float)
        values = np.array(fig["values"], dtype=float)
        entities = fig.get("entities")
        map_title = map_config["title"]
        data_error_reason = None

    valid_mask = np.isfinite(lats) & np.isfinite(lons)
    if not np.all(valid_mask):
        lats = lats[valid_mask]
        lons = lons[valid_mask]
        values = values[valid_mask]
        if entities is not None:
            entities = np.array(entities)[valid_mask]
        if marker_meta:
            marker_meta = [marker_meta[i] for i, keep in enumerate(valid_mask) if keep]

    if len(lats) == 0:
        reason = data_error_reason or "No valid map points available"
        logger.warning("Map data unavailable for group '%s': %s", data.group_path, reason)
        data_error_reason = reason
        map_state = {
            "lats": lats,
            "lons": lons,
            "marker_meta": marker_meta,
            "variable": default_display_variable,
            "data_error_reason": reason,
        }
        data.current_map_df = pd.DataFrame({
            lon_field: [], lat_field: [], entity_field: [], "value": [],
            "has_value": pd.Series(dtype=bool),
        })
        points_callback = _make_points_callback(
            data, map_config, lon_field, lat_field, entity_field, f"{map_config['title']} - {reason}"
        )
        map_points_dmap = hv.DynamicMap(points_callback, streams=[RangeXY()])
        tap_stream.source = map_points_dmap
        xlim, ylim = WORLD_XLIM, WORLD_YLIM
    else:
        map_df = pd.DataFrame({
            lon_field: lons,
            lat_field: lats,
            entity_field: entities if entities is not None else [""] * len(lons),
            "value": values,
            "has_value": np.isfinite(values),
        })
        if marker_meta:
            map_df["site_id"] = [m.get("site_id") for m in marker_meta]
            map_df["entity_index"] = [m.get("entity_index") for m in marker_meta]
            map_df["marker_has_value"] = [m.get("has_value") for m in marker_meta]
            map_df["marker_value"] = [m.get("value") for m in marker_meta]
        data.current_map_df = map_df
        points_callback = _make_points_callback(data, map_config, lon_field, lat_field, entity_field, map_title)
        map_points_dmap = hv.DynamicMap(points_callback, streams=[RangeXY()])
        tap_stream.source = map_points_dmap

        bounds = _data_bounds(lats, lons)
        if bounds is None:
            xlim, ylim = WORLD_XLIM, WORLD_YLIM
        else:
            min_lat, max_lat, min_lon, max_lon = bounds
            min_x, max_y = _lonlat_to_web_mercator(min_lon, max_lat)
            max_x, min_y = _lonlat_to_web_mercator(max_lon, min_lat)
            xlim, ylim = (min_x, max_x), (min_y, max_y)
        map_state = {
            "lats": lats,
            "lons": lons,
            "marker_meta": marker_meta,
            "variable": default_display_variable,
            "data_error_reason": data_error_reason,
        }

    layers = base_map * map_points_dmap
    if overlay_layer is not None:
        layers = base_map * overlay_layer * map_points_dmap
    map_view = layers.opts(xlim=xlim, ylim=ylim, framewise=True, axiswise=True)
    _stage.log()
    return map_view, map_state
