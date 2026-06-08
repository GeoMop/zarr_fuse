import logging
import math
import os
import time

import cartopy.crs as ccrs
import geoviews as gv
import holoviews as hv
from holoviews.streams import RangeXY
import numpy as np
import pandas as pd

from geoviews import tile_sources as gvts

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


def _cluster_points(x_range, y_range, df, lon_field, lat_field, entity_field, eps_factor=0.05, buffer_factor=0.1):
    """
    Cluster borehole points based on current view extent.
    Returns a DataFrame with lon, lat, merged_count, label columns.
    """
    if x_range is None or y_range is None or len(df) == 0:
        return pd.DataFrame({
            lon_field: df[lon_field] if len(df) > 0 else [],
            lat_field: df[lat_field] if len(df) > 0 else [],
            "merged_count": [1] * len(df) if len(df) > 0 else [],
            "label": df[entity_field] if len(df) > 0 else [],
        })

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
        return pd.DataFrame({lon_field: [], lat_field: [], "merged_count": [], "label": []})

    # Grid-based clustering: round coordinates to eps grid
    visible_df["grid_lon"] = (visible_df[lon_field] / eps).round() * eps
    visible_df["grid_lat"] = (visible_df[lat_field] / eps).round() * eps

    # Group by grid cell
    grouped = visible_df.groupby(["grid_lon", "grid_lat"])
    clustered_rows = []
    for (glon, glat), group in grouped:
        merged_count = len(group)
        # Use centroid of the group for the clustered point
        center_lon = group[lon_field].mean()
        center_lat = group[lat_field].mean()
        # Use first entity label as representative
        label = group[entity_field].iloc[0] if entity_field in group.columns else ""
        clustered_rows.append({
            lon_field: center_lon,
            lat_field: center_lat,
            "merged_count": merged_count,
            "label": label,
        })

    return pd.DataFrame(clustered_rows)


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
    schema_config = endpoint_config["schema"]
    schema_display = endpoint_config["schema_display"]

    lat_field = "lat"
    lon_field = "lon"
    entity_field = schema_display.get("entity_name") or "entity"

    if data.group_path:
        schema_dict = endpoint_config["schema"]
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

    overlay_layer = _load_overlay(endpoint_config)

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
                return gv.Points(
                    empty_clustered, kdims=[lon_f, lat_f], vdims=["label", "merged_count"], crs=ccrs.PlateCarree()
                ).opts(
                    color="navy", size=config["point_size"],
                    line_color="white", line_width=1.5,
                    tools=["hover"], responsive=True, title=title,
                )
            if cluster_enabled:
                clustered = _cluster_points(x_range, y_range, df, lon_f, lat_f, ent_f, eps_factor, buffer_factor)
            else:
                # No clustering - pass through all points
                clustered = df.assign(merged_count=1, label=df[ent_f] if ent_f in df.columns else "")
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
    if not default_display_variable:
        fig = {"status": "error", "reason": "No variable selected"}
    else:
        fig = data.client.get_map_data(
            data.endpoint_name,
            group_path=data.group_path,
            variable=default_display_variable,
            time_index=0,
            depth_index=0,
        )
    total_points = 0  # Initialize for logging
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
        print(f"[map] Total points: 0 (error: {reason})")
    else:
        # Print raw values BEFORE numpy conversion to see what's really in the data
        raw_lats = fig["lat"]
        raw_lons = fig["lon"]
        raw_values = fig["values"]
        marker_meta = fig.get("marker_meta") or []
        print(f"[map] Raw data sample - lat[0]: {repr(raw_lats[0])}, lon[0]: {repr(raw_lons[0])}, value[0]: {repr(raw_values[0])}")
        
        lats = np.array(raw_lats, dtype=float)
        lons = np.array(raw_lons, dtype=float)
        values = np.array(raw_values, dtype=float)
        entities = fig.get("entities")
        map_title = map_config["title"]
        data_error_reason = None
        total_points = len(lats)
        finite_selected_values = int(np.isfinite(values).sum())
        missing_selected_values = int(np.sum(~np.isfinite(values)))
        displayed_markers = int(np.sum(np.isfinite(lats) & np.isfinite(lons)))
        print(f"[map] Total entities: {total_points}")
        print(f"[map] Valid coordinate markers: {displayed_markers}")
        print(f"[map] Finite selected values: {finite_selected_values}")
        print(f"[map] Missing selected values: {missing_selected_values}")
        print(f"[map] Displayed markers: {displayed_markers}")
        print(f"[map] All points (including NaN values, invalid coordinates excluded later):")
        for i in range(total_points):
            lat_valid = "OK" if np.isfinite(lats[i]) else "INVALID"
            lon_valid = "OK" if np.isfinite(lons[i]) else "INVALID"
            val_valid = "OK" if np.isfinite(values[i]) else "INVALID/NaN"
            entity = entities[i] if entities is not None and i < len(entities) else "N/A"
            marker_info = marker_meta[i] if i < len(marker_meta) else {}
            # Show raw values from original data and converted numpy values
            print(f"  [{i}] raw_lat={repr(raw_lats[i])}, raw_lon={repr(raw_lons[i])}, lat={repr(lats[i])}, lon={repr(lons[i])} ({lat_valid}, {lon_valid}) raw_value={repr(raw_values[i])}, value={repr(values[i])} ({val_valid}) entity={entity} marker_meta={marker_info}")

    valid_mask = np.isfinite(lats) & np.isfinite(lons)
    if not np.all(valid_mask):
        lats = lats[valid_mask]
        lons = lons[valid_mask]
        values = values[valid_mask]
        if entities is not None:
            entities = np.array(entities)[valid_mask]
        if marker_meta:
            marker_meta = [marker_meta[i] for i, keep in enumerate(valid_mask) if keep]
        print(f"[map] Points after lat/lon filter: {len(lats)} (removed {total_points - len(lats)})")
    else:
        print(f"[map] All {len(lats)} points have valid lat/lon - displaying all")

    if len(lats) == 0:
        reason = fig.get("reason", "No valid map points available") if fig.get("status") == "error" else "No valid map points available"
        logger.warning("Map data unavailable for group '%s': %s", data.group_path, reason)
        print(f"[map] Final points to display: 0")
        map_state = {
            "lats": lats,
            "lons": lons,
            "marker_meta": marker_meta,
            "center_lat": map_config["center_lat"],
            "center_lon": map_config["center_lon"],
            "zoom": int(map_config["zoom"]),
            "variable": default_display_variable,
            "data_error_reason": reason,
        }
        empty_df = pd.DataFrame({lon_field: [], lat_field: [], entity_field: [], "value": []})
        data.current_map_df = empty_df  # Store empty df for refresh access

        # Step 4: Use DynamicMap even for empty case
        points_callback = _make_points_callback(data, map_config, lon_field, lat_field, entity_field, f"{map_config['title']} - {reason}")
        map_points_dmap = hv.DynamicMap(points_callback, streams=[RangeXY()])
        tap_stream.source = map_points_dmap

        center_lat = map_config["center_lat"]
        center_lon = map_config["center_lon"]
        zoom = int(map_config["zoom"])
        center_x, center_y = _lonlat_to_web_mercator(center_lon, center_lat)
        span_m = _zoom_to_span_meters(zoom)
        half_span = span_m / 2.0
        xlim = (center_x - half_span, center_x + half_span)
        ylim = (center_y - half_span, center_y + half_span)

        map_view = (base_map * map_points_dmap).opts(
            xlim=xlim,
            ylim=ylim,
            framewise=True,
            axiswise=True,
        )
        result = map_view, map_state
        print(f"[timing] build_map_view: {time.perf_counter() - start:.3f}s")
        return result

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
    data.current_map_df = map_df  # Store latest map_df on data object for refresh access

    points_callback = _make_points_callback(data, map_config, lon_field, lat_field, entity_field, map_title)
    map_points_dmap = hv.DynamicMap(points_callback, streams=[RangeXY()])

    tap_stream.source = map_points_dmap

    print(f"[map] Final points to display: {len(lats)}")

    # Selection marker that highlights the tapped point
    def _selection_marker(x, y):
        if x is None or y is None:
            return hv.Points([])
        mx, my = _lonlat_to_web_mercator(x, y)
        return hv.Points([(mx, my)]).opts(
            color="#ff0000",
            size=20,
            marker="circle",
            line_color="white",
            line_width=3,
            fill_alpha=0.5,
            tools=[],
        )

    selection_marker_dmap = hv.DynamicMap(_selection_marker, streams=[tap_stream])

    center_lat = map_config["center_lat"]
    center_lon = map_config["center_lon"]
    zoom = int(map_config["zoom"])
    center_x, center_y = _lonlat_to_web_mercator(center_lon, center_lat)
    span_m = _zoom_to_span_meters(zoom)
    half_span = span_m / 2.0
    xlim = (center_x - half_span, center_x + half_span)
    ylim = (center_y - half_span, center_y + half_span)

    map_state = {
        "lats": lats,
        "lons": lons,
        "marker_meta": marker_meta,
        "center_lat": center_lat,
        "center_lon": center_lon,
        "zoom": zoom,
        "variable": default_display_variable,
        "data_error_reason": data_error_reason,
    }

    if overlay_layer is not None:
        map_view = (base_map * overlay_layer * map_points_dmap).opts(
            xlim=xlim,
            ylim=ylim,
            framewise=True,
            axiswise=True,
        )
        result = map_view, map_state
        print(f"[timing] build_map_view: {time.perf_counter() - start:.3f}s")
        return result

    map_view = (base_map * map_points_dmap).opts(
        xlim=xlim,
        ylim=ylim,
        framewise=True,
        axiswise=True,
    )
    result = map_view, map_state
    print(f"[timing] build_map_view: {time.perf_counter() - start:.3f}s")
    return result