import holoviews as hv
import numpy as np
import pandas as pd
import time

from holoviews import streams


def _resolve_fields_for_group(schema_config, group_path):
    fields = schema_config.get("fields", {})
    group_fields = schema_config.get("group_fields", {})
    normalized = "/".join(part for part in (group_path or "").strip("/").split("/") if part)

    path = normalized
    while True:
        if path in group_fields:
            return group_fields[path]
        if not path:
            break
        path = path.rsplit("/", 1)[0] if "/" in path else ""

    return fields


def build_timeseries_views(data, depth_selector, borehole_info, borehole_stream, map_state):
    start_total = time.perf_counter()
    endpoint_config = data.client.get_endpoint(data.endpoint_name)
    defaults_config = endpoint_config["defaults"]
    schema_display = endpoint_config["schema_display"]
    schema_config = endpoint_config["schema"]
    fields_config = _resolve_fields_for_group(schema_config, data.group_path)
    time_dim = fields_config.get("time") or "time"
    visualization_config = endpoint_config["visualization"]
    timeseries_config = visualization_config["timeseries"]

    default_display_variable = data.display_variable

    metric_label = (
        default_display_variable
        or schema_display.get("display_variable")
        or "value"
    )
    display_unit = schema_display.get("display_unit")
    y_axis_label = f"{metric_label} ({display_unit})" if display_unit else metric_label
    entity_label = (
        schema_display.get("entity_name")
        or fields_config.get("entity")
        or "entity"
    )
    middle_window_days = timeseries_config["middle_window_days"]
    right_window_hours = timeseries_config["right_window_hours"]

    timeseries_state = {
        "times": pd.to_datetime([]),
        "depths": np.array([]),
        "series": [],
        "entity_index": 0,
        "entity_display_name": None,
        "selected_lat": None,
        "selected_lon": None,
        "selected_marker_has_value": True,
    }

    def format_depth(depth_value: float):
        if np.isnan(depth_value):
            return "NaN"
        return f"{depth_value:.2f}"

    def _default_coords():
        lats = map_state.get("lats")
        lons = map_state.get("lons")
        if lats is None or lons is None:
            lats = np.array([])
            lons = np.array([])
        if len(lats) == 0 or len(lons) == 0:
            return 0.0, 0.0
        return float(lats[0]), float(lons[0])

    def _update_depth_selector(depths, series, entity_index, borehole_name=None):
        available = []
        for idx, values in enumerate(series):
            if np.any(np.isfinite(values)):
                available.append(idx)
        if not available:
            available = list(range(len(series)))

        depth_selector.options = {
            f"{format_depth(depths[i])}" if i < len(depths) else str(i): i
            for i in available
        }
        depth_selector.value = available
        display_name = borehole_name if borehole_name else entity_label
        lat = timeseries_state.get("selected_lat")
        lon = timeseries_state.get("selected_lon")
        lat_lon_text = f" ({lat:.4f}, {lon:.4f})" if lat is not None and lon is not None else ""
        if timeseries_state.get("selected_marker_has_value", True):
            borehole_info.object = f"### {display_name}{lat_lon_text}"
        else:
            borehole_info.object = (
                f"### {display_name}{lat_lon_text}\n"
                "No data available for this site at the selected time and depth."
            )
        timeseries_state["entity_display_name"] = display_name

    def _fetch_timeseries(lat, lon, marker_meta=None):
        start = time.perf_counter()
        print(f"[fetch_ts] Called with lat={lat:.4f}, lon={lon:.4f}, marker_meta={marker_meta}")
        marker_entity_index = marker_meta.get("entity_index") if isinstance(marker_meta, dict) else None
        marker_site_id = marker_meta.get("site_id") if isinstance(marker_meta, dict) else None
        selected_entity_index = marker_entity_index
        if marker_entity_index is not None:
            print(f"[fetch_ts] Selected marker index={marker_entity_index}, site_id={marker_site_id}")
        fig = data.client.get_timeseries_data(
            data.endpoint_name,
            group_path=data.group_path,
            lat=lat,
            lon=lon,
            variable=default_display_variable,
            entity_index=selected_entity_index,
        )
        if fig.get("status") == "error":
            reason = fig.get("reason", "Failed to load timeseries")
            timeseries_state["times"] = pd.to_datetime([])
            timeseries_state["depths"] = np.array([])
            timeseries_state["series"] = []
            timeseries_state["entity_index"] = 0
            timeseries_state["selected_lat"] = lat
            timeseries_state["selected_lon"] = lon
            depth_selector.options = {}
            depth_selector.value = []
            borehole_info.object = f"### No data ({reason})"
            print(f"[timing] timeseries fetch failed: {time.perf_counter() - start:.3f}s")
            return None

        times = pd.to_datetime(fig.get("times", []))
        depths = np.array(fig.get("depths", []), dtype=float)
        series = [np.array(values, dtype=float) for values in fig.get("series", [])]
        entity_index = int(fig.get("borehole_index", 0))
        borehole_name = fig.get("borehole_name")

        # Only update state if we got data for the correct entity
        timeseries_state["times"] = times
        timeseries_state["depths"] = depths
        timeseries_state["series"] = series
        timeseries_state["entity_index"] = entity_index
        timeseries_state["selected_lat"] = lat
        timeseries_state["selected_lon"] = lon
        # Determine has_value based on fetched series content (not map-slice)
        has_series_data = False
        try:
            if series:
                has_series_data = any(np.any(np.isfinite(s)) for s in series)
        except Exception:
            has_series_data = False
        timeseries_state["selected_marker_has_value"] = bool(has_series_data)
        if not has_series_data:
            print(f"[fetch_ts] Fetched site {borehole_name}: no finite data in series")
        else:
            print(f"[fetch_ts] Fetched site {borehole_name}: has finite data")
        _update_depth_selector(depths, series, entity_index, borehole_name)
        print(f"[timing] timeseries fetch+state: {time.perf_counter() - start:.3f}s")
        return entity_index

    def build_timeseries_overlay(selected_depths):
        curves = []
        times = timeseries_state["times"]
        depths = timeseries_state["depths"]
        series = timeseries_state["series"]

        for depth_idx in selected_depths:
            if depth_idx >= len(series):
                continue
            depth_val = depths[depth_idx] if depth_idx < len(depths) else depth_idx
            label = f"{format_depth(depth_val)}"
            curve_df = pd.DataFrame({
                time_dim: times,
                y_axis_label: series[depth_idx],
            })
            curves.append(hv.Curve(curve_df, time_dim, y_axis_label, label=label))

        if not curves:
            curves = [hv.Curve([])]
        return hv.Overlay(curves)

    def clamp_range(center, span):
        times = timeseries_state["times"]
        if len(times) == 0:
            return (pd.Timestamp("1970-01-01"), pd.Timestamp("1970-01-02"))
        min_t = times.min()
        max_t = times.max()
        total_span = max_t - min_t
        if span >= total_span:
            return (min_t, max_t)

        start = center - span / 2
        end = center + span / 2
        if start < min_t:
            start = min_t
            end = min_t + span
        if end > max_t:
            end = max_t
            start = max_t - span
        return (start, end)

    mid_span = pd.Timedelta(days=middle_window_days)
    right_span = pd.Timedelta(hours=right_window_hours)

    center_state = {
        "center": None,
        "force_left": True,
        "force_mid": False,
        "force_right": False,
    }
    center_stream = streams.Stream.define("Center", center=None)()

    left_tap = streams.Tap()
    mid_tap = streams.Tap()
    right_tap = streams.Tap()

    _updating_center = False

    def update_center_from_tap(event):
        nonlocal _updating_center
        if _updating_center:
            return
        if event.new is None:
            return

        center = pd.to_datetime(event.new)
        _updating_center = True
        center_state["center"] = center
        center_state["force_mid"] = True
        center_state["force_right"] = True
        center_stream.event(center=center)
        _updating_center = False

    def make_xrange_hook(xlim, force_flag_key=None):
        def _hook(plot, element):
            plot.state.x_range.bounds = xlim
            if force_flag_key and center_state.get(force_flag_key, False):
                plot.state.x_range.start = xlim[0]
                plot.state.x_range.end = xlim[1]
                center_state[force_flag_key] = False
        return _hook

    left_tap.param.watch(update_center_from_tap, ["x"])
    mid_tap.param.watch(update_center_from_tap, ["x"])
    right_tap.param.watch(update_center_from_tap, ["x"])

    def create_timeseries_view(value=None, center=None, view="left", **_):
        selected_depths = value or []
        if not selected_depths:
            selected_depths = list(range(len(timeseries_state["series"])))

        times = timeseries_state["times"]
        if len(times) == 0:
            return hv.Overlay([hv.Curve([])])

        if center_state["center"] is None:
            full_span = times.max() - times.min()
            center_state["center"] = times.min() + (full_span / 2)
            center_stream.event(center=center_state["center"])

        center_time = center or center_state["center"]
        if view == "left":
            xlim = (times.min(), times.max())
        elif view == "mid":
            xlim = clamp_range(center_time, mid_span)
        else:
            xlim = clamp_range(center_time, right_span)

        overlay = build_timeseries_overlay(selected_depths)
        overlay = overlay.redim.range(**{time_dim: xlim})
        overlay = overlay * hv.VLine(center_time).opts(color="red", line_width=2)
        if view == "left":
            hooks = [make_xrange_hook(xlim, "force_left")]
        else:
            force_key = "force_mid" if view == "mid" else "force_right"
            hooks = [make_xrange_hook(xlim, force_key)]
        entity_display = timeseries_state.get("entity_display_name") or f"{entity_label.lower()} {timeseries_state['entity_index']}"
        return overlay.opts(
            responsive=True,
            title=f"{metric_label} over Time ({entity_display})",
            tools=["hover", "xwheel_zoom", "xpan", "tap", "reset"],
            active_tools=["xwheel_zoom", "xpan"],
            xlim=xlim,
            axiswise=True,
            shared_axes=False,
            show_legend=False,
            hooks=hooks,
            framewise=True,
        )

    line_left = hv.DynamicMap(
        lambda value=None, center=None, **kwargs: create_timeseries_view(
            value=value, center=center, view="left"
        ),
        streams=[
            borehole_stream,
            streams.Params(depth_selector, parameters=["value"]),
            left_tap,
            center_stream,
        ],
    )

    line_mid = hv.DynamicMap(
        lambda value=None, center=None, **kwargs: create_timeseries_view(
            value=value, center=center, view="mid"
        ),
        streams=[
            borehole_stream,
            streams.Params(depth_selector, parameters=["value"]),
            mid_tap,
            center_stream,
        ],
    )

    line_right = hv.DynamicMap(
        lambda value=None, center=None, **kwargs: create_timeseries_view(
            value=value, center=center, view="right"
        ),
        streams=[
            borehole_stream,
            streams.Params(depth_selector, parameters=["value"]),
            right_tap,
            center_stream,
        ],
    )

    def on_map_tap(x, y):
        if x is None or y is None:
            y, x = _default_coords()
            print(f"[tap] Initial load: using default coords y={y:.4f}, x={x:.4f}")
            print(f"[tap] marker_meta will be None (not computed for initial load)")
            print(f"[tap] Calling _fetch_timeseries with marker_meta=None")
            entity_index = _fetch_timeseries(lat=float(y), lon=float(x), marker_meta=None)
            if entity_index is not None:
                borehole_stream.event(borehole_index=entity_index)
            return None

        print(f"[tap] Click detected: x={x}, y={y}")
        lats_raw = map_state.get("lats")
        lons_raw = map_state.get("lons")
        all_meta = map_state.get("marker_meta") or []
        print(f"[tap] Computing nearest marker from {len(all_meta)} available markers")
        if lats_raw is None or lons_raw is None or not len(all_meta):
            print(f"[tap] No markers available or missing lats/lons")
            return None

        lats = np.array(lats_raw, dtype=float)
        lons = np.array(lons_raw, dtype=float)
        dist = (lats - float(y)) ** 2 + (lons - float(x)) ** 2
        nearest_idx = int(np.nanargmin(dist))
        print(f"[tap] nearest_idx={nearest_idx}, distance={dist[nearest_idx]:.2e}")
        if not (0 <= nearest_idx < len(all_meta)):
            return None

        min_dist = float(dist[nearest_idx])
        # Selection threshold (degrees). Tweak this for your map zoom level.
        threshold_deg = 0.0002
        if min_dist > threshold_deg ** 2:
            print(f"[tap] Outside threshold ({min_dist:.2e} > {threshold_deg**2:.2e}): not selecting")
            # Click wasn't close enough to any marker — don't select.
            return None

        marker_meta = all_meta[nearest_idx]
        print(f"[tap] Selected marker_meta={marker_meta}")
        # Always fetch timeseries - even if map-slice has no value.
        # _fetch_timeseries will check actual series content and update UI accordingly.
        print(f"[tap] Will fetch timeseries regardless of map-slice has_value")
        print(f"[tap] Calling _fetch_timeseries with marker_meta={marker_meta}")
        entity_index = _fetch_timeseries(lat=float(y), lon=float(x), marker_meta=marker_meta)
        if entity_index is not None:
            borehole_stream.event(borehole_index=entity_index)

    on_map_tap(None, None)
    print(f"[timing] build_timeseries_views: {time.perf_counter() - start_total:.3f}s")
    return line_left, line_mid, line_right, on_map_tap