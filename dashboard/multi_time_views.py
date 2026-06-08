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


def build_timeseries_views(data, map_state, selection_state):
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

    def _default_coords():
        lats = map_state.get("lats")
        lons = map_state.get("lons")
        if lats is None or lons is None:
            lats = np.array([])
            lons = np.array([])
        if len(lats) == 0 or len(lons) == 0:
            return 0.0, 0.0
        return float(lats[0]), float(lons[0])

    def _fetch_timeseries(lat, lon, marker_meta=None):
        start = time.perf_counter()
        print(f"[fetch_ts] Called with lat={lat:.4f}, lon={lon:.4f}, marker_meta={marker_meta}")
        if not default_display_variable:
            print(f"[fetch_ts] No variable selected — skipping fetch")
            return None
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
            print(f"[fetch_ts] Error: {reason}")
            print(f"[timing] timeseries fetch failed: {time.perf_counter() - start:.3f}s")
            return None

        times = pd.to_datetime(fig.get("times", []))
        depths = np.array(fig.get("depths", []), dtype=float)
        series = [np.array(values, dtype=float) for values in fig.get("series", [])]
        entity_index = int(fig.get("borehole_index", 0))
        borehole_name = fig.get("borehole_name")

        site_id = borehole_name if borehole_name else f"{entity_label}_{entity_index}"
        selection_state.add_site(
            entity_index=entity_index,
            site_id=site_id,
            depths=depths,
            series=series,
            times=times,
        )
        print(f"[plot_selection] Site added: {site_id}")
        print(f"[timing] timeseries fetch+state: {time.perf_counter() - start:.3f}s")
        return entity_index

    def build_timeseries_overlay():
        if selection_state is None:
            empty_df = pd.DataFrame({time_dim: pd.to_datetime([]), y_axis_label: []})
            return hv.Overlay([hv.Curve(empty_df, time_dim, y_axis_label)])
        selected_combos = selection_state.get_selected_combinations()
        if not selected_combos or not selection_state.sites:
            empty_df = pd.DataFrame({time_dim: pd.to_datetime([]), y_axis_label: []})
            return hv.Overlay([hv.Curve(empty_df, time_dim, y_axis_label)])

        site_lookup = {s["entity_index"]: s for s in selection_state.sites}
        times = next(
            (s["times"] for s in selection_state.sites if len(s["times"]) > 0),
            None,
        )
        if times is None or len(times) == 0:
            empty_df = pd.DataFrame({time_dim: pd.to_datetime([]), y_axis_label: []})
            return hv.Overlay([hv.Curve(empty_df, time_dim, y_axis_label)])

        curves = []
        for entity_idx, depth_idx in selected_combos:
            site = site_lookup.get(entity_idx)
            if site is None or depth_idx >= len(site["series"]):
                continue
            depths_arr = np.asarray(site["depths"]).ravel()
            depth_val = depths_arr[depth_idx] if depth_idx < len(depths_arr) else depth_idx
            label = f"{site['site_id']} @ {depth_val:.2f}"
            curve_df = pd.DataFrame({
                time_dim: times,
                y_axis_label: site["series"][depth_idx],
            })
            curves.append(hv.Curve(curve_df, time_dim, y_axis_label, label=label))

        if not curves:
            empty_df = pd.DataFrame({time_dim: pd.to_datetime([]), y_axis_label: []})
            curves = [hv.Curve(empty_df, time_dim, y_axis_label)]
        return hv.Overlay(curves)

    def clamp_range(center, span):
        if selection_state is None:
            return (pd.Timestamp("1970-01-01"), pd.Timestamp("1970-01-02"))
        times = next(
            (s["times"] for s in selection_state.sites if len(s["times"]) > 0),
            pd.to_datetime([]),
        )
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

    def _make_yrange_hook(ylim):
        def _hook(plot, element):
            plot.state.y_range.start = ylim[0]
            plot.state.y_range.end = ylim[1]
        return _hook

    left_tap.param.watch(update_center_from_tap, ["x"])
    mid_tap.param.watch(update_center_from_tap, ["x"])
    right_tap.param.watch(update_center_from_tap, ["x"])

    def _compute_ylim(times, selected_combos, xlim):
        if selection_state is None or len(times) == 0 or not selected_combos:
            return None
        mask = (times >= xlim[0]) & (times <= xlim[1])
        visible = np.where(mask)[0]
        if len(visible) == 0:
            return None
        values = []
        site_lookup = {s["entity_index"]: s for s in selection_state.sites}
        for entity_idx, depth_idx in selected_combos:
            site = site_lookup.get(entity_idx)
            if site is None or depth_idx >= len(site["series"]):
                continue
            vals = site["series"][depth_idx][visible]
            values.extend(vals[np.isfinite(vals)])
        if not values:
            return None
        ymin, ymax = float(np.min(values)), float(np.max(values))
        padding = (ymax - ymin) * 0.1 or 1.0
        return (ymin - padding, ymax + padding)

    def create_timeseries_view(center=None, view="left", **_):
        if selection_state is None:
            empty_df = pd.DataFrame({time_dim: pd.to_datetime([]), y_axis_label: []})
            return hv.Overlay([hv.Curve(empty_df, time_dim, y_axis_label)])

        selected_combos = selection_state.get_selected_combinations()
        times = next(
            (s["times"] for s in selection_state.sites if len(s["times"]) > 0),
            None,
        )
        if times is None or len(times) == 0:
            empty_df = pd.DataFrame({time_dim: pd.to_datetime([]), y_axis_label: []})
            return hv.Overlay([hv.Curve(empty_df, time_dim, y_axis_label)])

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

        overlay = build_timeseries_overlay()
        overlay = overlay.redim.range(**{time_dim: xlim})
        overlay = overlay * hv.VLine(center_time).opts(color="red", line_width=2)

        ylim = _compute_ylim(times, selected_combos, xlim)

        if view == "left":
            hooks = [make_xrange_hook(xlim, "force_left")]
        else:
            force_key = "force_mid" if view == "mid" else "force_right"
            hooks = [make_xrange_hook(xlim, force_key)]
        if ylim:
            hooks.append(_make_yrange_hook(ylim))
        n_sites = len(selection_state.sites)
        site_label = "1 site" if n_sites == 1 else f"{n_sites} sites"
        return overlay.opts(
            responsive=True,
            title=f"{metric_label} over Time ({site_label})",
            tools=["hover", "xwheel_zoom", "xpan", "tap", "reset"],
            active_tools=["xwheel_zoom", "xpan"],
            xlim=xlim,
            axiswise=True,
            shared_axes=False,
            show_legend=True,
            hooks=hooks,
            framewise=True,
        )

    line_left = hv.DynamicMap(
        lambda center=None, **kwargs: create_timeseries_view(
            center=center, view="left"
        ),
        streams=[
            streams.Params(selection_state, parameters=["version"]),
            left_tap,
            center_stream,
        ],
    )

    line_mid = hv.DynamicMap(
        lambda center=None, **kwargs: create_timeseries_view(
            center=center, view="mid"
        ),
        streams=[
            streams.Params(selection_state, parameters=["version"]),
            mid_tap,
            center_stream,
        ],
    )

    line_right = hv.DynamicMap(
        lambda center=None, **kwargs: create_timeseries_view(
            center=center, view="right"
        ),
        streams=[
            streams.Params(selection_state, parameters=["version"]),
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
            return entity_index

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
            return None

        marker_meta = all_meta[nearest_idx]
        print(f"[tap] Selected marker_meta={marker_meta}")
        print(f"[tap] Calling _fetch_timeseries with marker_meta={marker_meta}")
        entity_index = _fetch_timeseries(lat=float(y), lon=float(x), marker_meta=marker_meta)
        return entity_index

    print(f"[timing] build_timeseries_views: {time.perf_counter() - start_total:.3f}s")
    return line_left, line_mid, line_right, on_map_tap