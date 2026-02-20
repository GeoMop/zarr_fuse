import holoviews as hv
import geoviews as gv
import numpy as np
import pandas as pd

from holoviews import streams
from geoviews import tile_sources as gvts


def build_map_view(data, tap_stream):
    base_map = gvts.OSM()

    overlay = gv.Rectangles([data.overlay_bounds]).opts(
        alpha=0.2,
        color="orange",
        line_width=2,
        line_color="red",
    )

    map_points = gv.Points(data.map_df, kdims=["lon", "lat"], vdims=["value"]).opts(
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
    return base_map * overlay * map_points


def build_timeseries_views(data, depth_selector, borehole_info, borehole_stream):
    group = data.group
    depth_arr = data.depth_arr
    date_time_index = data.date_time_index
    lats_arr = data.lats
    lons_arr = data.lons

    def get_borehole_index(x, y):
        if x is None or y is None:
            return 0
        dist = (lons_arr - x) ** 2 + (lats_arr - y) ** 2
        return int(np.nanargmin(dist))

    def get_available_depth_indices(borehole_index: int):
        values = np.array(group["rock_temp"][:, borehole_index, :], dtype=float)
        mask = np.any(np.isfinite(values), axis=0)
        return [int(i) for i in np.where(mask)[0].tolist()]

    def format_depth(depth_value: float):
        if np.isnan(depth_value):
            return "NaN"
        return f"{depth_value:.2f}"

    def update_depth_selector(x, y):
        borehole_index = get_borehole_index(x, y)
        available = get_available_depth_indices(borehole_index)
        depth_selector.options = {format_depth(depth_arr[i]): i for i in available}
        depth_selector.value = available
        borehole_info.object = f"### Borehole {borehole_index}"
        return borehole_index

    def build_timeseries_overlay(borehole_index, selected_depths):
        curves = []
        values_by_depth = [
            np.array(group["rock_temp"][:, borehole_index, depth_idx], dtype=float)
            for depth_idx in selected_depths
        ]
        times = date_time_index
        for col_idx, depth_idx in enumerate(selected_depths):
            depth_val = depth_arr[depth_idx] if depth_idx < len(depth_arr) else depth_idx
            label = f"{format_depth(depth_val)} m"
            curve_df = pd.DataFrame({
                "time": times,
                "temperature": values_by_depth[col_idx],
            })
            curves.append(hv.Curve(curve_df, "time", "temperature", label=label))

        if not curves:
            curves = [hv.Curve([])]
        return hv.Overlay(curves)

    def clamp_range(center, span):
        min_t = date_time_index.min()
        max_t = date_time_index.max()
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

    full_span = date_time_index.max() - date_time_index.min()
    mid_span = pd.Timedelta(days=30)
    right_span = pd.Timedelta(hours=24)

    center_state = {
        "center": date_time_index.min() + (full_span / 2),
        "force_mid": False,
        "force_right": False,
    }
    center_stream = streams.Stream.define("Center", center=None)()
    center_stream.event(center=center_state["center"])

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

    def make_xrange_hook(xlim, force_flag_key):
        def _hook(plot, element):
            plot.state.x_range.bounds = xlim
            if center_state.get(force_flag_key, False):
                plot.state.x_range.start = xlim[0]
                plot.state.x_range.end = xlim[1]
                center_state[force_flag_key] = False
        return _hook

    left_tap.param.watch(update_center_from_tap, ["x"])
    mid_tap.param.watch(update_center_from_tap, ["x"])
    right_tap.param.watch(update_center_from_tap, ["x"])

    def create_timeseries_view(value=None, center=None, borehole_index=0, view="left", **_):
        selected_depths = value or []
        if not selected_depths:
            selected_depths = get_available_depth_indices(borehole_index)

        center_time = center or center_state["center"]
        if view == "left":
            xlim = (date_time_index.min(), date_time_index.max())
        elif view == "mid":
            xlim = clamp_range(center_time, mid_span)
        else:
            xlim = clamp_range(center_time, right_span)

        overlay = build_timeseries_overlay(borehole_index, selected_depths)
        overlay = overlay.redim.range(time=xlim)
        overlay = overlay * hv.VLine(center_time).opts(color="red", line_width=2)
        hooks = []
        if view != "left":
            force_key = "force_mid" if view == "mid" else "force_right"
            hooks = [make_xrange_hook(xlim, force_key)]
        return overlay.opts(
            responsive=True,
            title=f"Temperature over Time (borehole {borehole_index})",
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
        lambda value=None, center=None, borehole_index=0, **kwargs: create_timeseries_view(
            value=value, center=center, borehole_index=borehole_index, view="left"
        ),
        streams=[
            borehole_stream,
            streams.Params(depth_selector, parameters=["value"]),
            left_tap,
            center_stream,
        ],
    )

    line_mid = hv.DynamicMap(
        lambda value=None, center=None, borehole_index=0, **kwargs: create_timeseries_view(
            value=value, center=center, borehole_index=borehole_index, view="mid"
        ),
        streams=[
            borehole_stream,
            streams.Params(depth_selector, parameters=["value"]),
            mid_tap,
            center_stream,
        ],
    )

    line_right = hv.DynamicMap(
        lambda value=None, center=None, borehole_index=0, **kwargs: create_timeseries_view(
            value=value, center=center, borehole_index=borehole_index, view="right"
        ),
        streams=[
            borehole_stream,
            streams.Params(depth_selector, parameters=["value"]),
            right_tap,
            center_stream,
        ],
    )

    def on_map_tap(x, y):
        borehole_index = update_depth_selector(x, y)
        borehole_stream.event(borehole_index=borehole_index)

    update_depth_selector(None, None)

    return line_left, line_mid, line_right, on_map_tap
