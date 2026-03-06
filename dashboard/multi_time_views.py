import holoviews as hv
import numpy as np
import pandas as pd

from holoviews import streams


def build_timeseries_views(data, depth_selector, borehole_info, borehole_stream, map_state):
    timeseries_state = {
        "times": pd.to_datetime([]),
        "depths": np.array([]),
        "series": [],
        "borehole_index": 0,
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

    def _update_depth_selector(depths, series, borehole_index):
        available = []
        for idx, values in enumerate(series):
            if np.any(np.isfinite(values)):
                available.append(idx)
        if not available:
            available = list(range(len(series)))

        depth_selector.options = {
            format_depth(depths[i]) if i < len(depths) else str(i): i
            for i in available
        }
        depth_selector.value = available
        borehole_info.object = f"### Borehole {borehole_index}"

    def _fetch_timeseries(lat, lon):
        fig = data.client.get_timeseries_data(
            data.endpoint_name,
            group_path=data.group_path,
            lat=lat,
            lon=lon,
            variable="rock_temp",
        )
        if fig.get("status") == "error":
            raise ValueError(fig.get("reason", "Failed to load timeseries"))

        times = pd.to_datetime(fig.get("times", []))
        depths = np.array(fig.get("depths", []), dtype=float)
        series = [np.array(values, dtype=float) for values in fig.get("series", [])]
        borehole_index = int(fig.get("borehole_index", 0))

        timeseries_state["times"] = times
        timeseries_state["depths"] = depths
        timeseries_state["series"] = series
        timeseries_state["borehole_index"] = borehole_index
        _update_depth_selector(depths, series, borehole_index)
        return borehole_index

    def build_timeseries_overlay(selected_depths):
        curves = []
        times = timeseries_state["times"]
        depths = timeseries_state["depths"]
        series = timeseries_state["series"]

        for depth_idx in selected_depths:
            if depth_idx >= len(series):
                continue
            depth_val = depths[depth_idx] if depth_idx < len(depths) else depth_idx
            label = f"{format_depth(depth_val)} m"
            curve_df = pd.DataFrame({
                "time": times,
                "temperature": series[depth_idx],
            })
            curves.append(hv.Curve(curve_df, "time", "temperature", label=label))

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

    mid_span = pd.Timedelta(days=30)
    right_span = pd.Timedelta(hours=24)

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
        overlay = overlay.redim.range(time=xlim)
        overlay = overlay * hv.VLine(center_time).opts(color="red", line_width=2)
        if view == "left":
            hooks = [make_xrange_hook(xlim, "force_left")]
        else:
            force_key = "force_mid" if view == "mid" else "force_right"
            hooks = [make_xrange_hook(xlim, force_key)]
        return overlay.opts(
            responsive=True,
            title=f"Temperature over Time (borehole {timeseries_state['borehole_index']})",
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
        borehole_index = _fetch_timeseries(lat=float(y), lon=float(x))
        borehole_stream.event(borehole_index=borehole_index)

    on_map_tap(None, None)

    return line_left, line_mid, line_right, on_map_tap
