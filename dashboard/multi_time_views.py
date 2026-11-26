import asyncio
import logging
from dataclasses import dataclass
from typing import Any

import holoviews as hv
import numpy as np
import pandas as pd
import panel as pn
from holoviews import streams

from dashboard.config import _resolve_fields_for_group_raw
from dashboard.perf_summary import Timer, count, measure_operation, stage
from dashboard.profiling import operation, time_stage
from dashboard.timeseries_overview import extrema_indices
from dashboard.timeseries_requests import (
    SiteWindow,
    TimeseriesWindow,
    make_timeseries_windows,
)

logger = logging.getLogger(__name__)


@dataclass
class MapClickOperation:
    """Explicit single-flight state for interactive map-click loading."""

    active_generation: int | None = None

    @property
    def active(self) -> bool:
        """Whether a map read owns the click guard."""
        return self.active_generation is not None


def _site_metadata(figure: dict) -> dict:
    return {key: figure[key] for key in ("availability", "single_trace") if key in figure}


def fetch_registered_site_payload(
    client: Any,
    view_name: str | None,
    group_path: str,
    variable: str | None,
    entity_index: int,
) -> dict[str, Any] | None:
    """Fetch one registered site without touching dashboard UI state.

    This function runs in a worker thread. It may use only the supplied client
    and request arguments; callers apply any returned payload on the document
    thread.
    """
    timer = Timer("fill_registered_site")
    if not variable:
        timer.stop()
        return None
    figure = client.get_timeseries_site_metadata(
        view_name,
        group_path=group_path,
        lat=0.0,
        lon=0.0,
        variable=variable,
        entity_index=int(entity_index),
    )
    if figure.get("status") == "error":
        timer.stop()
        count("fill_registered_site.error")
        return None

    times = pd.to_datetime(figure.get("times", []))
    depths = np.array(figure.get("depths", []), dtype=float)
    resolved_index = int(figure.get("borehole_index", entity_index))
    if not len(depths):
        timer.stop()
        count("fill_registered_site.no_data")
        return None
    if resolved_index != entity_index:
        logger.warning(
            "[fill_ts] resolved idx=%s != registered idx=%s; filling registered row",
            resolved_index,
            entity_index,
        )
    timer.stop()
    return {
        "resolved_index": resolved_index,
        "times": times,
        "depths": depths,
        **_site_metadata(figure),
    }


def _resolve_marker_window(x_range, xlim, times):
    """Resolve the (start, end) marker window for a timeseries view.

    The marker window is the *intersection* of the view-specific ``xlim``
    with the live ``x_range`` (the visible window reported by the ``RangeX``
    stream after pan/zoom), clamped to the data bounds.

    Intersecting instead of preferring the live range keeps the markers inside
    the window the plot shows: on the initial render the plot range is
    auto-computed from the whole series, and following it would spread the
    markers over the entire data range, leaving them outside the narrower
    month/day windows and therefore invisible.

    When the live range does not overlap ``xlim`` at all (the user panned
    outside the window) the live range is used as-is, so the markers stay
    visible where the user is looking.  Returns ``None`` when no usable
    window can be derived.
    """
    if times is None or len(times) == 0:
        return None
    live = None
    if x_range is not None and len(x_range) == 2:
        try:
            if not pd.isna(x_range[0]) and not pd.isna(x_range[1]):
                live = (pd.Timestamp(x_range[0]), pd.Timestamp(x_range[1]))
        except (TypeError, ValueError):
            live = None
    if live is None:
        if xlim is None:
            return None
        start, end = xlim[0], xlim[1]
    else:
        start, end = live
        if xlim is not None:
            overlap_start = max(start, xlim[0])
            overlap_end = min(end, xlim[1])
            if overlap_end > overlap_start:
                start, end = overlap_start, overlap_end
    min_t, max_t = times.min(), times.max()
    start = max(start, min_t)
    end = min(end, max_t)
    if not end > start:
        return None
    return (start, end)


def _resolve_tap_targets(x, y, clusters, all_meta, nearest_idx, min_dist, threshold_deg):
    """Resolve the borehole target(s) for a map tap.

    A merged cluster dot is drawn at its centroid, which can sit farther than
    ``threshold_deg`` (a fixed single-marker tolerance) from any of its members.
    When the click lands within ``max(threshold_deg, cluster_radius)`` of a
    merged cluster centroid, return all of its members; otherwise fall back to
    the nearest single marker (respecting ``threshold_deg``).

    Returns a list of marker-meta dicts (each carrying ``entity_index`` for
    cluster members, or the original ``marker_meta`` for the single fallback),
    or ``None`` when the click resolves to nothing.
    """
    if clusters:
        cl_lats = np.array([c["lat"] for c in clusters], dtype=float)
        cl_lons = np.array([c["lon"] for c in clusters], dtype=float)
        c_dist = (cl_lats - float(y)) ** 2 + (cl_lons - float(x)) ** 2
        nearest_cluster = int(np.nanargmin(c_dist))
        cluster = clusters[nearest_cluster]
        radius = float(cluster.get("radius", 0.0))
        hit_threshold = max(threshold_deg, radius)
        if float(c_dist[nearest_cluster]) <= hit_threshold ** 2:
            members = cluster.get("entity_indices") or []
            if len(members) > 1:
                return [{"entity_index": int(m)} for m in members]

    if min_dist <= threshold_deg ** 2:
        return [all_meta[nearest_idx]]
    return None


def _default_coords(map_state: dict, default_site_name: str | None):
    lats = map_state.get("lats")
    lons = map_state.get("lons")
    if lats is None or lons is None:
        lats = np.array([])
        lons = np.array([])
    if len(lats) == 0 or len(lons) == 0:
        return 0.0, 0.0, None
    if default_site_name:
        all_meta = map_state.get("marker_meta") or []
        for meta in all_meta:
            if meta.get("site_id") == default_site_name:
                idx = all_meta.index(meta)
                return float(lats[idx]), float(lons[idx]), meta
    markers = map_state.get("marker_meta") or [None]
    return float(lats[0]), float(lons[0]), markers[0]


def fetch_default_site_figure(
    client: Any,
    view_name: str,
    group_path: str,
    variable: str,
    map_state: dict,
    default_site_name: str | None,
) -> dict:
    """Read the default site's data using the usual map-based selection rule."""
    lat, lon, marker = _default_coords(map_state, default_site_name)
    return client.get_timeseries_site_metadata(
        view_name, group_path=group_path, variable=variable, lat=lat, lon=lon,
        entity_index=marker.get("entity_index") if marker is not None else None,
    )


def build_timeseries_views(
    data,
    map_state,
    selection_state,
    render_spinner=None,
    *,
    map_click_client=None,
    map_click_state=None,
    map_click_generation=None,
    map_click_failure=None,
    initial_site: dict | None = None,
    variable_metadata: dict | None = None,
    windows: dict[str, TimeseriesWindow] | None = None,
):
    if map_click_client is None:
        map_click_client = lambda: data.client
    if map_click_state is None:
        map_click_state = MapClickOperation()
    if map_click_generation is None:
        map_click_generation = lambda: 0
    if windows is None:
        windows = make_timeseries_windows(pn.state.curdoc, data)

    _build_stage = time_stage("build_timeseries_views")
    view_config = data.client.get_view(data.view_name)
    schema_display = view_config["schema_display"]
    schema_config = view_config["schema"]
    fields_config = _resolve_fields_for_group_raw(schema_config, data.group_path)
    time_dim = fields_config.get("time") or "time"
    visualization_config = view_config["visualization"]
    timeseries_config = visualization_config["timeseries"]

    metric_label = (
        data.display_variable
        or schema_display.get("display_variable")
        or "value"
    )
    var_meta = variable_metadata
    if var_meta is None:
        var_meta = data.client.get_variable_metadata(data.view_name, data.group_path, metric_label)
    display_unit = var_meta.get("unit") if var_meta else None
    y_axis_label = f"{metric_label} ({display_unit})" if display_unit else metric_label
    entity_label = (
        schema_display.get("entity_name")
        or fields_config.get("entity")
        or "entity"
    )
    middle_window_days = timeseries_config["middle_window_days"]
    right_window_hours = timeseries_config["right_window_hours"]

    def _apply_site_figure(fig: dict) -> int | None:
        """Apply a fetched time-series figure to the selection on the document thread."""
        if fig.get("status") == "error":
            count("fetch_timeseries.error")
            return None

        times = pd.to_datetime(fig.get("times", []))
        depths = np.array(fig.get("depths", []), dtype=float)
        entity_index = int(fig.get("borehole_index", 0))
        borehole_name = fig.get("borehole_name")

        site_id = borehole_name if borehole_name else f"{entity_label}_{entity_index}"
        with stage("selection.update"):
            exists = any(s["entity_index"] == entity_index for s in selection_state.sites)
            if exists:
                # In-place refresh on a variable change: preserve identity, checked
                # depths and table position.  If the new variable has no depths,
                # blank the site (empty depths) so no stale old-variable data is
                # drawn, and report it as no-data via the None return.
                if not len(depths):
                    selection_state.update_site_data(
                        entity_index,
                        times=pd.to_datetime([]),
                        depths=np.array([]),
                    )
                    count("fetch_timeseries.blank")
                    return None
                selection_state.update_site_data(
                    entity_index,
                    times=times,
                    depths=depths,
                    **_site_metadata(fig),
                )
            else:
                selection_state.add_site(
                    entity_index=entity_index,
                    site_id=site_id,
                    depths=depths,
                    times=times,
                    **_site_metadata(fig),
                )
        return entity_index

    if initial_site is not None:
        _apply_site_figure(initial_site)

    def clamp_range(center, span, times):
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

    _center_time = None
    center_stream = streams.Stream.define("Center", center=None)()

    left_tap = streams.Tap()
    mid_tap = streams.Tap()
    right_tap = streams.Tap()

    left_range_x = streams.RangeX()
    mid_range_x = streams.RangeX()
    right_range_x = streams.RangeX()

    def update_center_from_tap(event):
        if event.new is not None:
            nonlocal _center_time
            with measure_operation("center.update"):
                _center_time = pd.to_datetime(event.new)
                if render_spinner is not None:
                    render_spinner.visible = True
                center_stream.event(center=_center_time)
                if render_spinner is not None:
                    try:
                        pn.state.curdoc.add_timeout_callback(
                            lambda: setattr(render_spinner, 'visible', False), 500
                        )
                    except Exception:
                        pass

    def _make_yrange_hook(ylim):
        def _hook(plot, element):
            plot.state.y_range.start = ylim[0]
            plot.state.y_range.end = ylim[1]
        return _hook

    def _make_xrange_hook(xlim, bounds=None, max_interval_ms=None, reset=None):
        def _hook(plot, element):
            plot.state.x_range.start = xlim[0]
            plot.state.x_range.end = xlim[1]
            reset_start, reset_end = xlim if reset is None else reset
            plot.state.x_range.reset_start = reset_start
            plot.state.x_range.reset_end = reset_end
            if bounds is not None:
                plot.state.x_range.bounds = bounds
            if max_interval_ms is not None:
                plot.state.x_range.max_interval = max_interval_ms
        return _hook

    left_tap.param.watch(update_center_from_tap, ["x"])
    mid_tap.param.watch(update_center_from_tap, ["x"])
    right_tap.param.watch(update_center_from_tap, ["x"])

    ranges = {}

    def plot_viewport(view, times, center, x_range):
        full = (times.min(), times.max())
        span = mid_span if view == "mid" else right_span
        home = full if view == "left" else clamp_range(center, span, times)
        reported = None
        if x_range is not None:
            try:
                endpoints = [
                    pd.Timestamp(value, unit="ms")
                    if isinstance(value, (int, float, np.number))
                    else pd.Timestamp(value)
                    for value in x_range
                ]
                reported = _resolve_marker_window(endpoints, None, times)
            except (TypeError, ValueError, OverflowError):
                pass  # Bokeh may report incomplete bounds during initialization.
        anchor = (center, home)
        previous_anchor, previous_report, visible = ranges.get(view, (None, None, home))
        if anchor != previous_anchor:
            visible = home
        elif (
            reported is not None
            and reported != previous_report
            and any(
                abs(new - old) > pd.Timedelta(milliseconds=1)
                for new, old in zip(reported, visible)
            )
        ):
            visible = reported
        ranges[view] = (anchor, reported, visible)
        return visible, home, full

    def selected_sites():
        sites = {site["entity_index"]: site for site in selection_state.sites}
        depths_by_site = {}
        for entity, depth in selection_state.get_selected_combinations():
            site = sites[entity]
            if len(site["times"]):
                depths_by_site.setdefault(entity, []).append(depth)
        requests = tuple(
            SiteWindow(
                entity,
                tuple(depths),
                sites[entity].get("single_trace", len(sites[entity]["depths"]) == 1),
            )
            for entity, depths in depths_by_site.items()
        )
        return sites, requests

    def build_curves(xlim, requests, sites, traces, *, reduce_dense=False):
        curves, markers, extrema = [], [], []
        shapes = getattr(selection_state, "_row_shapes", {})
        colors = getattr(selection_state, "_col_colors", {})
        marker_times = pd.date_range(*xlim, periods=5)
        for request in requests:
            site = sites[request.entity]
            for depth in request.depths:
                trace = traces.get((request.entity, depth))
                if trace is None:
                    continue
                trace_times, values = trace
                if trace_times.tz is not None:
                    trace_times = trace_times.tz_convert("UTC").tz_localize(None)
                visible = (trace_times >= xlim[0]) & (trace_times <= xlim[1])
                if not visible.any():
                    continue
                depth_value = site["depths"][depth]
                site_id = site["site_id"]
                label = f"{site_id} @ {depth_value:.2f}"
                row_key = (
                    site_id if selection_state.row_dim == "entity" else depth_value
                )
                col_key = (
                    depth_value if selection_state.col_dim == "vertical" else site_id
                )
                color = colors.get(str(col_key), "#000000")
                visible_times, visible_values = trace_times[visible], values[visible]
                retained = (
                    extrema_indices(visible_times, visible_values, 750)
                    if reduce_dense and len(visible_values) > 3000
                    else slice(None)
                )
                data_frame = pd.DataFrame(
                    {
                        time_dim: visible_times[retained],
                        y_axis_label: visible_values[retained],
                    }
                )
                count("timeseries.rendered_samples", len(data_frame))
                curves.append(
                    hv.Curve(data_frame, time_dim, y_axis_label, label=label).opts(
                        color=color
                    )
                )
                finite = values[visible & np.isfinite(values)]
                if finite.size:
                    extrema.extend((finite.min(), finite.max()))
                marker_y = np.interp(
                    marker_times.as_unit("ns").asi8,
                    trace_times.as_unit("ns").asi8,
                    values,
                    left=np.nan,
                    right=np.nan,
                )
                markers.append(
                    hv.Scatter(
                        (marker_times, marker_y), time_dim, y_axis_label, label=label
                    ).opts(
                        color=color, marker=shapes.get(str(row_key), "circle"), size=8
                    )
                )

        ylim = None
        if extrema:
            low, high = float(min(extrema)), float(max(extrema))
            padding = (high - low) * 0.1 or 1.0
            ylim = (low - padding, high + padding)
        if not curves:
            empty = pd.DataFrame({time_dim: pd.to_datetime([]), y_axis_label: []})
            curves.append(hv.Curve(empty, time_dim, y_axis_label))
        return hv.Overlay(curves + markers), ylim

    def create_timeseries_view(center=None, view="left", x_range=None):
        nonlocal _center_time
        timer = Timer("create_timeseries_view")
        count("plot.redraws")
        controller = windows[view]
        times = (
            next(
                (site["times"] for site in selection_state.sites if len(site["times"])),
                None,
            )
            if selection_state is not None
            else None
        )
        if times is None:
            controller.clear_selection()
            return build_curves(
                (pd.Timestamp(0), pd.Timestamp(0)),
                (),
                {},
                {},
            )[0]
        if center is not None:
            _center_time = center
        elif _center_time is None:
            _center_time = times.min() + 0.95 * (times.max() - times.min())
        viewport, home, full = plot_viewport(view, times, _center_time, x_range)
        sites, requests = selected_sites()
        overview = view == "left" and (
            viewport == full or viewport[1] - viewport[0] > mid_span
        )
        # A coarse summary covers the whole history and can be reused across overview pans.
        bounds = full if overview else viewport
        traces, status = controller.request(
            bounds,
            requests,
            time_dim,
            fields_config.get("vertical"),
            overview=overview,
        )
        with stage(f"timeseries.{view}_window.plot"):
            overlay, ylim = build_curves(
                viewport,
                requests,
                sites,
                traces,
                reduce_dense=view == "mid"
                or (view == "left" and not overview and not status),
            )
        overlay = overlay * hv.VLine(_center_time).opts(color="red", line_width=2)
        span = {"mid": mid_span, "right": right_span}.get(view)
        max_interval_ms = int(span.total_seconds() * 1000) if span is not None else None
        hooks = [
            _make_xrange_hook(
                viewport, bounds=full, max_interval_ms=max_interval_ms, reset=home
            )
        ]
        if ylim:
            hooks.append(_make_yrange_hook(ylim))

        def _apply_axis_theme(plot, element):
            axis = plot.state.axis
            axis.major_label_text_color = "#000000"
            axis.axis_label_text_color = "#000000"
            axis.axis_line_alpha = 0
            axis.major_tick_line_alpha = 0
            axis.minor_tick_line_alpha = 0

        hooks.append(_apply_axis_theme)
        n_sites = len(sites)
        site_label = "1 site" if n_sites == 1 else f"{n_sites} sites"
        timer.stop()
        return overlay.opts(
            responsive=True,
            title=f"{metric_label} over Time ({site_label})"
            + (f" ? {status}" if status else ""),
            tools=["hover", "xwheel_zoom", "xpan", "tap", "reset"],
            active_tools=["xwheel_zoom", "xpan"],
            show_legend=False,
            framewise=True,
            hooks=hooks,
        )

    def make_view(view, tap, ranges_x):
        controller = windows[view]
        return hv.DynamicMap(
            lambda center=None, x_range=None, **_: create_timeseries_view(
                center, view, x_range
            ),
            streams=[
                controller.watch_selection(selection_state),
                tap,
                center_stream,
                ranges_x,
                controller.changed,
            ],
        )

    line_left = make_view("left", left_tap, left_range_x)
    line_mid = make_view("mid", mid_tap, mid_range_x)
    line_right = make_view("right", right_tap, right_range_x)

    def _apply_registered_site(entity_index, payload):
        """Apply one fetched payload to the registered selection row."""
        if payload is None:
            return None
        with stage("selection.update"):
            filled = selection_state.fill_site(
                entity_index,
                times=payload["times"],
                depths=payload["depths"],
                **_site_metadata(payload),
            )
        if not filled:
            return None
        resolved_index = int(payload["resolved_index"])
        return resolved_index

    def _map_click_request_is_current(generation, view_name, group_path, variable):
        return (
            generation == map_click_generation()
            and data.view_name == view_name
            and data.group_path == group_path
            and data.display_variable == variable
        )

    def _apply_map_click_result(
        generation, entity_index, payload, view_name, group_path, variable
    ):
        if not _map_click_request_is_current(
            generation, view_name, group_path, variable
        ):
            return None
        return _apply_registered_site(entity_index, payload)

    def _finish_map_click(generation, failed, loading):
        if map_click_state.active_generation != generation:
            return
        map_click_state.active_generation = None
        if generation != map_click_generation():
            return
        if loading is not None:
            loading.visible = False
        if failed and map_click_failure is not None:
            map_click_failure(failed)

    def on_map_tap(x, y, loading=None, doc=None):
        if x is None or y is None:
            return None

        if map_click_state.active:
            return None
        lats_raw = map_state.get("lats")
        lons_raw = map_state.get("lons")
        all_meta = map_state.get("marker_meta") or []
        if lats_raw is None or lons_raw is None or not len(all_meta):
            if loading is not None:
                loading.visible = False
            return None

        lats = np.array(lats_raw, dtype=float)
        lons = np.array(lons_raw, dtype=float)
        dist = (lats - float(y)) ** 2 + (lons - float(x)) ** 2
        nearest_idx = int(np.nanargmin(dist))
        if not (0 <= nearest_idx < len(all_meta)):
            if loading is not None:
                loading.visible = False
            return None

        min_dist = float(dist[nearest_idx])
        # Selection threshold (degrees). Tweak this for your map zoom level.
        threshold_deg = 0.0002
        # Merge-aware resolution: clicking a merged cluster dot selects all of
        # its members even when the cluster centroid lies farther than the
        # single-marker tolerance from the nearest member.  Falls back to the
        # plain nearest-single-borehole behavior when no cluster matches.
        clusters = getattr(data, "current_clusters", None) or []
        targets = _resolve_tap_targets(
            x, y, clusters, all_meta, nearest_idx, min_dist, threshold_deg
        )
        if targets is None:
            if loading is not None:
                loading.visible = False
            return None
        # Cluster merge is a normal interaction detail; no log by default.

        if loading is not None:
            loading.visible = True

        # Registration-first: surface every selected borehole in the plot
        # selection table immediately, then fetch/fill each site's data as it
        # arrives (plots update progressively per member).
        meta_for_eidx = {
            int(m["entity_index"]): m.get("site_id")
            for m in all_meta
            if isinstance(m, dict) and m.get("entity_index") is not None
        }
        for target in targets:
            eidx = int(target["entity_index"])
            sid = target.get("site_id")
            if not sid:
                sid = meta_for_eidx.get(eidx) or f"{entity_label}_{eidx}"
            selection_state.register_site(eidx, sid)

        generation = map_click_generation()
        view_name = data.view_name
        group_path = data.group_path
        variable = data.display_variable
        client = map_click_client()
        target_doc = doc if doc is not None else pn.state.curdoc
        _select_op = operation("borehole.select", site=entity_label, count=len(targets))
        map_click_state.active_generation = generation

        async def _load_targets():
            failed: list[int] = []
            try:
                for target in targets:
                    if generation != map_click_generation():
                        break
                    eidx = int(target["entity_index"])
                    try:
                        payload = await asyncio.to_thread(
                            fetch_registered_site_payload,
                            client,
                            view_name,
                            group_path,
                            variable,
                            eidx,
                        )
                    except Exception:  # noqa: BLE001 - report and continue
                        logger.exception("[tap] failed to fetch site idx=%s", eidx)
                        failed.append(eidx)
                        continue
                    if payload is None:
                        failed.append(eidx)
                        continue
                    if generation != map_click_generation():
                        break

                    def _apply(eid=eidx, result=payload):
                        try:
                            resolved = _apply_map_click_result(
                                generation,
                                eid,
                                result,
                                view_name,
                                group_path,
                                variable,
                            )
                        except Exception:  # noqa: BLE001 - report and continue
                            logger.exception("[tap] failed to apply site idx=%s", eid)
                            resolved = None
                        if resolved is None:
                            failed.append(eid)

                    target_doc.add_next_tick_callback(_select_op.wrap(_apply))
            finally:
                if generation != map_click_generation():
                    if map_click_state.active_generation == generation:
                        map_click_state.active_generation = None
                    _select_op.finish(status="cancelled")
                else:
                    def _finish():
                        _finish_map_click(generation, failed, loading)
                        if failed and generation == map_click_generation():
                            _select_op.finish(status="error")

                    target_doc.add_next_tick_callback(_select_op.wrap(_finish, finish=True))

        if target_doc is not None:
            asyncio.create_task(_select_op.wrap(_load_targets)())
        else:
            def _load_targets_sync():
                failed: list[int] = []
                try:
                    with measure_operation(
                        "borehole.select", site=entity_label, sites=len(targets)
                    ):
                        for target in targets:
                            eidx = int(target["entity_index"])
                            payload = fetch_registered_site_payload(
                                client, view_name, group_path, variable, eidx
                            )
                            if payload is None:
                                failed.append(eidx)
                                continue
                            try:
                                resolved = _apply_map_click_result(
                                    generation,
                                    eidx,
                                    payload,
                                    view_name,
                                    group_path,
                                    variable,
                                )
                            except Exception:  # noqa: BLE001 - report and continue
                                logger.exception("[tap] failed to apply site idx=%s", eidx)
                                resolved = None
                            if resolved is None:
                                failed.append(eidx)
                finally:
                    _finish_map_click(generation, failed, loading)
                    if failed and generation == map_click_generation():
                        _select_op.finish(status="error")

            _select_op.wrap(_load_targets_sync, finish=True)()

        return int(targets[0]["entity_index"]) if targets else None

    _build_stage.log()
    return line_left, line_mid, line_right, on_map_tap
