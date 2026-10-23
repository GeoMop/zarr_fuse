import asyncio

import holoviews as hv
import panel as pn
from bokeh.util.serialization import make_globally_unique_id
from holoviews import streams

from dashboard.config import find_view_file, get_default_endpoint_name, load_view_config, load_views
from dashboard.data import load_data
from dashboard.map_views import build_map_view
from dashboard.multi_time_views import build_timeseries_views
from dashboard.perf_summary import begin, end, measure_operation
from dashboard.plot_selection import build_plot_selection_panel, resolve_available_dimensions
from dashboard.profiling import operation, session, time_stage
from dashboard.sidebar import _flatten_nodes, build_sidebar

JS_FILES = {
    "jquery": "https://code.jquery.com/jquery-1.11.1.min.js",
    "goldenlayout": "https://golden-layout.com/files/latest/js/goldenlayout.min.js",
}
CSS_FILES = [
    "https://golden-layout.com/files/latest/css/goldenlayout-base.css",
    "https://golden-layout.com/files/latest/css/goldenlayout-dark-theme.css",
]

pn.extension(
    "tabulator",
    js_files=JS_FILES,
    css_files=CSS_FILES,
    design="material",
    theme="dark",
    sizing_mode="stretch_width",
)
pn.config.reconnect = True
pn.config.notifications = True
hv.extension("bokeh")
hv.renderer("bokeh").theme = "dark_minimal"

_POPULATE_PAUSE = 0.05


def _placeholder_root(sizing_mode: str, label: str) -> pn.Column:
    """Create an empty layout root that shows one spinner until it is populated.

    The spinner is the explicit ``LoadingSpinner`` next to the per-window label;
    ``loading=True`` is deliberately not used here so a window does not show a
    second, unlabelled Panel overlay spinner on top of it.
    """
    return pn.Column(
        pn.Row(
            pn.indicators.LoadingSpinner(value=True, color="light", bgcolor="dark", size=32),
            pn.pane.Markdown(label, styles={"color": "#dbeafe"}),
            align="center",
            sizing_mode=sizing_mode,
        ),
        sizing_mode=sizing_mode,
    )


def _set_root(root: pn.Column, content: object, label: str = "") -> None:
    """Replace a skeleton root's spinner with its real content.

    ``label`` names the pane in the ``pane.attach`` stage record so the model
    construction cost of each window can be attributed separately.
    """
    with time_stage("pane.attach", pane=label):
        root.objects = [content]


def _report_error(label: str, exc: BaseException) -> None:
    """Print one traceback for a populate phase that failed."""
    import traceback

    traceback.print_exc()
    print(f"[populate] {label} failed: {exc}")


def _set_root_error(root: pn.Column, label: str, exc: BaseException) -> None:
    """Show a populate failure inside the pane it belongs to."""
    _set_root(
        root,
        pn.pane.Alert(
            f"⚠️ {label} failed: {str(exc)[:120]}",
            alert_type="danger",
            sizing_mode="stretch_width",
        ),
        label=f"{label} (error)",
    )


def _map_pane(content: object) -> pn.pane.HoloViews:
    """Wrap a map element in the pane used by the map window."""
    return pn.pane.HoloViews(content, sizing_mode="stretch_both")


def _line_pane(content: object) -> pn.pane.HoloViews:
    """Wrap a timeseries element in the pane used by the timeseries windows."""
    return pn.pane.HoloViews(content, sizing_mode="stretch_both", linked_axes=False)


def build_dashboard():
    _build_stage = time_stage("build_dashboard")

    session_ctx = pn.state.curdoc.session_context if pn.state.curdoc and pn.state.curdoc.session_context else None
    session_id = session_ctx.id if session_ctx else "no-session"
    # Manual scopes (no re-indentation of the whole body): every timer created
    # below inherits the session and operation tags.
    _session_scope = session(session_id)
    _session_scope.__enter__()
    _load_op = operation("dashboard.initial_load", session=session_id)
    _load_scope = _load_op.activate()
    _load_scope.__enter__()
    _summary = begin("dashboard.initial_load")
    print(f"[session] build_dashboard started session_id={session_id}")

    def _on_session_destroyed(session_context):
        # Idempotent: finishes an initial_load left open by a failed build.
        _load_op.finish(status="error")
        sid = session_context.id if session_context is not None else session_id
        print(f"[session] session destroyed session_id={sid}")

    pn.state.on_session_destroyed(_on_session_destroyed)

    views_path = find_view_file()
    print(f"Using views config: {views_path}")

    configured_default = get_default_endpoint_name(views_path)
    views = load_views(views_path)
    if not views:
        raise ValueError(f"No views configured in {views_path}")

    view_name = configured_default or next(iter(views.keys()))

    data = load_data(
        "local",
        view_name=view_name,
        views_path=views_path,
        display_variable="",
    )

    views = data.client.get_views()
    view = views.get(view_name) or data.client.get_view(view_name)
    structure: dict | None = None

    loading_indicator = pn.Row(
        pn.indicators.LoadingSpinner(value=True, color='light', bgcolor='dark', size=32),
        pn.pane.Markdown("Loading selected dataset...", styles={"color": "#dbeafe"}),
        visible=False,
        sizing_mode="stretch_width",
    )
    timeseries_loading = pn.Row(
        pn.indicators.LoadingSpinner(value=True, color='light', bgcolor='dark', size=32),
        pn.pane.Markdown("Loading timeseries data...", styles={"color": "#dbeafe"}),
        visible=False,
        sizing_mode="stretch_width",
    )
    rendering_status = pn.pane.Alert(
        "",
        alert_type="danger",
        visible=False,
        sizing_mode="stretch_width",
    )
    render_spinner = pn.Row(
        pn.indicators.LoadingSpinner(value=True, color='light', bgcolor='dark', size=32),
        pn.pane.Markdown("Rendering...", styles={"color": "#dbeafe"}),
        visible=False,
        sizing_mode="stretch_width",
    )
    table_loading = pn.Row(visible=False)

    controller = _placeholder_root("stretch_width", "Loading dataset structure...")
    store_selector = None
    node_select = None
    variable_selector = None
    variable_metadata = None
    node_hint = None
    store_info = None
    reload_button = None

    # ── Variable dropdown for the plot selection panel ──────────────
    plot_var_selector = pn.widgets.Select(
        name="Variable",
        options=[],
        value=None,
        width=120,
    )

    # ── Table-style plot selection ──────────────────────────────────
    # Built in _populate(): it depends on data.group_path, which is read from
    # the store structure.
    selection_state = None  # created by build_plot_selection_panel
    # ────────────────────────────────────────────────────────────────

    tap_stream = streams.Tap(x=None, y=None)

    map_handlers = {"on_map_tap": lambda *_, **__: None}

    map_view, map_state = None, None

    _current_var_label: str | None = None
    _suppress_variable_change = False

    def _fetch_and_show_metadata(var_name: str):
        name_for_api = var_name
        meta = data.client.get_variable_metadata(
            data.view_name, data.group_path, name_for_api
        )
        if meta:
            coords_text = ", ".join(meta.get("coords", []))
            unit_text = f" ({meta['unit']})" if meta.get("unit") else ""
            variable_metadata.object = (
                "<div style='background: #1e293b; padding: 12px; border-radius: 8px; "
                "margin: 8px 0; border-left: 3px solid #10b981;'>"
                "<div style='font-size: 11px; color: #94a3b8; margin-bottom: 6px; "
                "font-weight: 600;'>📋 VARIABLE METADATA</div>"
                f"<div style='font-size: 13px; color: #e2e8f0; font-weight: 600; "
                f"margin-bottom: 6px;'>{meta['name']}{unit_text}</div>"
                f"<div style='font-size: 11px; color: #cbd5e1; line-height: 1.6;'>"
                f"<b>Description:</b> {meta.get('description', '—')}<br>"
                f"<b>Unit:</b> {meta.get('unit', '—')}<br>"
                f"<b>Coordinates:</b> {coords_text or '—'}"
                "</div></div>"
            )
            variable_metadata.visible = True

    def _populate_variable_selector(view_name: str, group_path: str):
        nonlocal _current_var_label, _suppress_variable_change
        try:
            print(f"[variables] Loading for view={view_name}, group={group_path}")
            with time_stage("get_variables", view=view_name, group=group_path):
                variables = data.client.get_variables(view_name, group_path)
            print(f"[variables] Found: {len(variables)} variables")

            if variables:
                var_options = [f"{name} ({unit})" if unit else name for name, unit in variables.items()]

                for name, unit in variables.items():
                    print(f"[variables]   - {name}: unit={unit}")

                view_cfg = load_view_config(views_path, view_name)
                default_var = view_cfg.defaults.display_variable if view_cfg.defaults else None

                if default_var and default_var in variables:
                    var_label = default_var + (f" ({variables[default_var]})" if variables[default_var] else "")
                else:
                    var_label = var_options[0] if var_options else None

                # Changing options/value would otherwise fire the watcher once
                # for the first option and once for the default, rebuilding all
                # panes twice before the first real render.
                _suppress_variable_change = True
                try:
                    variable_selector.options = var_options
                    if var_label:
                        var_name = var_label.split(" (")[0] if " (" in var_label else var_label
                        _current_var_label = var_label
                        data.display_variable = var_name
                        variable_metadata.visible = False
                        variable_selector.value = var_label
                        plot_var_selector.options = [var_label]
                        plot_var_selector.value = var_label
                        _fetch_and_show_metadata(var_name)
                    else:
                        variable_selector.value = None
                        data.display_variable = ""
                        variable_metadata.visible = False
                        plot_var_selector.options = []
                finally:
                    _suppress_variable_change = False

                print(f"[variables] Loaded {len(variables)} variables successfully")
            else:
                variable_selector.options = []
                variable_metadata.visible = False
                plot_var_selector.options = []
                print(f"[variables] No variables in group {group_path}")
        except Exception as e:
            variable_selector.options = []
            variable_metadata.visible = False
            plot_var_selector.options = []
            print(f"[variables] ERROR: {e}")

    def _select_variable(var_name: str, label: str | None = None):
        if var_name != data.display_variable:
            print(f"[variables] Changing from {data.display_variable} to {var_name}")
            data.display_variable = var_name
            loading_indicator.visible = True
            display_label = label or var_name
            # Keep the single-option dropdown in sync with the selected variable
            plot_var_selector.options = [display_label]
            plot_var_selector.value = display_label

            _switch_op = operation("variable.switch", variable=var_name)

            def _run_refresh():
                with measure_operation("variable.switch", variable=var_name):
                    try:
                        # Refresh each site's data with the current variable first,
                        # then rebuild the plot closures so the freshly-built
                        # dynamic plots render already-updated data on their first
                        # render.  The map and plot-selection table are not
                        # rerendered (their content is variable-independent).
                        _refetch_sites()
                        refresh_views(rebuild_map=False)
                        plot_var_selector.value = display_label
                        _fetch_and_show_metadata(var_name)
                    except Exception as e:
                        import traceback; traceback.print_exc()
                        plot_var_selector.options = []
                        rendering_status.object = f"⚠️ Render error: {str(e)[:80]}"
                        rendering_status.visible = True
                        pn.state.add_timeout(None, 5000, lambda: setattr(rendering_status, "visible", False))
                        print(f"[variables] Error viewing {var_name}: {e}")
                    finally:
                        loading_indicator.visible = False

            _wrapped = _switch_op.wrap(_run_refresh, finish=True)
            doc = pn.state.curdoc
            if doc is not None:
                doc.add_next_tick_callback(_wrapped)
            else:
                _wrapped()

    def _refetch_sites():
        """Re-fetch every registered site with the current variable, in place.

        Each site's data is replaced via ``fetch_site`` (by stable
        ``entity_index``) so every selected site moves to the new variable
        regardless of coordinate filtering or map CRS units.  Sites are never
        cleared, so checked depths and table position are preserved.  Reports
        per-site no-data/failure in the service status.
        """
        if not selection_state.sites:
            return
        failed: list[str] = []
        timeseries_loading.visible = True
        try:
            for site in list(selection_state.sites):
                idx = site["entity_index"]
                result = map_handlers["fetch_site"](idx)
                if result is None:
                    failed.append(str(idx))
                    print(f"[refresh_views] Failed to refresh site idx={idx}")
                else:
                    print(f"[refresh_views] Refreshed site idx={idx}")
        finally:
            timeseries_loading.visible = False
        if failed:
            _show_refresh_error(failed)

    def _show_refresh_error(failed: list[str]):
        label = plot_var_selector.value or data.display_variable
        rendering_status.object = (
            f"⚠️ {label}: data not available at {len(failed)} site(s) "
            f"({', '.join(failed)})"
        )
        rendering_status.visible = True
        doc = pn.state.curdoc
        if doc is not None:
            doc.add_timeout_callback(
                lambda: setattr(rendering_status, "visible", False), 5000
            )
        else:
            rendering_status.visible = False

    def on_variable_change(event):
        if _suppress_variable_change:
            return
        selected_label = event.new
        if selected_label:
            var_name = selected_label.split(" (")[0] if " (" in selected_label else selected_label
            _select_variable(var_name, label=selected_label)

    def _refresh_sidebar_for_view(selected_view: str):
        nonlocal views, view, structure
        with time_stage("_refresh_sidebar", view=selected_view):
            views = data.client.get_views()
            view = views.get(selected_view) or data.client.get_view(selected_view)
            structure = data.client.get_structure(selected_view)

            node_items = _flatten_nodes(structure)
            node_options = {label: path for label, path in node_items}
            node_select.options = node_options
            node_select.value = node_items[0][1] if node_items else "/"

            store_info.object = (
                "<div style='background: #1e293b; padding: 12px; border-radius: 8px; margin: 8px 0;"
                " border-left: 3px solid #3b82f6;'>"
                "<div style='font-size: 11px; color: #94a3b8; margin-bottom: 4px; font-weight: 600;'>"
                "STORE URI</div>"
                f"<div style='font-size: 12px; color: #e2e8f0; font-family: monospace;'>{view['source']['uri']}</div>"
                "</div>"
            )
            _populate_variable_selector(selected_view, data.group_path)

    def _switch_view(selected_view: str):
        nonlocal data, view_name

        view_name = selected_view
        view_obj = data.client.get_view(selected_view)
        data.view_name = selected_view
        data.group_path = "/"
        data.display_variable = ""
        
        # Clear cache for new view
        data.client.clear_cache()
        
        selection_state.clear()
        with time_stage("_switch_view", view=selected_view):
            _refresh_sidebar_for_view(selected_view)
            refresh_views()

    def update_data_warnings(state):
        reason = (state or {}).get("data_error_reason")
        if reason:
            group_path = data.group_path
            variable = (state or {}).get("variable") or "<unknown>"
            message = (
                f"No data for group '{group_path}' with variable '{variable}': {reason}. "
                "Please select another dataset node from Data Structure."
            )
            node_hint.object = message
            node_hint.visible = True
            return

        node_hint.object = ""
        node_hint.visible = False

    def on_tap_event(*_):
        if timeseries_loading.visible:
            return  # debounce — already loading
        timeseries_loading.visible = True
        # Capture coordinates at tap time (avoid stale values after delay)
        _x, _y = tap_stream.x, tap_stream.y
        doc = pn.state.curdoc

        def _do_tap():
            # The spinner is turned off by the deferred fetch loop inside
            # on_map_tap once the last member's data has landed, not here.
            map_handlers["on_map_tap"](_x, _y, loading=timeseries_loading)

        if doc is not None:
            # Use timeout, not next_tick — next_tick runs synchronously during
            # document unlock, batching visible=True + visible=False together
            # so the frontend never sees the spinner.  A small timeout lets the
            # UI flush visible=True before the blocking I/O starts.
            doc.add_timeout_callback(_do_tap, 100)
        else:
            _do_tap()

    top_left = _placeholder_root("stretch_both", "Loading map view...")
    top_right = _placeholder_root("stretch_both", "Loading plot selection...")
    bottom_left = _placeholder_root("stretch_both", "Loading year timeseries...")
    bottom_mid = _placeholder_root("stretch_both", "Loading month timeseries...")
    bottom_right = _placeholder_root("stretch_both", "Loading day timeseries...")

    def refresh_views(rebuild_map: bool = True):
        with time_stage("refresh_views", rebuild_map=rebuild_map):
            if rebuild_map:
                new_map_view, new_map_state = build_map_view(data, tap_stream)
                update_data_warnings(new_map_state)
                map_state.clear()
                map_state.update(new_map_state)
            else:
                # Reuse the existing map: its overlay tiles and point positions are
                # variable-independent, so a variable change does not require a map
                # rebuild (only the timeseries plots need to redraw).
                new_map_state = map_state

            new_line_left, new_line_mid, new_line_right, new_on_map_tap, new_fetch_site = build_timeseries_views(
                data,
                new_map_state,
                selection_state=selection_state,
                render_spinner=render_spinner,
            )

            map_handlers["on_map_tap"] = new_on_map_tap
            map_handlers["fetch_site"] = new_fetch_site
            if rebuild_map:
                _set_root(top_left, _map_pane(new_map_view), label="top_left")
            _set_root(bottom_left, _line_pane(new_line_left), label="bottom_left")
            _set_root(bottom_mid, _line_pane(new_line_mid), label="bottom_mid")
            _set_root(bottom_right, _line_pane(new_line_right), label="bottom_right")

    def on_store_change(event):
        if event.new and event.new != view_name:
            loading_indicator.visible = True

            _switch_op = operation("view.switch", view=event.new)

            def _run_switch():
                with measure_operation("view.switch", view=event.new):
                    try:
                        _switch_view(event.new)
                    finally:
                        loading_indicator.visible = False

            _wrapped = _switch_op.wrap(_run_switch, finish=True)
            doc = pn.state.curdoc
            if doc is not None:
                doc.add_next_tick_callback(_wrapped)
            else:
                _wrapped()

    def on_node_change(event):
        if event.new:
            data.group_path = event.new
            selection_state.clear()
            loading_indicator.visible = True

            _switch_op = operation("group.switch", group=event.new)

            def _run_refresh():
                with measure_operation("group.switch", group=event.new):
                    try:
                        _populate_variable_selector(data.view_name, data.group_path)
                        refresh_views()
                    finally:
                        loading_indicator.visible = False

            _wrapped = _switch_op.wrap(_run_refresh, finish=True)
            doc = pn.state.curdoc
            if doc is not None:
                doc.add_next_tick_callback(_wrapped)
            else:
                _wrapped()

    def on_reload(_event=None):
        """Re-read the current view / group / variable from the store.

        The current dashboard state is kept: view, group path, variable, the
        registered sites and their checked depths.  Only the underlying data is
        discarded and fetched again, then the map and the timeseries panes are
        rebuilt.  This is intentionally a data refresh only -- the data
        structure tree and the variable list are not re-read, and the
        timeseries time window re-anchors to the rebuilt views.
        """
        if loading_indicator.visible:
            return  # debounce — a reload is already running

        loading_indicator.visible = True

        _reload_op = operation(
            "data.reload",
            view=view_name,
            group=data.group_path,
            variable=data.display_variable,
        )

        def _run_reload():
            with measure_operation("data.reload", view=view_name):
                try:
                    # Drop cached map/timeseries payloads; the zarr nodes reopen
                    # their dataset lazily so values are re-read from the store.
                    data.client.clear_cache()
                    if data.display_variable:
                        # Refresh each site in place so the table position and the
                        # checked depths survive the reload.
                        _refetch_sites()
                    refresh_views(rebuild_map=True)
                except Exception as exc:  # noqa: BLE001 - surface any reload failure
                    import traceback
                    traceback.print_exc()
                    rendering_status.object = f"⚠️ Reload failed: {str(exc)[:80]}"
                    rendering_status.visible = True
                    pn.state.add_timeout(None, 5000, lambda: setattr(rendering_status, "visible", False))
                    print(f"[reload] ERROR: {exc}")
                finally:
                    loading_indicator.visible = False

        _wrapped = _reload_op.wrap(_run_reload, finish=True)
        doc = pn.state.curdoc
        if doc is not None:
            doc.add_next_tick_callback(_wrapped)
        else:
            _wrapped()

    async def _populate() -> None:
        """Fill the skeleton roots one pane at a time, in dependency order.

        The first phase performs the only store round-trip of the initial load.
        Every later phase is local work, and the pause after each phase gives
        the browser time to show the pane that was just filled.
        """
        nonlocal structure, store_selector, node_select, variable_selector
        nonlocal variable_metadata, node_hint, store_info, reload_button
        nonlocal selection_state, map_view, map_state

        with _load_op.activate():
            _populate_stage = time_stage("populate_dashboard")
            _status = "error"
            try:
                print(f"[populate] start session_id={session_id}")

                # Phase 1: opens the store; the sidebar needs its structure.
                try:
                    with time_stage("get_structure", view=view_name):
                        structure = await asyncio.to_thread(
                            data.client.get_structure, view_name
                        )
                except Exception as exc:
                    _report_error("Data structure", exc)
                    _set_root_error(controller, "Data structure", exc)
                    return
                await asyncio.sleep(_POPULATE_PAUSE)

                # Phase 2: sidebar, then the group path taken from the tree.
                try:
                    (
                        controller_content,
                        store_selector,
                        node_select,
                        variable_selector,
                        variable_metadata,
                        node_hint,
                        store_info,
                        reload_button,
                    ) = build_sidebar(
                        view_name, view, structure, views=views,
                        loading_indicator=loading_indicator,
                        timeseries_loading=timeseries_loading,
                        render_spinner=render_spinner,
                        rendering_status=rendering_status,
                        table_loading=table_loading,
                    )
                except Exception as exc:
                    _report_error("Dataset panel", exc)
                    _set_root_error(controller, "Dataset panel", exc)
                    return
                with time_stage("pane.attach", pane="controller"):
                    controller.objects = controller_content.objects
                    controller.styles = dict(controller_content.styles)
                data.group_path = node_select.value
                await asyncio.sleep(_POPULATE_PAUSE)

                # Phase 3: plot selection table (depends on data.group_path).
                try:
                    view_cfg = data.client.get_view(view_name)
                    available_dims = resolve_available_dimensions(
                        view_config=view_cfg,
                        group_path=data.group_path,
                        schema_display=view_cfg.get("schema_display", {}),
                    )
                    with time_stage("build_plot_selection_panel", view=view_name):
                        panel_table, selection_state = build_plot_selection_panel(
                            state=selection_state,
                            available_dims=available_dims,
                            plot_var_selector=plot_var_selector,
                            table_loading=table_loading,
                        )
                except Exception as exc:
                    _report_error("Plot selection", exc)
                    _set_root_error(top_right, "Plot selection", exc)
                else:
                    _set_root(top_right, panel_table, label="top_right")
                await asyncio.sleep(_POPULATE_PAUSE)

                # Phase 4: map, the heaviest read after the structure.
                try:
                    map_view, map_state = await asyncio.to_thread(
                        build_map_view, data, tap_stream
                    )
                except Exception as exc:
                    _report_error("Map view", exc)
                    map_state = {}
                    _set_root_error(top_left, "Map view", exc)
                else:
                    _set_root(top_left, _map_pane(map_view), label="top_left")
                    update_data_warnings(map_state)
                await asyncio.sleep(_POPULATE_PAUSE)

                # Phase 5: variables; assigning the value triggers on_variable_change.
                variable_selector.param.watch(on_variable_change, ["value"])
                _populate_variable_selector(view_name, data.group_path)
                await asyncio.sleep(_POPULATE_PAUSE)

                # Phase 6: timeseries panes, then the default site fetch.
                try:
                    line_left, line_mid, line_right, on_map_tap, fetch_site_entity = (
                        build_timeseries_views(
                            data,
                            map_state,
                            selection_state=selection_state,
                            render_spinner=render_spinner,
                        )
                    )
                except Exception as exc:
                    _report_error("Timeseries", exc)
                    for pane_root in (bottom_left, bottom_mid, bottom_right):
                        _set_root_error(pane_root, "Timeseries", exc)
                else:
                    map_handlers["on_map_tap"] = on_map_tap
                    map_handlers["fetch_site"] = fetch_site_entity
                    # Select the initial site before the panes exist so the
                    # overlay update cannot race the pane layout pass, which
                    # left the Plot Selection Tabulator unlaid out.
                    on_map_tap(tap_stream.x, tap_stream.y)
                    _set_root(bottom_left, _line_pane(line_left), label="bottom_left")
                    _set_root(bottom_mid, _line_pane(line_mid), label="bottom_mid")
                    _set_root(bottom_right, _line_pane(line_right), label="bottom_right")

                tap_stream.param.watch(on_tap_event, ["x", "y"])
                await asyncio.sleep(_POPULATE_PAUSE)

                # Phase 7: user actions go live only once everything is built.
                node_select.param.watch(on_node_change, ["value"])
                store_selector.param.watch(on_store_change, ["value"])
                reload_button.on_click(on_reload)

                _status = "ok"
            except Exception as exc:
                _report_error("Population", exc)
                rendering_status.object = f"⚠️ Render error: {str(exc)[:80]}"
                rendering_status.visible = True
            finally:
                _populate_stage.log(status=_status)
                _load_op.finish(status=_status, view=view_name, group=data.group_path)
                if _summary is not None:
                    end()
                print(f"[populate] finished status={_status} session_id={session_id}")

    template = """
{%% extends base %%}
{%% block contents %%}
{%% set context = '%s' %%}

{%% if context == 'notebook' %%}
    {%% set slicer_id = get_id() %%}
    <div id='{{slicer_id}}'></div>
{%% endif %%}

<style>
:host {
    width: auto;
}
</style>

<script>
var config = {
    settings: {
        hasHeaders: true,
        constrainDragToContainer: true,
        reorderEnabled: true,
        selectionEnabled: false,
        popoutWholeStack: false,
        blockedPopoutsThrowError: true,
        closePopoutsOnUnload: true,
        showPopoutIcon: false,
        showMaximiseIcon: true,
        showCloseIcon: false
    },
    content: [{
        type: 'row',
        content:[
            {
                type: 'component',
                componentName: 'view',
                    componentState: {
                    model: '{{ embed(roots.controller) }}',
                    title: 'Dataset',
                    width: 350,
                    css_classes:['scrollable']
                },
                isClosable: false,
            },
            {
                type: 'column',
                content: [
                    {
                        type: 'row',
                        content:[
                            {
                                type: 'component',
                                componentName: 'view',
                                    componentState: {
                                    model: '{{ embed(roots.top_left) }}',
                                    title: 'Map view'
                                },
                                isClosable: false,
                            },
                            {
                                type: 'component',
                                componentName: 'view',
                                    componentState: {
                                    model: '{{ embed(roots.top_right) }}',
                                    title: 'Plot Selection'
                                },
                                isClosable: false,
                            }
                        ]
                    },
                    {
                        type: 'row',
                        content:[
                            {
                                type: 'component',
                                componentName: 'view',
                                componentState: {
                                    model: '{{ embed(roots.bottom_left) }}',
                                    title: 'Time dependent, Year scale'
                                },
                                isClosable: false,
                            },
                            {
                                type: 'component',
                                componentName: 'view',
                                componentState: {
                                    model: '{{ embed(roots.bottom_mid) }}',
                                    title: 'Time dependent, Month scale'
                                },
                                isClosable: false,
                            },
                            {
                                type: 'component',
                                componentName: 'view',
                                componentState: {
                                    model: '{{ embed(roots.bottom_right) }}',
                                    title: 'Time dependent, Day scale'
                                },
                                isClosable: false,
                            }
                        ]
                    }
                ]
            }
        ]
    }]
};

{%% if context == 'notebook' %%}
    var myLayout = new GoldenLayout(config, '#' + '{{slicer_id}}');
    $('#' + '{{slicer_id}}').css({width: '100%%', height: '800px', margin: '0px'})
{%% else %%}
    var myLayout = new GoldenLayout(config);
{%% endif %%}

myLayout.registerComponent('view', function(container, componentState) {
    const {width, css_classes} = componentState;

    if (width)
        container.on('open', () => container.setSize(width, container.height));

    if (css_classes)
        css_classes.map((item) => container.getElement().addClass(item));

    container.setTitle(componentState.title);
    container.getElement().html(componentState.model);

    container.on('resize', () => window.dispatchEvent(new Event('resize')));
});

myLayout.init();
</script>
{%% endblock %%}
"""

    tmpl = pn.Template(
        template=(template % "server"),
        nb_template=(template % "notebook"),
    )

    tmpl.nb_template.globals["get_id"] = make_globally_unique_id

    tmpl.add_panel("controller", controller)
    tmpl.add_panel("top_left", top_left)
    tmpl.add_panel("top_right", top_right)
    tmpl.add_panel("bottom_left", bottom_left)
    tmpl.add_panel("bottom_mid", bottom_mid)
    tmpl.add_panel("bottom_right", bottom_right)

    _build_stage.log()
    # The contextvars tokens belong to this frame, so the scopes are closed here;
    # _populate() re-activates the operation itself and finishes it when done.
    _session_scope.__exit__(None, None, None)
    _load_scope.__exit__(None, None, None)

    pn.state.onload(_populate)
    print(f"[session] build_dashboard ready session_id={session_id}")
    return tmpl