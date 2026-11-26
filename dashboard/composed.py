import asyncio
import logging
from collections.abc import Callable
from functools import partial
import html
import traceback

import holoviews as hv
import panel as pn
from bokeh.util.serialization import make_globally_unique_id
from holoviews import streams

from dashboard.config import (
    find_view_file,
    get_default_endpoint_name,
    load_view_config,
    load_views,
)
from dashboard.data import LocalClient, load_data
from dashboard.log_buffer import mask_secrets
from dashboard.log_view import LogView, build_logs_link
from dashboard.logs_page import LOGS_TAB_URL
from dashboard.map_views import build_map_view, fetch_map_payload
from dashboard.multi_time_views import (
    MapClickOperation,
    build_timeseries_views,
    fetch_default_site_figure,
    fetch_registered_site_payload,
)
from dashboard.perf_summary import measure_operation, stage
from dashboard.plot_selection import (
    SelectionState,
    build_plot_selection_panel,
    resolve_available_dimensions,
)
from dashboard.profiling import log_message, operation, session, time_stage
from dashboard.sidebar import _flatten_nodes, build_sidebar

logger = logging.getLogger(__name__)

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

_LOGS_MODAL_CSS = """
.dialog-content {
    background: #0b1220;
    padding: 0;
    width: min(1100px, 94vw);
    height: min(680px, 86vh);
    overflow: hidden;
    display: flex;
    flex-direction: column;
    position: relative;
}
.dialog-content > :not(.pnx-dialog-close) {
    min-height: 0;
    height: 100%;
    flex: 1 1 auto;
    min-width: 0;
}
.dialog-content .pnx-dialog-close {
    position: absolute;
    top: 6px;
    right: 8px;
    width: 1.5em;
    height: 1.5em;
    z-index: 100003;
    color: #94a3b8;
}
.dialog-content .pnx-dialog-close:hover {
    background: rgba(148, 163, 184, 0.2);
}
.zf-log-view {
    height: 100%;
    display: flex;
    flex-direction: column;
    min-width: 0;
    position: relative;
}
"""


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
    """Log a dashboard operation failure to the normal server log."""
    logger.exception("[populate] %s failed", label)


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
    session_ctx = pn.state.curdoc.session_context if pn.state.curdoc and pn.state.curdoc.session_context else None
    session_id = session_ctx.id if session_ctx else "no-session"
    # Manual scopes (no re-indentation of the whole body): every timer created
    # below inherits the session and operation tags.
    _session_scope = session(session_id)
    _session_scope.__enter__()
    _load_op = operation("dashboard.initial_load", session=session_id)
    _load_scope = _load_op.activate()
    _load_scope.__enter__()
    _build_stage = time_stage("build_dashboard")
    print(f"[session] build_dashboard started session_id={session_id}", flush=True)

    def _on_session_destroyed(session_context):
        # Idempotent: finishes an initial_load left open by a failed build.
        _load_op.finish(status="error")
        sid = session_context.id if session_context is not None else session_id
        print(f"[session] session destroyed session_id={sid}", flush=True)

    pn.state.on_session_destroyed(_on_session_destroyed)

    views_path = find_view_file()
    log_message(f"Using views config: {views_path}")

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

    # ── Server log viewer (sidebar text link + modal, no layout change) ──
    log_view = LogView(active=False)
    logs_modal = pn.Modal(log_view, stylesheets=[_LOGS_MODAL_CSS])

    def _on_logs_modal_open(event) -> None:
        log_view.set_active(bool(event.new))

    logs_modal.param.watch(_on_logs_modal_open, "open")
    logs_link = build_logs_link()
    logs_link.on_click(lambda _event: setattr(logs_modal, "open", True))

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
    map_click_state = MapClickOperation()
    variable_switch_task: asyncio.Task[list[tuple[int, dict | None]]] | None = None
    variable_switch_active = False
    _map_click_generation = 0
    _map_click_client: LocalClient | None = None

    def _map_click_generation_value() -> int:
        return _map_click_generation

    def _clear_variable_switch() -> None:
        nonlocal variable_switch_active, variable_switch_task
        variable_switch_active = False
        variable_switch_task = None
        timeseries_loading.visible = False
        loading_indicator.visible = False

    def _invalidate_requests() -> None:
        """Retire site loads before changing the request context."""
        nonlocal _map_click_generation
        _map_click_generation += 1
        if variable_switch_task is not None:
            variable_switch_task.cancel()
        if variable_switch_active:
            _clear_variable_switch()
        elif map_click_state.active:
            timeseries_loading.visible = False

    def _map_click_client_factory() -> LocalClient:
        nonlocal _map_click_client
        if _map_click_client is None:
            _map_click_client = LocalClient(data.client.views_path)
        return _map_click_client

    def _replace_map_click_client() -> None:
        nonlocal _map_click_client
        _map_click_client = None

    def _detach_map_click() -> None:
        """Release the guard while any retired worker keeps its own client."""
        _replace_map_click_client()
        map_click_state.active_generation = None

    map_state: dict = {}

    _current_var_label: str | None = None
    _suppress_variable_change = False

    def _fetch_and_show_metadata(var_name: str):
        _show_variable_metadata(data.client.get_variable_metadata(data.view_name, data.group_path, var_name))

    def _show_variable_metadata(meta: dict | None) -> None:
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
        else:
            variable_metadata.visible = False

    def _set_variable_options(variables: dict[str, str], default_var: str | None) -> str | None:
        nonlocal _current_var_label, _suppress_variable_change
        var_name = default_var if default_var in variables else next(iter(variables), None)
        if var_name is not None:
            data.display_variable = var_name
        if variable_selector is None:
            return var_name

        labels = {name: f"{name} ({unit})" if unit else name for name, unit in variables.items()}
        var_label = labels.get(var_name)
        _suppress_variable_change = True
        try:
            variable_selector.options = list(labels.values())
            if var_label is not None:
                _current_var_label = var_label
            variable_selector.value = var_label
            plot_var_selector.options = [var_label] if var_label is not None else []
            if var_label is not None:
                plot_var_selector.value = var_label
            variable_metadata.visible = False
        finally:
            _suppress_variable_change = False
        return var_name

    def _populate_variable_selector(view_name: str, group_path: str):
        try:
            log_message(f"[variables] Loading for view={view_name}, group={group_path}")
            with time_stage("get_variables", view=view_name, group=group_path):
                variables = data.client.get_variables(view_name, group_path)
            if variables:
                view_cfg = load_view_config(views_path, view_name)
                default_var = view_cfg.defaults.display_variable if view_cfg.defaults else None
                var_name = _set_variable_options(variables, default_var)
                if var_name is not None:
                    _fetch_and_show_metadata(var_name)
            else:
                _set_variable_options({}, None)
        except Exception:
            _set_variable_options({}, None)
            logger.exception("[variables] failed to load variables for view=%s group=%s", view_name, group_path)

    def _select_variable(var_name: str, label: str | None = None) -> None:
        nonlocal variable_switch_task, variable_switch_active
        if var_name == data.display_variable:
            return

        _invalidate_requests()
        if map_click_state.active:
            _detach_map_click()
        generation = _map_click_generation_value()
        request_view = data.view_name
        request_group = data.group_path
        views_path = data.client.views_path
        display_label = label or var_name
        entity_indices = [int(site["entity_index"]) for site in selection_state.sites]
        target_doc = pn.state.curdoc
        switch_op = operation("variable.switch", variable=var_name)

        log_message(f"[variables] Changing from {data.display_variable} to {var_name}")
        data.display_variable = var_name
        variable_switch_active = True
        loading_indicator.visible = True
        timeseries_loading.visible = bool(entity_indices)
        plot_var_selector.options = [display_label]
        plot_var_selector.value = display_label

        def _fetch_site(client: LocalClient, idx: int) -> tuple[int, dict | None]:
            try:
                payload = fetch_registered_site_payload(
                    client, request_view, request_group, var_name, idx
                )
            except Exception:  # noqa: BLE001 - report this site's failure and continue
                logger.exception("[variables] failed to load site idx=%s variable=%s", idx, var_name)
                payload = None
            return idx, payload

        def _fetch_metadata() -> dict:
            try:
                return data.client.get_variable_metadata(request_view, request_group, var_name) or {}
            except Exception:  # noqa: BLE001 - metadata is optional for the switch
                logger.exception("[variables] failed to load metadata for %s", var_name)
                return {}

        def _complete(
            get_results: Callable[[], tuple[list[tuple[int, dict | None]], dict]],
        ) -> None:
            status = "cancelled"
            try:
                if (
                    generation != _map_click_generation_value()
                    or request_view != data.view_name
                    or request_group != data.group_path
                    or var_name != data.display_variable
                ):
                    return

                with measure_operation("variable.switch", variable=var_name):
                    results, metadata = get_results()
                    with stage("selection.update"):
                        # Replacement plots render the batch when they are attached.
                        failed = selection_state.update_sites_data(results, notify_version=False)
                    refresh_views(rebuild_map=False, variable_metadata=metadata)
                    _show_variable_metadata(metadata)
                if failed:
                    _show_refresh_error(failed)
                status = "error" if failed else "ok"
            except asyncio.CancelledError:
                pass
            except Exception as exc:  # noqa: BLE001 - report load and render failures in the UI
                status = "error"
                _report_error(f"Viewing {var_name}", exc)
                plot_var_selector.options = []
                rendering_status.object = f"⚠️ Render error: {str(exc)[:80]}"
                rendering_status.visible = True
                if target_doc is not None:
                    target_doc.add_timeout_callback(
                        lambda: setattr(rendering_status, "visible", False), 5000
                    )
            finally:
                if generation == _map_click_generation_value():
                    _clear_variable_switch()
                switch_op.finish(status=status)

        async def _load() -> tuple[list[tuple[int, dict | None]], dict]:
            client = LocalClient(views_path)
            metadata_task = asyncio.create_task(asyncio.to_thread(_fetch_metadata))
            try:
                results = []
                for idx in entity_indices:
                    results.append(await asyncio.to_thread(_fetch_site, client, idx))
                return results, await metadata_task
            except asyncio.CancelledError:
                metadata_task.cancel()
                try:
                    await metadata_task
                except asyncio.CancelledError:
                    pass
                raise

        def _load_sync() -> tuple[list[tuple[int, dict | None]], dict]:
            client = LocalClient(views_path)
            return [_fetch_site(client, idx) for idx in entity_indices], _fetch_metadata()

        complete_sync = switch_op.wrap(partial(_complete, _load_sync))
        if target_doc is None:
            complete_sync()
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            target_doc.add_next_tick_callback(complete_sync)
        else:
            def _loaded(
                task: asyncio.Task[tuple[list[tuple[int, dict | None]], dict]],
            ) -> None:
                if generation != _map_click_generation_value():
                    if not task.cancelled():
                        task.exception()
                    switch_op.finish(status="cancelled")
                else:
                    target_doc.add_next_tick_callback(
                        switch_op.wrap(partial(_complete, task.result))
                    )

            variable_switch_task = loop.create_task(switch_op.wrap(_load)())
            variable_switch_task.add_done_callback(_loaded)

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
        failed: list[int] = []
        timeseries_loading.visible = True
        try:
            for site in list(selection_state.sites):
                idx = site["entity_index"]
                result = map_handlers["fetch_site"](idx)
                if result is None:
                    failed.append(idx)
        finally:
            timeseries_loading.visible = False
        if failed:
            _show_refresh_error(failed)

    def _show_refresh_error(failed: list[int]) -> None:
        label = plot_var_selector.value or data.display_variable
        rendering_status.object = (
            f"⚠️ {label}: data not available at {len(failed)} site(s) "
            f"({', '.join(map(str, failed))})"
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

        _invalidate_requests()
        _detach_map_click()
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
        x, y = tap_stream.x, tap_stream.y
        if x is None or y is None:
            return
        if map_click_state.active or variable_switch_active or loading_indicator.visible:
            return  # debounce — a background load is already running
        timeseries_loading.visible = True
        # The spinner is turned off by the map-click completion callback inside
        # on_map_tap once the last member's data has landed, not here.
        map_handlers["on_map_tap"](x, y, loading=timeseries_loading)

    top_left = _placeholder_root("stretch_both", "Loading map view...")
    top_right = _placeholder_root("stretch_both", "Loading plot selection...")
    bottom_left = _placeholder_root("stretch_both", "Loading year timeseries...")
    bottom_mid = _placeholder_root("stretch_both", "Loading month timeseries...")
    bottom_right = _placeholder_root("stretch_both", "Loading day timeseries...")

    def refresh_views(rebuild_map: bool = True, *, variable_metadata: dict | None = None):
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
                map_click_client=_map_click_client_factory,
                map_click_state=map_click_state,
                map_click_generation=_map_click_generation_value,
                map_click_failure=_show_refresh_error,
                variable_metadata=variable_metadata,
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
            _invalidate_requests()
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
            _invalidate_requests()
            if map_click_state.active:
                _detach_map_click()
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
                    # Invalidate map-click requests before the data changes.  A
                    # new background client prevents new clicks from reusing the
                    # old client's cache; already-running reads keep their client.
                    _invalidate_requests()
                    _replace_map_click_client()
                    # Drop cached map/timeseries payloads; the zarr nodes reopen
                    # their dataset lazily so values are re-read from the store.
                    data.client.clear_cache()
                    if data.display_variable:
                        # Refresh each site in place so the table position and the
                        # checked depths survive the reload.
                        _refetch_sites()
                    refresh_views(rebuild_map=True)
                except Exception as exc:  # noqa: BLE001 - surface any reload failure
                    logger.exception("[reload] failed")
                    rendering_status.object = f"⚠️ Reload failed: {str(exc)[:80]}"
                    rendering_status.visible = True
                    pn.state.add_timeout(None, 5000, lambda: setattr(rendering_status, "visible", False))
                finally:
                    loading_indicator.visible = False

        _wrapped = _reload_op.wrap(_run_reload, finish=True)
        doc = pn.state.curdoc
        if doc is not None:
            doc.add_next_tick_callback(_wrapped)
        else:
            _wrapped()

    async def _populate() -> None:
        """Read the initial group and populate each pane as its data becomes available."""
        nonlocal structure, store_selector, node_select, variable_selector
        nonlocal variable_metadata, node_hint, store_info, reload_button
        nonlocal selection_state, map_state

        with _load_op.activate():
            populate_stage = time_stage("populate_dashboard")
            status = "ok"
            controls = ()

            def report_failure(label: str, exc: Exception, *roots: pn.Column) -> None:
                nonlocal status
                status = "error"
                _report_error(label, exc)
                for root in roots:
                    _set_root_error(root, label, exc)

            log_message(f"[populate] start session_id={session_id}")
            try:
                try:
                    with time_stage("get_structure", view=view_name):
                        structure = await asyncio.to_thread(data.client.get_structure, view_name)
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
                    data.group_path = node_select.value or data.group_path
                    controls = (store_selector, node_select, variable_selector, reload_button)
                    for control in controls:
                        control.disabled = True
                    with time_stage("pane.attach", pane="controller"):
                        controller.objects = [*controller_content.objects, logs_link]
                        controller.styles = dict(controller_content.styles)
                except Exception as exc:  # noqa: BLE001 - keep other panes available
                    report_failure("Dataset panel", exc, controller)

                try:
                    available_dims = resolve_available_dimensions(
                        view_config=view,
                        group_path=data.group_path,
                        schema_display=view.get("schema_display", {}),
                    )
                    with time_stage("build_plot_selection_panel", view=view_name):
                        panel_table, selection_state = build_plot_selection_panel(
                            state=selection_state,
                            available_dims=available_dims,
                            plot_var_selector=plot_var_selector,
                            table_loading=table_loading,
                        )
                    _set_root(top_right, panel_table, label="top_right")
                except Exception as exc:  # noqa: BLE001 - plots can use selection without a table
                    report_failure("Plot selection", exc, top_right)
                    if selection_state is None:
                        selection_state = SelectionState()

                # The tree determines the group. A configured variable lets the
                # map read overlap discovery, using a separate store client.
                group_path = data.group_path
                configured_var = data.display_variable
                initial_view = view_name
                client = data.client
                client_path = client.views_path

                def read_map(variable: str) -> dict:
                    return fetch_map_payload(LocalClient(client_path), initial_view, group_path, variable)

                async def load_map(variable: str) -> dict | Exception:
                    try:
                        with time_stage("get_map_data", view=initial_view, group=group_path, variable=variable):
                            return await asyncio.to_thread(read_map, variable)
                    except Exception as exc:  # noqa: BLE001 - report only the selected variable's map failure
                        return exc

                async def load_metadata(variable: str) -> dict:
                    try:
                        # Metadata reads the schema file without touching the store client's caches.
                        with time_stage("get_variable_metadata", view=initial_view, group=group_path):
                            metadata = await asyncio.to_thread(
                                client.get_variable_metadata, initial_view, group_path, variable,
                            )
                        if variable_metadata is not None and variables:
                            _show_variable_metadata(metadata)
                        return metadata or {}
                    except Exception as exc:  # noqa: BLE001 - units and sidebar metadata are optional
                        report_failure("Variable metadata", exc)
                        return {}

                initial_site = None
                async with asyncio.TaskGroup() as tasks:
                    map_task = tasks.create_task(load_map(configured_var)) if configured_var else None
                    try:
                        with time_stage("get_variables", view=initial_view, group=group_path):
                            variables = await asyncio.to_thread(client.get_variables, initial_view, group_path)
                        _set_variable_options(variables, configured_var)
                    except Exception as exc:  # noqa: BLE001 - the configured variable can still be read
                        variables = {}
                        report_failure("Variables", exc)

                    variable = data.display_variable
                    metadata_task = tasks.create_task(load_metadata(variable)) if variable else None
                    if map_task is not None and variable == configured_var:
                        payload = await map_task
                    else:
                        if map_task is not None:
                            map_task.cancel()
                        payload = await load_map(variable)

                    try:
                        if isinstance(payload, Exception):
                            raise payload
                        map_view, map_state = build_map_view(data, tap_stream, map_payload=payload)
                        _set_root(top_left, _map_pane(map_view), label="top_left")
                        if node_hint is not None:
                            update_data_warnings(map_state)
                    except Exception as exc:  # noqa: BLE001 - keep the timeseries panes usable
                        report_failure("Map view", exc, top_left)

                    if variable:
                        try:
                            with time_stage("get_default_site", view=initial_view, group=group_path):
                                initial_site = await asyncio.to_thread(
                                    fetch_default_site_figure, client, initial_view, group_path,
                                    variable, map_state, view.get("defaults", {}).get("default_site"),
                                )
                        except Exception as exc:  # noqa: BLE001 - interactive plots can open without default data
                            report_failure("Default site", exc)
                            rendering_status.object = f"⚠️ Default site failed: {str(exc)[:80]}"
                            rendering_status.visible = True

                line_roots = (bottom_left, bottom_mid, bottom_right)
                try:
                    left, mid, right, on_map_tap, fetch_site_entity = build_timeseries_views(
                        data,
                        map_state,
                        selection_state=selection_state,
                        render_spinner=render_spinner,
                        map_click_client=_map_click_client_factory,
                        map_click_state=map_click_state,
                        map_click_generation=_map_click_generation_value,
                        map_click_failure=_show_refresh_error,
                        initial_site=initial_site,
                        variable_metadata=metadata_task.result() if metadata_task is not None else {},
                    )
                except Exception as exc:  # noqa: BLE001 - leave the map and table visible
                    report_failure("Timeseries", exc, *line_roots)
                else:
                    map_handlers["on_map_tap"] = on_map_tap
                    map_handlers["fetch_site"] = fetch_site_entity
                    for root, line, label in zip(line_roots, (left, mid, right), ("bottom_left", "bottom_mid", "bottom_right")):
                        try:
                            _set_root(root, _line_pane(line), label=label)
                        except Exception as exc:  # noqa: BLE001 - the other timeseries panes are independent
                            report_failure(label, exc, root)
                    tap_stream.param.watch(on_tap_event, ["x", "y"])

                if controls:
                    variable_selector.param.watch(on_variable_change, ["value"])
                    node_select.param.watch(on_node_change, ["value"])
                    store_selector.param.watch(on_store_change, ["value"])
                    reload_button.on_click(on_reload)
                    for control in controls:
                        control.disabled = False
            except asyncio.CancelledError:
                status = "cancelled"
                raise
            except Exception as exc:  # noqa: BLE001 - report unexpected population errors
                report_failure("Population", exc)
                rendering_status.object = f"⚠️ Render error: {str(exc)[:80]}"
                rendering_status.visible = True
            finally:
                populate_stage.log(status=status)
                _load_op.finish(status=status, view=view_name, group=data.group_path)
                log_message(f"[populate] finished status={status} session_id={session_id}")

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
{{ embed(roots.logs_modal) }}
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
    tmpl.add_panel("logs_modal", logs_modal)

    _build_stage.log()
    # The contextvars tokens belong to this frame, so the scopes are closed here;
    # _populate() re-activates the operation itself and finishes it when done.
    _session_scope.__exit__(None, None, None)
    _load_scope.__exit__(None, None, None)

    pn.state.onload(_populate)
    log_message(f"[session] build_dashboard ready session_id={session_id}")
    return tmpl


def _build_crash_page(tb: str) -> pn.Column:
    """Render the initial-load failure page shown instead of the dashboard."""
    trace = html.escape(mask_secrets(tb))
    return pn.Column(
        pn.pane.Alert(
            "Dashboard failed to load. The full server log is available at /logs.",
            alert_type="danger",
            sizing_mode="stretch_width",
        ),
        pn.pane.HTML(
            "<pre style='margin:0;padding:12px;border-radius:8px;"
            "background:#111827;color:#fca5a5;overflow:auto;"
            "white-space:pre-wrap;word-break:break-word;max-height:70vh;'>"
            f"{trace}</pre>",
            sizing_mode="stretch_both",
        ),
        pn.pane.HTML(
            f'<a href="{LOGS_TAB_URL}" target="_blank" rel="noopener" '
            'style="color:#93c5fd;font-size:13px;">'
            "Open full server logs &#8594;</a>",
            sizing_mode="stretch_width",
        ),
        sizing_mode="stretch_both",
        styles={
            "background": "#0b1220",
            "padding": "16px",
            "color": "#cbd5e1",
        },
    )


def build_dashboard_or_error():
    """Build the dashboard, returning a readable crash page if bootstrap fails."""
    try:
        return build_dashboard()
    except Exception:  # noqa: BLE001 - the crash page is the fallback
        traceback.print_exc()
        print("[session] build_dashboard failed; returning crash page")
        return _build_crash_page(traceback.format_exc())
