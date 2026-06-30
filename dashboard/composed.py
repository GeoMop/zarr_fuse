import time

import holoviews as hv
import panel as pn
from bokeh.util.serialization import make_globally_unique_id
from holoviews import streams

from dashboard.config import get_default_endpoint_name, get_endpoint_config, load_endpoints, resolve_endpoints_path
from dashboard.data import load_data
from dashboard.map_views import build_map_view
from dashboard.multi_time_views import build_timeseries_views
from dashboard.plot_selection import build_plot_selection_panel, resolve_available_dimensions
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
    js_files=JS_FILES,
    css_files=CSS_FILES,
    design="material",
    theme="dark",
    sizing_mode="stretch_width",
)
hv.extension("bokeh")
hv.renderer("bokeh").theme = "dark_minimal"


def build_dashboard():
    start_total = time.perf_counter()

    endpoints_path = resolve_endpoints_path()
    print(f"Using endpoints config: {endpoints_path}")

    configured_default = get_default_endpoint_name(endpoints_path)
    endpoints = load_endpoints(endpoints_path)
    if not endpoints:
        raise ValueError(f"No endpoints configured in {endpoints_path}")

    endpoint_name = configured_default or next(iter(endpoints.keys()))

    data = load_data(
        "local",
        endpoint_name=endpoint_name,
        endpoints_path=endpoints_path,
        display_variable="",
    )

    endpoints = data.client.get_endpoints()
    endpoint = endpoints.get(endpoint_name) or data.client.get_endpoint(endpoint_name)
    structure = data.client.get_structure(endpoint_name)

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
    controller, store_selector, node_select, variable_selector, variable_metadata, node_hint, store_info = build_sidebar(
        endpoint_name, endpoint, structure, endpoints=endpoints,
        loading_indicator=loading_indicator, timeseries_loading=timeseries_loading,
        render_spinner=render_spinner, rendering_status=rendering_status,
    )
    data.group_path = node_select.value

    # ── Variable dropdown for the plot selection panel ──────────────
    plot_var_selector = pn.widgets.Select(
        name="Variable",
        options=[],
        value=None,
        width=120,
    )

    # ── Table-style plot selection ──────────────────────────────────
    endpoint_cfg = data.client.get_endpoint(endpoint_name)
    schema_display_tbl = endpoint_cfg.get("schema_display", {})

    selection_state = None  # created by build_plot_selection_panel
    available_dims = resolve_available_dimensions(
        endpoint_config=endpoint_cfg,
        group_path=data.group_path,
        schema_display=schema_display_tbl,
    )

    panel_table, selection_state = build_plot_selection_panel(
        state=selection_state,
        available_dims=available_dims,
        plot_var_selector=plot_var_selector,
    )
    # ────────────────────────────────────────────────────────────────

    tap_stream = streams.Tap(x=None, y=None)

    map_handlers = {"on_map_tap": lambda *_: None}

    map_view, map_state = build_map_view(data, tap_stream)

    _current_var_label: str | None = None

    def _populate_variable_selector(endpoint_name: str, group_path: str):
        nonlocal _current_var_label
        try:
            print(f"[variables] Loading for endpoint={endpoint_name}, group={group_path}")
            t0 = time.perf_counter()
            variables = data.client.get_variables(endpoint_name, group_path)
            print(f"[timing] get_variables: {time.perf_counter() - t0:.3f}s")
            print(f"[variables] Found: {len(variables)} variables")

            if variables:
                var_options = [f"{name} ({unit})" if unit else name for name, unit in variables.items()]

                for name, unit in variables.items():
                    print(f"[variables]   - {name}: unit={unit}")

                variable_selector.options = var_options

                endpoint_cfg = get_endpoint_config(endpoints_path, endpoint_name)
                default_var = endpoint_cfg.defaults.display_variable if endpoint_cfg.defaults else None

                if default_var and default_var in variables:
                    var_label = default_var + (f" ({variables[default_var]})" if variables[default_var] else "")
                else:
                    var_label = var_options[0] if var_options else None

                if var_label:
                    var_name = var_label.split(" (")[0] if " (" in var_label else var_label
                    _current_var_label = var_label
                    variable_metadata.visible = False
                    variable_selector.value = var_label
                    plot_var_selector.options = [var_label]
                    plot_var_selector.value = var_label
                else:
                    variable_selector.value = None
                    data.display_variable = ""
                    variable_metadata.visible = False
                    plot_var_selector.options = []

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
            plot_var_selector.value = display_label

            def _update_metadata():
                meta = data.client.get_variable_metadata(
                    data.endpoint_name, data.group_path, var_name
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

            def _run_refresh():
                try:
                    refresh_views()
                    plot_var_selector.value = display_label
                    _update_metadata()
                except Exception as e:
                    import traceback; traceback.print_exc()
                    plot_var_selector.options = []
                    rendering_status.object = f"⚠️ Render error: {str(e)[:80]}"
                    rendering_status.visible = True
                    pn.state.add_timeout(None, 5000, lambda: setattr(rendering_status, "visible", False))
                    print(f"[variables] Error viewing {var_name}: {e}")
                finally:
                    loading_indicator.visible = False

            doc = pn.state.curdoc
            if doc is not None:
                doc.add_next_tick_callback(_run_refresh)
            else:
                _run_refresh()

    def on_variable_change(event):
        selected_label = event.new
        if selected_label:
            var_name = selected_label.split(" (")[0] if " (" in selected_label else selected_label
            _select_variable(var_name, label=selected_label)

    variable_selector.param.watch(on_variable_change, ["value"])

    _populate_variable_selector(endpoint_name, data.group_path)

    def _refresh_sidebar_for_endpoint(selected_endpoint: str):
        nonlocal endpoints, endpoint, structure
        print(f"[timing] _refresh_sidebar: start for {selected_endpoint}")

        endpoints = data.client.get_endpoints()
        print(f"[timing] _refresh_sidebar: get_endpoints done")
        endpoint = endpoints.get(selected_endpoint) or data.client.get_endpoint(selected_endpoint)
        structure = data.client.get_structure(selected_endpoint)
        print(f"[timing] _refresh_sidebar: get_structure done")

        node_items = _flatten_nodes(structure)
        node_options = {label: path for label, path in node_items}
        node_select.options = node_options
        node_select.value = node_items[0][1] if node_items else "/"

        store_info.object = (
            "<div style='background: #1e293b; padding: 12px; border-radius: 8px; margin: 8px 0;"
            " border-left: 3px solid #3b82f6;'>"
            "<div style='font-size: 11px; color: #94a3b8; margin-bottom: 4px; font-weight: 600;'>"
            "STORE URI</div>"
            f"<div style='font-size: 12px; color: #e2e8f0; font-family: monospace;'>{endpoint['source']['uri']}</div>"
            "</div>"
        )
        print(f"[timing] _refresh_sidebar: about to populate variables")

        _populate_variable_selector(selected_endpoint, data.group_path)
        print(f"[timing] _refresh_sidebar: done")

    def _switch_endpoint(selected_endpoint: str):
        nonlocal data, endpoint_name

        endpoint_name = selected_endpoint
        endpoint_obj = data.client.get_endpoint(selected_endpoint)
        data.endpoint_name = selected_endpoint
        data.group_path = "/"
        data.display_variable = ""
        
        # Clear cache for new endpoint
        data.client.clear_cache()
        
        selection_state.clear()
        print(f"[timing] _switch_endpoint: starting refresh for {selected_endpoint}")
        _refresh_sidebar_for_endpoint(selected_endpoint)
        print(f"[timing] _switch_endpoint: calling refresh_views")
        refresh_views()
        print(f"[timing] _switch_endpoint: done")

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

    update_data_warnings(map_state)

    line_left, line_mid, line_right, on_map_tap = build_timeseries_views(
        data,
        map_state,
        selection_state=selection_state,
        render_spinner=render_spinner,
    )
    map_handlers["on_map_tap"] = on_map_tap

    def on_tap_event(*_):
        if timeseries_loading.visible:
            return  # debounce — already loading
        timeseries_loading.visible = True
        # Capture coordinates at tap time (avoid stale values after delay)
        _x, _y = tap_stream.x, tap_stream.y
        doc = pn.state.curdoc

        def _do_tap():
            try:
                map_handlers["on_map_tap"](_x, _y)
            finally:
                timeseries_loading.visible = False

        if doc is not None:
            # Use timeout, not next_tick — next_tick runs synchronously during
            # document unlock, batching visible=True + visible=False together
            # so the frontend never sees the spinner.  A small timeout lets the
            # UI flush visible=True before the blocking I/O starts.
            doc.add_timeout_callback(_do_tap, 100)
        else:
            _do_tap()

    tap_stream.param.watch(on_tap_event, ["x", "y"])

    top_left = pn.pane.HoloViews(map_view, sizing_mode="stretch_both")
    top_right = pn.Column(
        panel_table,
        sizing_mode="stretch_both",
    )
    bottom_left = pn.pane.HoloViews(line_left, sizing_mode="stretch_both", linked_axes=False)
    bottom_mid = pn.pane.HoloViews(line_mid, sizing_mode="stretch_both", linked_axes=False)
    bottom_right = pn.pane.HoloViews(line_right, sizing_mode="stretch_both", linked_axes=False)

    def refresh_views():
        # Save existing site indices *before* rebuilding (they will get stale data
        # replaced via force=True in _fetch_timeseries after the variable change)
        saved_indices = [s["entity_index"] for s in selection_state.sites]

        print(f"[timing] refresh_views: start building map_view")
        new_map_view, new_map_state = build_map_view(data, tap_stream)
        print(f"[timing] refresh_views: map_view done, building timeseries")
        update_data_warnings(new_map_state)

        new_line_left, new_line_mid, new_line_right, new_on_map_tap = build_timeseries_views(
            data,
            new_map_state,
            selection_state=selection_state,
            render_spinner=render_spinner,
        )
        print(f"[timing] refresh_views: timeseries done, updating panes")

        map_handlers["on_map_tap"] = new_on_map_tap
        top_left.object = new_map_view
        bottom_left.object = new_line_left
        bottom_mid.object = new_line_mid
        bottom_right.object = new_line_right

        # ── Re-fetch existing sites with the current (possibly changed) variable ──
        lats = new_map_state.get("lats", [])
        lons = new_map_state.get("lons", [])
        if saved_indices:
            timeseries_loading.visible = True
        for idx in saved_indices:
            if idx < len(lats) and idx < len(lons):
                lat = float(lats[idx])
                lon = float(lons[idx])
                new_on_map_tap(lon, lat)  # x=lon, y=lat
                print(f"[refresh_views] Re-fetched site idx={idx} at ({lat:.4f}, {lon:.4f})")
        if saved_indices:
            timeseries_loading.visible = False

        print(f"[timing] refresh_views: done")

    def on_store_change(event):
        if event.new and event.new != endpoint_name:
            loading_indicator.visible = True

            def _run_switch():
                try:
                    _switch_endpoint(event.new)
                finally:
                    loading_indicator.visible = False

            doc = pn.state.curdoc
            if doc is not None:
                doc.add_next_tick_callback(_run_switch)
            else:
                _run_switch()

    def on_node_change(event):
        if event.new:
            data.group_path = event.new
            selection_state.clear()
            loading_indicator.visible = True

            def _run_refresh():
                try:
                    _populate_variable_selector(data.endpoint_name, data.group_path)
                    refresh_views()
                finally:
                    loading_indicator.visible = False

            doc = pn.state.curdoc
            if doc is not None:
                doc.add_next_tick_callback(_run_refresh)
            else:
                _run_refresh()

    node_select.param.watch(on_node_change, ["value"])
    store_selector.param.watch(on_store_change, ["value"])

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

    print(f"[timing] build_dashboard: {time.perf_counter() - start_total:.3f}s")
    return tmpl