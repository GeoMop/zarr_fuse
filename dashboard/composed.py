import time

import holoviews as hv
import panel as pn
from bokeh.util.serialization import make_globally_unique_id
from dotenv import load_dotenv
from holoviews import streams

from dashboard.config import get_default_endpoint_name, get_endpoint_config, load_endpoints, resolve_endpoints_path
from dashboard.data import load_data
from dashboard.map_views import build_map_view
from dashboard.multi_time_views import build_timeseries_views
from dashboard.sidebar import _flatten_nodes, build_depth_controls, build_sidebar

# Load environment variables from .env file if present
load_dotenv()

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

    endpoint_config = get_endpoint_config(endpoints_path, endpoint_name)
    default_display_variable = endpoint_config.defaults.display_variable

    data = load_data(
        "local",
        endpoint_name=endpoint_name,
        endpoints_path=endpoints_path,
        display_variable=default_display_variable,
    )

    endpoints = data.client.get_endpoints()
    endpoint = endpoints.get(endpoint_name) or data.client.get_endpoint(endpoint_name)
    structure = data.client.get_structure(endpoint_name)

    controller, store_selector, node_select, variable_selector, variable_info, node_hint, store_info = build_sidebar(
        endpoint_name, endpoint, structure, endpoints=endpoints
    )
    depth_selector, borehole_info = build_depth_controls()

    tap_stream = streams.Tap(x=None, y=None)
    borehole_stream = streams.Stream.define("Borehole", borehole_index=0)()
    borehole_stream.event(borehole_index=0)

    map_handlers = {"on_map_tap": lambda *_: None}

    map_view, map_state = build_map_view(data, tap_stream)

    def _populate_variable_selector(endpoint_name: str, group_path: str):
        try:
            print(f"[variables] Loading for endpoint={endpoint_name}, group={group_path}")
            t0 = time.perf_counter()
            variables = data.client.get_variables(endpoint_name, group_path)
            print(f"[timing] get_variables: {time.perf_counter() - t0:.3f}s")
            print(f"[variables] Found: {len(variables)} variables")
            
            if variables:
                var_options = [f"{name} ({unit})" if unit else name for name, unit in variables.items()]
                variable_selector.options = var_options
                
                # Log each variable
                for name, unit in variables.items():
                    print(f"[variables]   - {name}: unit={unit}")
                
                current_var = data.client.get_endpoint(endpoint_name).get("defaults", {}).get("display_variable")
                if current_var:
                    # Find matching label
                    for i, label in enumerate(var_options):
                        if label.startswith(current_var):
                            variable_selector.value = label
                            print(f"[variables] Selected default: {current_var}")
                            break
                    else:
                        variable_selector.value = var_options[0]
                        print(f"[variables] Using first: {var_options[0]}")
                else:
                    variable_selector.value = var_options[0]
                    
                # Update info text
                variable_info.object = f"**{len(variables)} variables available**\nClick to select"
                print(f"[variables] Loaded {len(variables)} variables successfully")
            else:
                variable_selector.options = []
                variable_info.object = "⚠️ No variables found"
                print(f"[variables] No variables in group {group_path}")
        except Exception as e:
            variable_selector.options = []
            variable_info.object = f"❌ Error: {str(e)[:50]}"
            print(f"[variables] ERROR: {e}")

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
        data.display_variable = endpoint_obj.get("defaults", {}).get("display_variable") or ""
        
        # Clear cache for new endpoint
        data.client.clear_cache()
        
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
        depth_selector,
        borehole_info,
        borehole_stream,
        map_state,
    )
    map_handlers["on_map_tap"] = on_map_tap

    def on_tap_event(*_):
        map_handlers["on_map_tap"](tap_stream.x, tap_stream.y)

    tap_stream.param.watch(on_tap_event, ["x", "y"])
    map_handlers["on_map_tap"](tap_stream.x, tap_stream.y)

    top_left = pn.pane.HoloViews(map_view, sizing_mode="stretch_both")
    loading_indicator = pn.Row(
        pn.indicators.LoadingSpinner(value=True, width=24, height=24),
        pn.pane.Markdown("Loading selected dataset...", styles={"color": "#dbeafe"}),
        visible=False,
        sizing_mode="stretch_width",
    )
    top_right = pn.Column(
        loading_indicator,
        variable_info,
        borehole_info,
        depth_selector,
        sizing_mode="stretch_both",
    )
    bottom_left = pn.pane.HoloViews(line_left, sizing_mode="stretch_both")
    bottom_mid = pn.pane.HoloViews(line_mid, sizing_mode="stretch_both")
    bottom_right = pn.pane.HoloViews(line_right, sizing_mode="stretch_both")

    def refresh_views():
        print(f"[timing] refresh_views: start building map_view")
        new_map_view, new_map_state = build_map_view(data, tap_stream)
        print(f"[timing] refresh_views: map_view done, building timeseries")
        update_data_warnings(new_map_state)

        new_line_left, new_line_mid, new_line_right, new_on_map_tap = build_timeseries_views(
            data,
            depth_selector,
            borehole_info,
            borehole_stream,
            new_map_state,
        )
        print(f"[timing] refresh_views: timeseries done, updating panes")

        map_handlers["on_map_tap"] = new_on_map_tap
        top_left.object = new_map_view
        bottom_left.object = new_line_left
        bottom_mid.object = new_line_mid
        bottom_right.object = new_line_right
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

    def on_variable_change(event):
        selected_label = event.new
        if selected_label:
            # Extract variable name from label (strip unit part like " (degC)")
            var_name = selected_label.split(" (")[0] if " (" in selected_label else selected_label
            
            if var_name != data.display_variable:
                print(f"[variables] Changing from {data.display_variable} to {var_name}")
                data.display_variable = var_name
                variable_info.object = f"**Loading {var_name}...**"
                loading_indicator.visible = True

                def _run_refresh():
                    try:
                        refresh_views()
                        variable_info.object = f"**Viewing: {var_name}**"
                    except Exception as e:
                        variable_info.object = f"❌ Error: {str(e)[:50]}"
                        print(f"[variables] Error viewing {var_name}: {e}")
                    finally:
                        loading_indicator.visible = False

                doc = pn.state.curdoc
                if doc is not None:
                    doc.add_next_tick_callback(_run_refresh)
                else:
                    _run_refresh()

    node_select.param.watch(on_node_change, ["value"])
    store_selector.param.watch(on_store_change, ["value"])
    variable_selector.param.watch(on_variable_change, ["value"])

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
                    title: 'Controls',
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
                                    title: 'Top Left'
                                },
                                isClosable: false,
                            },
                            {
                                type: 'component',
                                componentName: 'view',
                                componentState: {
                                    model: '{{ embed(roots.top_right) }}',
                                    title: 'Top Right'
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
                                    title: 'Bottom Left'
                                },
                                isClosable: false,
                            },
                            {
                                type: 'component',
                                componentName: 'view',
                                componentState: {
                                    model: '{{ embed(roots.bottom_mid) }}',
                                    title: 'Bottom Mid'
                                },
                                isClosable: false,
                            },
                            {
                                type: 'component',
                                componentName: 'view',
                                componentState: {
                                    model: '{{ embed(roots.bottom_right) }}',
                                    title: 'Bottom Right'
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