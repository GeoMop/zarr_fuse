import os
import time

import holoviews as hv
import panel as pn
from bokeh.util.serialization import make_globally_unique_id
from holoviews import streams

from data import load_data
from map_views import build_map_view
from multi_time_views import build_timeseries_views
from ui import build_depth_controls, build_sidebar


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
    endpoint_name = os.getenv("HV_DASHBOARD_ENDPOINT")

    if not endpoint_name:
        raise ValueError("HV_DASHBOARD_ENDPOINT is required")

    data = load_data(
        "local",
        endpoint_name=endpoint_name,
    )

    endpoints = data.client.get_endpoints()
    endpoint = endpoints.get(endpoint_name) or data.client.get_endpoint(endpoint_name)
    structure = data.client.get_structure(endpoint_name)

    controller, node_select = build_sidebar(
        endpoint_name, endpoint, structure, endpoints=endpoints
    )
    depth_selector, borehole_info = build_depth_controls()

    tap_stream = streams.Tap(x=None, y=None)
    borehole_stream = streams.Stream.define("Borehole", borehole_index=0)()
    borehole_stream.event(borehole_index=0)

    map_handlers = {"on_map_tap": lambda *_: None}

    map_view, map_state = build_map_view(data, tap_stream)
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
    top_right = pn.Column(
        borehole_info,
        depth_selector,
        sizing_mode="stretch_both",
    )
    bottom_left = pn.pane.HoloViews(line_left, sizing_mode="stretch_both")
    bottom_mid = pn.pane.HoloViews(line_mid, sizing_mode="stretch_both")
    bottom_right = pn.pane.HoloViews(line_right, sizing_mode="stretch_both")

    def refresh_views():
        new_map_view, new_map_state = build_map_view(data, tap_stream)
        new_line_left, new_line_mid, new_line_right, new_on_map_tap = build_timeseries_views(
            data,
            depth_selector,
            borehole_info,
            borehole_stream,
            new_map_state,
        )
        map_handlers["on_map_tap"] = new_on_map_tap
        top_left.object = new_map_view
        bottom_left.object = new_line_left
        bottom_mid.object = new_line_mid
        bottom_right.object = new_line_right

    def on_node_change(event):
        if event.new:
            data.group_path = event.new
            refresh_views()

    node_select.param.watch(on_node_change, ["value"])

    template = """
{%% extends base %%}
{%% block contents %%}
{%% set context = '%s' %%}

<!-- Notebook-specific container -->
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
// GoldenLayout configuration
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
            // Left sidebar: Controls
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
            // Right section: 2x2 grid
            {
                type: 'column',
                content: [
                    // Top row: Map + Depths
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
                    // Bottom row: Three synchronized views
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

// Initialize GoldenLayout
{%% if context == 'notebook' %%}
    var myLayout = new GoldenLayout( config, '#' + '{{slicer_id}}' );
    $('#' + '{{slicer_id}}').css({width: '100%%', height: '800px', margin: '0px'})
{%% else %%}
    var myLayout = new GoldenLayout( config );
{%% endif %%}

// Register component handler
myLayout.registerComponent('view', function( container, componentState ){
    const {width, css_classes} = componentState

    // Set initial width if specified
    if (width)
      container.on('open', () => container.setSize(width, container.height))

    // Apply CSS classes
    if (css_classes)
      css_classes.map((item) => container.getElement().addClass(item))

    // Set title and inject Panel model
    container.setTitle(componentState.title)
    container.getElement().html(componentState.model);

    // Trigger resize event for responsive plots
    container.on('resize', () => window.dispatchEvent(new Event('resize')))
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