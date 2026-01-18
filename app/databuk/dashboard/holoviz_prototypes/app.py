import panel as pn
import holoviews as hv
import geoviews as gv
import numpy as np
import pandas as pd
from holoviews import streams
from bokeh.util.serialization import make_globally_unique_id
from geoviews import tile_sources as gvts

from mock_data import generate_timeseries_data, generate_geographic_data, get_overlay_bounds

js_files = {
    'jquery': 'https://code.jquery.com/jquery-1.11.1.min.js',
    'goldenlayout': 'https://golden-layout.com/files/latest/js/goldenlayout.min.js'
}
css_files = [
    'https://golden-layout.com/files/latest/css/goldenlayout-base.css',
    'https://golden-layout.com/files/latest/css/goldenlayout-dark-theme.css'
]

pn.extension('bokeh', js_files=js_files, css_files=css_files, design='material', theme='dark', sizing_mode="stretch_width")
hv.extension('bokeh')
hv.renderer('bokeh').theme = 'dark_minimal'

# Sidebar Controls (enhanced UI)
header = pn.pane.HTML("""
<div style="background: linear-gradient(135deg, #2563eb 0%, #4f46e5 100%);
            padding: 12px 15px; border-radius: 12px; box-shadow: 0 4px 6px rgba(0,0,0,0.3);
            margin-bottom: 15px;">
    <div style="display: flex; align-items: center; gap: 10px;">
        <div style="background: rgba(255,255,255,0.2); padding: 8px; border-radius: 8px;">
            <span style="font-size: 20px;">🗄️</span>
        </div>
        <div>
            <h2 style="margin: 0; font-size: 16px; font-weight: 700; color: white;">ZARR FUSE</h2>
            <p style="margin: 0; font-size: 11px; color: #bfdbfe;">Data Platform</p>
        </div>
    </div>
</div>
""", sizing_mode='stretch_width')

store_selector = pn.widgets.Select(
    name='📦 Store Name', 
    value='Mock Store A', 
    options=['Mock Store A', 'Mock Store B', 'Mock Store C'],
    width=320,
    stylesheets=["""
    select {
        background-color: #1e293b !important;
        color: #e2e8f0 !important;
        border: 1px solid #475569 !important;
        border-radius: 6px !important;
        padding: 8px !important;
        font-weight: 500 !important;
    }
    select option {
        background-color: #1e293b !important;
        color: #e2e8f0 !important;
    }
    """]
)

store_info = pn.pane.HTML("""
<div style="background: #1e293b; padding: 12px; border-radius: 8px; margin: 8px 0;
            border-left: 3px solid #3b82f6;">
    <div style="font-size: 11px; color: #94a3b8; margin-bottom: 4px; font-weight: 600;">
        STORE URL
    </div>
    <div style="font-size: 12px; color: #e2e8f0; font-family: monospace;">
        s3://mock-bucket/path/to/store.zarr
    </div>
</div>
""", sizing_mode='stretch_width')

status_section = pn.pane.HTML("""
<div style="background: #0f172a; padding: 12px; border-radius: 8px; margin: 8px 0;">
    <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 8px;">
        <span style="font-size: 12px; color: #94a3b8; font-weight: 600;">SERVICE STATUS</span>
        <div style="display: flex; align-items: center; gap: 6px;">
            <span style="width: 8px; height: 8px; background: #10b981; border-radius: 50%;
                        box-shadow: 0 0 10px #10b981;"></span>
            <span style="font-size: 11px; color: #10b981; font-weight: 600;">Active</span>
        </div>
    </div>
    <div style="font-size: 10px; color: #64748b; display: flex; align-items: center; gap: 4px;">
        <span>🕐</span>
        <span>Updated: """ + pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S') + """</span>
    </div>
</div>
""", sizing_mode='stretch_width')

reload_button = pn.widgets.Button(
    name='🔄 Reload Data',
    button_type='primary',
    width=320,
    height=40,
    styles={
        'background': '#3b82f6',
        'font-weight': '600',
        'font-size': '14px',
        'border-radius': '8px'
    }
)

# Tree view with Panel Accordions for expandable nodes
tree_header = pn.pane.Markdown(
    "### 📁 DATA STRUCTURE",
    styles={
        'background': '#1e293b',
        'padding': '12px 14px 8px 14px',
        'margin': '12px 0 0 0',
        'border-radius': '8px 8px 0 0',
        'border-bottom': '1px solid #334155',
        'font-size': '13px',
        'color': '#f1f5f9'
    }
)

# Create leaf buttons
temp_children = pn.Column(
    pn.widgets.Button(name='📊 surface_temp', button_type='default', width=280,
                      styles={'background': 'transparent', 'border': '1px solid #475569', 
                             'color': '#94a3b8', 'font-size': '11px', 'text-align': 'left'}),
    pn.widgets.Button(name='📊 air_temp', button_type='default', width=280,
                      styles={'background': 'transparent', 'border': '1px solid #475569',
                             'color': '#94a3b8', 'font-size': '11px', 'text-align': 'left'}),
    margin=(4, 0, 4, 20)
)

pressure_children = pn.Column(
    pn.widgets.Button(name='📊 sea_level', button_type='default', width=280,
                      styles={'background': 'transparent', 'border': '1px solid #475569',
                             'color': '#94a3b8', 'font-size': '11px', 'text-align': 'left'}),
    margin=(4, 0, 4, 20)
)

wind_children = pn.Column(
    pn.widgets.Button(name='📊 u_component', button_type='default', width=280,
                      styles={'background': 'transparent', 'border': '1px solid #475569',
                             'color': '#94a3b8', 'font-size': '11px', 'text-align': 'left'}),
    pn.widgets.Button(name='📊 v_component', button_type='default', width=280,
                      styles={'background': 'transparent', 'border': '1px solid #475569',
                             'color': '#94a3b8', 'font-size': '11px', 'text-align': 'left'}),
    margin=(4, 0, 4, 20)
)

# Create accordion
tree_accordion = pn.Accordion(
    ('📂 temperature', temp_children),
    ('📂 pressure', pressure_children),
    ('📂 wind', wind_children),
    active=[],
    toggle=True,
    styles={
        'background': '#1e293b',
        'border-radius': '0 0 8px 8px',
        'padding': '0 14px 14px 14px',
        'margin': '0'
    },
    stylesheets=["""
    .accordion-header {
        background: #334155 !important;
        color: #fbbf24 !important;
        padding: 6px 10px !important;
        border-radius: 6px !important;
        font-weight: 600 !important;
        font-size: 11px !important;
        margin-bottom: 6px !important;
    }
    .accordion-header:hover {
        background: #475569 !important;
    }
    """]
)

tree_view = pn.Column(
    tree_header,
    tree_accordion,
    sizing_mode='stretch_width',
    margin=0
)

# Assemble controller panel
controller = pn.Column(
    header,
    store_selector,
    store_info,
    status_section,
    reload_button,
    tree_view,
    pn.layout.VSpacer(),
    sizing_mode='stretch_width',
    styles={'padding': '10px'}
)

# Load mock data
df = generate_timeseries_data()

# Create linked plots
selection = streams.Selection1D()

# Scatter plot: X vs Y
scatter = hv.Scatter(df, kdims=['x', 'y'], vdims=['temperature']).opts(
    color='temperature', cmap='plasma', size=12, alpha=0.8, 
    line_color='white', line_width=1,
    tools=['tap', 'hover'],
    colorbar=True, width=600, height=400, title='X vs Y (Click to select)',
    responsive=True
)

# Line plot: Temperature over time
line = hv.Curve(df, 'time', 'temperature').opts(
    color='cyan', line_width=2, tools=['hover'],
    width=600, height=400, title='Temperature over Time',
    responsive=True
)

# Dynamic line plot that highlights selected points
def selected_points(index):
    if not index:
        # Return overlay with empty points layer
        empty_points = hv.Points([], ['time', 'temperature']).opts(
            color='red', size=15, marker='o', line_color='white', line_width=2
        )
        return line * empty_points
    selected_df = df.iloc[index]
    points = hv.Points(selected_df, ['time', 'temperature']).opts(
        color='red', size=15, marker='o', line_color='white', line_width=2
    )
    return line * points

dynamic_line = hv.DynamicMap(selected_points, streams=[selection])

# Connect selection stream to scatter plot
selection.source = scatter

# Load mock geographic data
map_df = generate_geographic_data()
overlay_bounds = get_overlay_bounds()

# Layer 1: Background map (OpenStreetMap tiles)
base_map = gvts.OSM()

# Layer 2: Mock overlay (semi-transparent rectangle as example)
overlay = gv.Rectangles([overlay_bounds]).opts(
    alpha=0.2, color='orange', line_width=2, line_color='red'
)

# Layer 3: Scatter points on map
map_points = gv.Points(map_df, kdims=['lon', 'lat'], vdims=['value']).opts(
    color='value', cmap='viridis', size=10, alpha=0.8,
    line_color='white', line_width=1.5,
    tools=['hover'], colorbar=True,
    width=600, height=400, title='Geographic Data View'
)

# Combine all layers
map_view = base_map * overlay * map_points

# Placeholder panes
top_left = pn.pane.HoloViews(scatter, sizing_mode='stretch_both')
top_right = pn.pane.HoloViews(dynamic_line, sizing_mode='stretch_both')
bottom_left = pn.pane.HoloViews(map_view, sizing_mode='stretch_both')
bottom_right = pn.pane.Markdown("## Bottom Right View\n\nPlaceholder for additional view", sizing_mode='stretch_both')

# Set up template
template = """
{%% extends base %%}
<!-- goes in body -->
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
                componentState: { model: '{{ embed(roots.controller) }}',
                                  title: 'Controls',
                                  width: 350,
                                  css_classes:['scrollable']},
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
                                componentState: { model: '{{ embed(roots.top_left) }}', title: 'Top Left'},
                                isClosable: false,
                            },
                            {
                                type: 'component',
                                componentName: 'view',
                                componentState: { model: '{{ embed(roots.top_right) }}', title: 'Top Right'},
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
                                componentState: { model: '{{ embed(roots.bottom_left) }}', title: 'Bottom Left'},
                                isClosable: false,
                            },
                            {
                                type: 'component',
                                componentName: 'view',
                                componentState: { model: '{{ embed(roots.bottom_right) }}', title: 'Bottom Right'},
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
    var myLayout = new GoldenLayout( config, '#' + '{{slicer_id}}' );
    $('#' + '{{slicer_id}}').css({width: '100%%', height: '800px', margin: '0px'})
{%% else %%}
    var myLayout = new GoldenLayout( config );
{%% endif %%}

myLayout.registerComponent('view', function( container, componentState ){
    const {width, css_classes} = componentState
    if(width)
      container.on('open', () => container.setSize(width, container.height))
    if (css_classes)
      css_classes.map((item) => container.getElement().addClass(item))
    container.setTitle(componentState.title)
    container.getElement().html(componentState.model);
    container.on('resize', () => window.dispatchEvent(new Event('resize')))
});

myLayout.init();
</script>
{%% endblock %%}
"""


tmpl = pn.Template(template=(template % 'server'), nb_template=(template % 'notebook'))
tmpl.nb_template.globals['get_id'] = make_globally_unique_id

tmpl.add_panel('controller', controller)
tmpl.add_panel('top_left', top_left)
tmpl.add_panel('top_right', top_right)
tmpl.add_panel('bottom_left', bottom_left)
tmpl.add_panel('bottom_right', bottom_right)

tmpl.servable(title='HoloViz Prototypes')
