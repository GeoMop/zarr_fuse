import panel as pn
import holoviews as hv
import geoviews as gv
import numpy as np
import pandas as pd
from holoviews import streams
from bokeh.util.serialization import make_globally_unique_id
from geoviews import tile_sources as gvts

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

# Declare Panel components
controller = pn.Column(
    pn.widgets.Select(name='Dataset', value='Mock A', options={'Mock A': 'data_a', 'Mock B': 'data_b'}),
    pn.widgets.Toggle(name='Toggle Option', value=False),
    pn.widgets.IntSlider(name='Slider', start=1, end=10, value=5),
    pn.panel("This app demos **advanced layout** using [Panel](https://panel.holoviz.org/) and [GoldenLayout](https://golden-layout.com/).", margin=(5,15)),
    pn.layout.VSpacer(),
)

# Generate mock time-series data
np.random.seed(42)
times = pd.date_range('2024-01-01', periods=100, freq='h')
x_vals = np.cumsum(np.random.randn(100)) + 10
y_vals = np.cumsum(np.random.randn(100)) + 20
temperature = 15 + 5 * np.sin(np.linspace(0, 4*np.pi, 100)) + np.random.randn(100)

df = pd.DataFrame({
    'time': times,
    'x': x_vals,
    'y': y_vals,
    'temperature': temperature
})

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

# Create geographic map with multiple layers
# Generate mock geographic data (locations in Europe)
np.random.seed(123)
map_lons = np.random.uniform(10, 20, 30)  # Longitude range
map_lats = np.random.uniform(45, 55, 30)  # Latitude range
map_values = np.random.uniform(0, 100, 30)  # Some measurement values

map_df = pd.DataFrame({
    'lon': map_lons,
    'lat': map_lats,
    'value': map_values
})

# Layer 1: Background map (OpenStreetMap tiles)
base_map = gvts.OSM()

# Layer 2: Mock overlay (semi-transparent rectangle as example)
overlay_bounds = (12, 47, 18, 53)  # (lon_min, lat_min, lon_max, lat_max)
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
