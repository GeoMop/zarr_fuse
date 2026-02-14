"""
HoloViz Dashboard Prototype

A Panel-based dashboard demonstrating:
- GoldenLayout for resizable panes
- Linked interactive visualizations (scatter + line plots)
- Multi-layer geographic maps with GeoViews
- Dashboard-style control panel
"""

import panel as pn
import holoviews as hv
import geoviews as gv
import pandas as pd
from holoviews import streams
from bokeh.util.serialization import make_globally_unique_id
from geoviews import tile_sources as gvts

from mock_data import generate_timeseries_data, generate_geographic_data, get_overlay_bounds

# ============================================================================
# CONFIGURATION
# ============================================================================

# GoldenLayout CDN resources
JS_FILES = {
    'jquery': 'https://code.jquery.com/jquery-1.11.1.min.js',
    'goldenlayout': 'https://golden-layout.com/files/latest/js/goldenlayout.min.js'
}
CSS_FILES = [
    'https://golden-layout.com/files/latest/css/goldenlayout-base.css',
    'https://golden-layout.com/files/latest/css/goldenlayout-dark-theme.css'
]

# Panel and HoloViews configuration
pn.extension('bokeh', js_files=JS_FILES, css_files=CSS_FILES, 
             design='material', theme='dark', sizing_mode="stretch_width")
hv.extension('bokeh')
hv.renderer('bokeh').theme = 'dark_minimal'

# ============================================================================
# CONTROL PANEL COMPONENTS
# ============================================================================
# ============================================================================
# CONTROL PANEL COMPONENTS
# ============================================================================

# Header with gradient background
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

# Store selector dropdown with custom styling
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

# Store information card
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

# Service status indicator with timestamp
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

# Reload button
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

# Hierarchical tree view with clickable nodes
tree_view = pn.pane.HTML("""
<div style="background: #1e293b; padding: 14px; border-radius: 8px; margin-top: 12px;">
    <div style="font-size: 13px; color: #f1f5f9; font-weight: 600; margin-bottom: 12px;
                border-bottom: 1px solid #334155; padding-bottom: 8px;">
        📁 DATA STRUCTURE
    </div>
    <div style="font-size: 11px; color: #cbd5e1;">
        <!-- Temperature group -->
        <div style="margin-bottom: 6px;">
            <button style="background: #334155; border: none; color: #fbbf24; padding: 6px 10px;
                          border-radius: 6px; cursor: pointer; font-weight: 600; width: 100%;
                          text-align: left; font-size: 11px; transition: all 0.2s;"
                    onmouseover="this.style.background='#475569'"
                    onmouseout="this.style.background='#334155'">
                📂 temperature
            </button>
            <div style="margin-left: 20px; margin-top: 4px;">
                <button style="background: transparent; border: 1px solid #475569; color: #94a3b8;
                              padding: 4px 8px; border-radius: 4px; cursor: pointer; width: 100%;
                              text-align: left; font-size: 11px; margin-bottom: 3px; transition: all 0.2s;"
                        onmouseover="this.style.background='#334155'; this.style.borderColor='#64748b'"
                        onmouseout="this.style.background='transparent'; this.style.borderColor='#475569'">
                    📊 surface_temp
                </button>
                <button style="background: transparent; border: 1px solid #475569; color: #94a3b8;
                              padding: 4px 8px; border-radius: 4px; cursor: pointer; width: 100%;
                              text-align: left; font-size: 11px; margin-bottom: 3px; transition: all 0.2s;"
                        onmouseover="this.style.background='#334155'; this.style.borderColor='#64748b'"
                        onmouseout="this.style.background='transparent'; this.style.borderColor='#475569'">
                    📊 air_temp
                </button>
            </div>
        </div>
        <!-- Pressure group -->
        <div style="margin-bottom: 6px;">
            <button style="background: #334155; border: none; color: #fbbf24; padding: 6px 10px;
                          border-radius: 6px; cursor: pointer; font-weight: 600; width: 100%;
                          text-align: left; font-size: 11px; transition: all 0.2s;"
                    onmouseover="this.style.background='#475569'"
                    onmouseout="this.style.background='#334155'">
                📂 pressure
            </button>
            <div style="margin-left: 20px; margin-top: 4px;">
                <button style="background: transparent; border: 1px solid #475569; color: #94a3b8;
                              padding: 4px 8px; border-radius: 4px; cursor: pointer; width: 100%;
                              text-align: left; font-size: 11px; margin-bottom: 3px; transition: all 0.2s;"
                        onmouseover="this.style.background='#334155'; this.style.borderColor='#64748b'"
                        onmouseout="this.style.background='transparent'; this.style.borderColor='#475569'">
                    📊 sea_level
                </button>
            </div>
        </div>
        <!-- Wind group -->
        <div>
            <button style="background: #334155; border: none; color: #fbbf24; padding: 6px 10px;
                          border-radius: 6px; cursor: pointer; font-weight: 600; width: 100%;
                          text-align: left; font-size: 11px; transition: all 0.2s;"
                    onmouseover="this.style.background='#475569'"
                    onmouseout="this.style.background='#334155'">
                📂 wind
            </button>
            <div style="margin-left: 20px; margin-top: 4px;">
                <button style="background: transparent; border: 1px solid #475569; color: #94a3b8;
                              padding: 4px 8px; border-radius: 4px; cursor: pointer; width: 100%;
                              text-align: left; font-size: 11px; margin-bottom: 3px; transition: all 0.2s;"
                        onmouseover="this.style.background='#334155'; this.style.borderColor='#64748b'"
                        onmouseout="this.style.background='transparent'; this.style.borderColor='#475569'">
                    📊 u_component
                </button>
                <button style="background: transparent; border: 1px solid #475569; color: #94a3b8;
                              padding: 4px 8px; border-radius: 4px; cursor: pointer; width: 100%;
                              text-align: left; font-size: 11px; transition: all 0.2s;"
                        onmouseover="this.style.background='#334155'; this.style.borderColor='#64748b'"
                        onmouseout="this.style.background='transparent'; this.style.borderColor='#475569'">
                    📊 v_component
                </button>
            </div>
        </div>
    </div>
</div>
""", sizing_mode='stretch_width')

# Assemble all components into control panel
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

# ============================================================================
# DATA PREPARATION
# ============================================================================

# Load mock time-series data
df = generate_timeseries_data()

# Load mock geographic data  
map_df = generate_geographic_data()
overlay_bounds = get_overlay_bounds()

# ============================================================================
# INTERACTIVE VISUALIZATIONS
# ============================================================================

# Selection stream for linking plots
selection = streams.Selection1D()

# ============================================================================
# INTERACTIVE VISUALIZATIONS
# ============================================================================

# Selection stream for linking plots
selection = streams.Selection1D()

# --- Scatter Plot: X vs Y colored by temperature ---
scatter = hv.Scatter(df, kdims=['x', 'y'], vdims=['temperature']).opts(
    color='temperature', 
    cmap='plasma', 
    size=12, 
    alpha=0.8, 
    line_color='white', 
    line_width=1,
    tools=['tap', 'hover'],
    colorbar=True, 
    width=600, 
    height=400, 
    title='X vs Y (Click to select)',
    responsive=True
)

# --- Line Plot: Temperature over time ---
line = hv.Curve(df, 'time', 'temperature').opts(
    color='cyan', 
    line_width=2, 
    tools=['hover'],
    width=600, 
    height=400, 
    title='Temperature over Time',
    responsive=True
)

# --- Dynamic overlay: Highlight selected points on line plot ---
def create_selection_overlay(index):
    """
    Create an overlay showing selected points on the line plot.
    
    Args:
        index: List of selected point indices from scatter plot
        
    Returns:
        Overlay of line plot with red markers at selected points
    """
    if not index:
        # Return empty overlay when no selection
        empty_points = hv.Points([], ['time', 'temperature']).opts(
            color='red', size=15, marker='o', line_color='white', line_width=2
        )
        return line * empty_points
    
    # Show red markers at selected indices
    selected_df = df.iloc[index]
    points = hv.Points(selected_df, ['time', 'temperature']).opts(
        color='red', size=15, marker='o', line_color='white', line_width=2
    )
    return line * points

# Create dynamic map bound to selection stream
dynamic_line = hv.DynamicMap(create_selection_overlay, streams=[selection])

# Link selection stream to scatter plot
selection.source = scatter

# ============================================================================
# GEOGRAPHIC MAP
# ============================================================================

# --- Layer 1: Base map tiles ---
base_map = gvts.OSM()

# --- Layer 2: Overlay region ---
overlay = gv.Rectangles([overlay_bounds]).opts(
    alpha=0.2, 
    color='orange', 
    line_width=2, 
    line_color='red'
)

# --- Layer 3: Scatter points ---
map_points = gv.Points(map_df, kdims=['lon', 'lat'], vdims=['value']).opts(
    color='value', 
    cmap='viridis', 
    size=10, 
    alpha=0.8,
    line_color='white', 
    line_width=1.5,
    tools=['hover'], 
    colorbar=True,
    width=600, 
    height=400, 
    title='Geographic Data View'
)

# Combine all map layers
map_view = base_map * overlay * map_points

# ============================================================================
# PANE ASSEMBLY
# ============================================================================

# Wrap visualizations in Panel panes
top_left = pn.pane.HoloViews(scatter, sizing_mode='stretch_both')
top_right = pn.pane.HoloViews(dynamic_line, sizing_mode='stretch_both')
bottom_left = pn.pane.HoloViews(map_view, sizing_mode='stretch_both')
bottom_right = pn.pane.Markdown(
    "## Bottom Right View\n\nPlaceholder for additional view", 
    sizing_mode='stretch_both'
)

# ============================================================================
# GOLDENLAYOUT TEMPLATE
# ============================================================================
# ============================================================================
# GOLDENLAYOUT TEMPLATE
# ============================================================================

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
                    // Top row: Scatter + Line
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
                    // Bottom row: Map + Placeholder
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
    if(width)
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

# ============================================================================
# TEMPLATE INITIALIZATION
# ============================================================================

# Create Panel template with server and notebook variants
tmpl = pn.Template(
    template=(template % 'server'), 
    nb_template=(template % 'notebook')
)

# Add globally unique ID generator for notebook context
tmpl.nb_template.globals['get_id'] = make_globally_unique_id

# Register all panels with the template
tmpl.add_panel('controller', controller)
tmpl.add_panel('top_left', top_left)
tmpl.add_panel('top_right', top_right)
tmpl.add_panel('bottom_left', bottom_left)
tmpl.add_panel('bottom_right', bottom_right)

# Make template servable
tmpl.servable(title='HoloViz Prototypes')