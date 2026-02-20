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
import numpy as np
import zarr
from pathlib import Path
from holoviews import streams
from bokeh.util.serialization import make_globally_unique_id
from geoviews import tile_sources as gvts

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

DATA_ROOT = Path(__file__).parent / "bukov.zarr" / "bukov.zarr"
DEFAULT_GROUP = "bukov"


def load_bukov_group():
    """Open local Bukov Zarr group."""
    root = zarr.open_group(DATA_ROOT, mode="r")
    return root[DEFAULT_GROUP] if DEFAULT_GROUP in root else root


def get_overlay_bounds_from_coords(lats: np.ndarray, lons: np.ndarray, pad_ratio: float = 0.05):
    """Compute overlay bounds with a small padding."""
    lat_min, lat_max = np.nanmin(lats), np.nanmax(lats)
    lon_min, lon_max = np.nanmin(lons), np.nanmax(lons)
    lat_pad = (lat_max - lat_min) * pad_ratio if lat_max > lat_min else 0.01
    lon_pad = (lon_max - lon_min) * pad_ratio if lon_max > lon_min else 0.01
    return (lon_min - lon_pad, lat_min - lat_pad, lon_max + lon_pad, lat_max + lat_pad)


def load_bukov_map_data(group, var_name: str = "rock_temp", time_index: int = 0, depth_index: int = 0):
    """Load map-ready data from Bukov Zarr arrays."""
    lats = np.array(group["latitude"][:], dtype=float)
    lons = np.array(group["longitude"][:], dtype=float)

    valid_mask = np.isfinite(lats) & np.isfinite(lons)
    print(f"[Bukov] Total locations: {lats.size}, Valid locations: {int(valid_mask.sum())}")
    if not valid_mask.all():
        invalid_indices = np.where(~valid_mask)[0].tolist()
        print(f"[Bukov] Invalid location indices: {invalid_indices}")

    if var_name not in group:
        raise KeyError(f"Variable '{var_name}' not found in Bukov group")

    values = np.array(group[var_name][time_index, :, depth_index], dtype=float)

    map_df = pd.DataFrame({
        "lon": lons,
        "lat": lats,
        "value": values
    })
    overlay_bounds = get_overlay_bounds_from_coords(lats, lons)
    return map_df, overlay_bounds, lats, lons


def to_datetime_index(values, units: str | None = None):
    arr = np.array(values)
    if np.issubdtype(arr.dtype, np.datetime64):
        return pd.to_datetime(arr)

    if units and "since" in units:
        unit_part, origin_part = units.split("since", 1)
        unit_part = unit_part.strip().lower()
        origin_part = origin_part.strip()
        unit_map = {
            "seconds": "s",
            "second": "s",
            "minutes": "m",
            "minute": "m",
            "hours": "h",
            "hour": "h",
            "days": "D",
            "day": "D",
        }
        unit_code = unit_map.get(unit_part, "s")
        return pd.to_datetime(arr, unit=unit_code, origin=origin_part, utc=True).tz_convert(None)

    if np.issubdtype(arr.dtype, np.integer) or np.issubdtype(arr.dtype, np.floating):
        return pd.to_datetime(arr, unit="s", utc=True).tz_convert(None)

    return pd.to_datetime(arr, errors="coerce")


def load_bukov_timeseries(group, var_name: str = "rock_temp", borehole_index: int = 0, depth_index: int = 0):
    """Load a single-borehole time series for plots."""
    if var_name not in group:
        raise KeyError(f"Variable '{var_name}' not found in Bukov group")

    units = group["date_time"].attrs.get("units")
    times = to_datetime_index(group["date_time"][:], units=units)
    values = np.array(group[var_name][:, borehole_index, depth_index], dtype=float)

    return pd.DataFrame({
        "time": times,
        "x": np.arange(len(times)),
        "y": values,
        "temperature": values
    })


bukov_group = load_bukov_group()
df = load_bukov_timeseries(bukov_group)
map_df, overlay_bounds, lats_arr, lons_arr = load_bukov_map_data(bukov_group)
depth_arr = np.array(bukov_group["depth"][:], dtype=float)
date_time_units = bukov_group["date_time"].attrs.get("units")
date_time_values = bukov_group["date_time"][:]
date_time_index = to_datetime_index(date_time_values, units=date_time_units)
if len(date_time_index) >= 2:
    step = date_time_index[1] - date_time_index[0]
else:
    step = None
print(
    "[Bukov] date_time range:",
    date_time_index.min(),
    "->",
    date_time_index.max(),
    "step:",
    step,
    "count:",
    len(date_time_index),
    "units:",
    date_time_units,
)

# ============================================================================
# INTERACTIVE VISUALIZATIONS
# ============================================================================

# --- Line Plot: Temperature over time (linked to map tap + depth selection) ---
tap_stream = streams.Tap(x=None, y=None)
borehole_stream = streams.Stream.define("Borehole", borehole_index=0)()
borehole_stream.event(borehole_index=0)

depth_selector = pn.widgets.CheckBoxGroup(
    name="Depths (m)",
    options=[],
    value=[],
    inline=False,
    sizing_mode="stretch_width"
)
borehole_info = pn.pane.Markdown("### Borehole 0", sizing_mode="stretch_width")


def get_borehole_index(x, y):
    if x is None or y is None:
        return 0
    dist = (lons_arr - x) ** 2 + (lats_arr - y) ** 2
    return int(np.nanargmin(dist))


def get_available_depth_indices(borehole_index: int):
    values = np.array(bukov_group["rock_temp"][:, borehole_index, :], dtype=float)
    mask = np.any(np.isfinite(values), axis=0)
    return [int(i) for i in np.where(mask)[0].tolist()]


def format_depth(depth_value: float):
    if np.isnan(depth_value):
        return "NaN"
    return f"{depth_value:.2f}"


def update_depth_selector(x, y):
    borehole_index = get_borehole_index(x, y)
    available = get_available_depth_indices(borehole_index)
    depth_selector.options = {format_depth(depth_arr[i]): i for i in available}
    depth_selector.value = available
    borehole_info.object = f"### Borehole {borehole_index}"
    return borehole_index


def build_timeseries_overlay(borehole_index, selected_depths, time_slice=None):
    curves = []
    values_by_depth = [
        np.array(bukov_group["rock_temp"][:, borehole_index, depth_idx], dtype=float)
        for depth_idx in selected_depths
    ]
    times = date_time_index
    if time_slice is not None:
        start, end = time_slice
        mask = (times >= start) & (times <= end)
        times = times[mask]
        values_by_depth = [vals[mask] for vals in values_by_depth]
    for col_idx, depth_idx in enumerate(selected_depths):
        depth_val = depth_arr[depth_idx] if depth_idx < len(depth_arr) else depth_idx
        label = f"{format_depth(depth_val)} m"
        curve_df = pd.DataFrame({
            "time": times,
            "temperature": values_by_depth[col_idx]
        })
        curves.append(hv.Curve(curve_df, 'time', 'temperature', label=label))

    if not curves:
        curves = [hv.Curve([])]
    return hv.Overlay(curves)


def clamp_range(center, span):
    min_t = date_time_index.min()
    max_t = date_time_index.max()
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


full_span = date_time_index.max() - date_time_index.min()
mid_span = pd.Timedelta(days=30)
right_span = pd.Timedelta(hours=24)

center_state = {
    "center": date_time_index.min() + (full_span / 2),
}
center_stream = streams.Stream.define("Center", center=None)()
center_stream.event(center=center_state["center"])
center_state["force_mid"] = False
center_state["force_right"] = False

left_range = streams.RangeX()
mid_range = streams.RangeX()
right_range = streams.RangeX()
left_tap = streams.Tap()
mid_tap = streams.Tap()
right_tap = streams.Tap()

_updating_center = False


def update_center_from_range(event, source):
    return


def update_center_from_tap(event, source):
    global _updating_center
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


def make_xrange_hook(xlim, force_flag_key):
    def _hook(plot, element):
        plot.state.x_range.bounds = xlim
        if center_state.get(force_flag_key, False):
            plot.state.x_range.start = xlim[0]
            plot.state.x_range.end = xlim[1]
            center_state[force_flag_key] = False
    return _hook


left_tap.param.watch(lambda e: update_center_from_tap(e, "left"), ["x"])
mid_tap.param.watch(lambda e: update_center_from_tap(e, "mid"), ["x"])
right_tap.param.watch(lambda e: update_center_from_tap(e, "right"), ["x"])


def create_timeseries_view(
    x=None,
    y=None,
    value=None,
    x_range=None,
    center=None,
    borehole_index=0,
    view="left",
    **kwargs,
):
    selected_depths = value or []
    if not selected_depths:
        selected_depths = get_available_depth_indices(borehole_index)

    center_time = center or center_state["center"]
    if view == "left":
        xlim = (date_time_index.min(), date_time_index.max())
    elif view == "mid":
        xlim = clamp_range(center_time, mid_span)
    else:
        xlim = clamp_range(center_time, right_span)

    overlay = build_timeseries_overlay(borehole_index, selected_depths, time_slice=None)
    overlay = overlay.redim.range(time=xlim)
    overlay = overlay * hv.VLine(center_time).opts(color='red', line_width=2)
    hooks = []
    if view != "left":
        force_key = "force_mid" if view == "mid" else "force_right"
        hooks = [make_xrange_hook(xlim, force_key)]
    return overlay.opts(
        width=600,
        height=400,
        responsive=True,
        title=f"Temperature over Time (borehole {borehole_index})",
        tools=['hover', 'xwheel_zoom', 'xpan', 'tap', 'reset'],
        active_tools=['xwheel_zoom', 'xpan'],
        xlim=xlim,
        axiswise=True,
        shared_axes=False,
        legend_position='right',
        hooks=hooks,
        framewise=True
    )


line_left = hv.DynamicMap(
    lambda x=None, y=None, value=None, x_range=None, center=None, borehole_index=0, **kwargs: create_timeseries_view(
        x=x, y=y, value=value, x_range=x_range, center=center, borehole_index=borehole_index, view="left"
    ),
    streams=[
        borehole_stream,
        streams.Params(depth_selector, parameters=['value']),
        left_tap,
        center_stream,
    ]
)

line_mid = hv.DynamicMap(
    lambda x=None, y=None, value=None, x_range=None, center=None, borehole_index=0, **kwargs: create_timeseries_view(
        x=x, y=y, value=value, x_range=x_range, center=center, borehole_index=borehole_index, view="mid"
    ),
    streams=[
        borehole_stream,
        streams.Params(depth_selector, parameters=['value']),
        mid_tap,
        center_stream,
    ]
)

line_right = hv.DynamicMap(
    lambda x=None, y=None, value=None, x_range=None, center=None, borehole_index=0, **kwargs: create_timeseries_view(
        x=x, y=y, value=value, x_range=x_range, center=center, borehole_index=borehole_index, view="right"
    ),
    streams=[
        borehole_stream,
        streams.Params(depth_selector, parameters=['value']),
        right_tap,
        center_stream,
    ]
)

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
    tools=['hover', 'tap'], 
    colorbar=True,
    width=600, 
    height=400, 
    title='Geographic Data View'
)

tap_stream.source = map_points
def on_tap_event(*args, **kwargs):
    borehole_index = update_depth_selector(tap_stream.x, tap_stream.y)
    borehole_stream.event(borehole_index=borehole_index)


tap_stream.param.watch(on_tap_event, ['x', 'y'])
update_depth_selector(tap_stream.x, tap_stream.y)

# Combine all map layers
map_view = base_map * overlay * map_points

# ============================================================================
# PANE ASSEMBLY
# ============================================================================

# Wrap visualizations in Panel panes
top_left = pn.pane.HoloViews(map_view, sizing_mode='stretch_both')
top_right = pn.Column(
    borehole_info,
    depth_selector,
    sizing_mode='stretch_both'
)
bottom_left = pn.pane.HoloViews(line_left, sizing_mode='stretch_both')
bottom_mid = pn.pane.HoloViews(line_mid, sizing_mode='stretch_both')
bottom_right = pn.pane.HoloViews(line_right, sizing_mode='stretch_both')

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
tmpl.add_panel('bottom_mid', bottom_mid)
tmpl.add_panel('bottom_right', bottom_right)

# Make template servable
tmpl.servable(title='HoloViz Prototypes')