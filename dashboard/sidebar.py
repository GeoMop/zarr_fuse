import panel as pn
import pandas as pd


def _flatten_nodes(structure, depth: int = 0, items=None, prefixes=None, is_last: bool = True):
    if items is None:
        items = []
    if prefixes is None:
        prefixes = []

    name = structure.get("name") or "root"
    path = structure.get("path") or "/"
    children = structure.get("children", []) or []

    # Skip rendering synthetic root if it only contains real child groups.
    render_current = not (path == "/" and children)
    if render_current:
        branch = "".join(prefixes)
        connector = "└─ " if is_last else "├─ "
        label = f"{branch}{connector if branch else ''}{name}"
        items.append((label, path))

    next_depth = depth + 1 if render_current else depth
    next_prefixes = list(prefixes)
    if render_current:
        next_prefixes.append("   " if is_last else "│  ")

    for index, child in enumerate(children):
        child_is_last = index == len(children) - 1
        _flatten_nodes(child, next_depth, items, next_prefixes, child_is_last)

    return items


def build_sidebar(endpoint_name, endpoint_config, structure, endpoints=None,
                  loading_indicator=None, timeseries_loading=None,
                  render_spinner=None, rendering_status=None):
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
""", sizing_mode="stretch_width")

    endpoint_options = list(endpoints.keys()) if endpoints else [endpoint_name]
    store_selector = pn.widgets.Select(
        name="📦 Store Name",
        value=endpoint_name,
        options=endpoint_options,
        width=320,
        disabled=False,
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

    store_uri = ""
    if isinstance(endpoint_config, dict):
        source_config = endpoint_config.get("source", {}) or {}
        store_uri = source_config.get("uri", "")

    store_info = pn.pane.HTML("""
<div style="background: #1e293b; padding: 12px; border-radius: 8px; margin: 8px 0;
            border-left: 3px solid #3b82f6;">
    <div style="font-size: 11px; color: #94a3b8; margin-bottom: 4px; font-weight: 600;">
        STORE URI
    </div>
    <div style="font-size: 12px; color: #e2e8f0; font-family: monospace;">
        """ + store_uri + """
    </div>
</div>
""", sizing_mode="stretch_width")

    _status_header = pn.pane.HTML(sizing_mode="stretch_width")

    def _update_status_header(*_):
        if rendering_status is not None and rendering_status.visible:
            text = "⚠️ Error"
            dot_color = "#ef4444"
        elif loading_indicator is not None and loading_indicator.visible:
            text = "⏳ Loading dataset..."
            dot_color = "#f59e0b"
        elif timeseries_loading is not None and timeseries_loading.visible:
            text = "⏳ Loading timeseries..."
            dot_color = "#f59e0b"
        elif render_spinner is not None and render_spinner.visible:
            text = "⏳ Rendering..."
            dot_color = "#f59e0b"
        else:
            text = "Active"
            dot_color = "#10b981"
        _status_header.object = f"""
<div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 8px;">
    <span style="font-size: 12px; color: #94a3b8; font-weight: 600;">SERVICE STATUS</span>
    <div style="display: flex; align-items: center; gap: 6px;">
        <span style="width: 8px; height: 8px; background: {dot_color}; border-radius: 50%;
                    box-shadow: 0 0 10px {dot_color};"></span>
        <span style="font-size: 11px; color: {dot_color}; font-weight: 600;">{text}</span>
    </div>
</div>
"""

    if loading_indicator is not None:
        loading_indicator.param.watch(_update_status_header, ["visible"])
    if timeseries_loading is not None:
        timeseries_loading.param.watch(_update_status_header, ["visible"])
    if render_spinner is not None:
        render_spinner.param.watch(_update_status_header, ["visible"])
    if rendering_status is not None:
        rendering_status.param.watch(_update_status_header, ["visible"])
    _update_status_header()

    _status_children = [_status_header]
    if rendering_status is not None:
        _status_children.append(rendering_status)
    status_section = pn.Column(
        *_status_children,
        styles={"background": "#0f172a", "padding": "12px", "border-radius": "8px", "margin": "8px 0"},
        sizing_mode="stretch_width",
    )

    reload_button = pn.widgets.Button(
        name="🔄 Reload Data",
        button_type="primary",
        width=320,
        height=40,
        styles={
            "background": "#3b82f6",
            "font-weight": "600",
            "font-size": "14px",
            "border-radius": "8px",
        },
    )

    node_items = _flatten_nodes(structure)
    node_options = {label: path for label, path in node_items}
    first_value = node_items[0][1] if node_items else None
    tree_view = pn.widgets.Select(
        name="DATA STRUCTURE",
        options=node_options,
        value=first_value,
        size=min(12, max(3, len(node_items))),
        sizing_mode="stretch_width",
    )

    variable_selector = pn.widgets.Select(
        name="VARIABLE",
        options=[],
        size=5,
        width=320,
    )

    variable_metadata = pn.pane.HTML(
        "",
        visible=False,
        sizing_mode="stretch_width",
    )

    node_hint = pn.pane.Alert(
        "",
        alert_type="warning",
        visible=False,
        sizing_mode="stretch_width",
    )

    controller = pn.Column(
        header,
        store_selector,
        store_info,
        status_section,
        reload_button,
        tree_view,
        variable_selector,
        variable_metadata,
        node_hint,
        pn.layout.VSpacer(),
        sizing_mode="stretch_width",
        styles={"padding": "10px"},
    )

    return controller, store_selector, tree_view, variable_selector, variable_metadata, node_hint, store_info



