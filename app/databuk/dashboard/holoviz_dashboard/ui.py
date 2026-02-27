import panel as pn
import pandas as pd
import numpy as np


def _format_unit(value):
    if value is None:
        return ""
    if isinstance(value, dict):
        return ", ".join(f"{k}={v}" for k, v in value.items())
    return str(value)


def _format_range(values: np.ndarray) -> str:
    if values.size == 0:
        return "(empty)"
    try:
        if np.issubdtype(values.dtype, np.datetime64):
            min_v = pd.to_datetime(values.min()).strftime("%Y-%m-%d")
            max_v = pd.to_datetime(values.max()).strftime("%Y-%m-%d")
            return f"[{min_v} ... {max_v}]"
        if np.issubdtype(values.dtype, np.number):
            min_v = np.nanmin(values)
            max_v = np.nanmax(values)
            return f"[{min_v:g} ... {max_v:g}]"
    except Exception:
        pass
    first = values.flat[0]
    last = values.flat[-1]
    return f"{{{first}, ... , {last}}}"


def _flatten_nodes(node, depth: int = 0, items=None):
    if items is None:
        items = []

    name = node.name or "root"
    path = node.group_path or "/"
    label = f"{'  ' * depth}{name}"
    items.append((label, path))

    try:
        for child in node.children.values():
            _flatten_nodes(child, depth + 1, items)
    except Exception:
        pass

    return items


def _build_node_tree_dict(node):
    name = node.name or "root"
    children = {}
    try:
        for child in node.children.values():
            child_name = child.name or "root"
            children[child_name] = _build_node_tree_dict(child)
    except Exception:
        children = {}

    return children


def build_sidebar(endpoint_name, endpoint_config, node, endpoints=None):
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
        disabled=True,
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
        """ + endpoint_config.store_url + """
    </div>
</div>
""", sizing_mode="stretch_width")

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
        <span>Updated: """ + pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S") + """</span>
    </div>
</div>
""", sizing_mode="stretch_width")

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

    node_items = _flatten_nodes(node)
    node_options = {label: path for label, path in node_items}
    first_value = node_items[0][1] if node_items else None
    tree_view = pn.widgets.Select(
        name="DATA STRUCTURE",
        options=node_options,
        value=first_value,
        size=min(12, max(3, len(node_items))),
        sizing_mode="stretch_width",
    )

    controller = pn.Column(
        header,
        store_selector,
        store_info,
        status_section,
        reload_button,
        tree_view,
        pn.layout.VSpacer(),
        sizing_mode="stretch_width",
        styles={"padding": "10px"},
    )

    return controller


def build_depth_controls():
    depth_selector = pn.widgets.CheckBoxGroup(
        name="Depths (m)",
        options=[],
        value=[],
        inline=False,
        sizing_mode="stretch_width",
    )
    borehole_info = pn.pane.Markdown("### Borehole 0", sizing_mode="stretch_width")
    return depth_selector, borehole_info
