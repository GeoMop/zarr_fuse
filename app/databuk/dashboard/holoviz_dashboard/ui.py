import panel as pn
import pandas as pd


def build_sidebar():
    header = pn.pane.HTML("""
<div style="background: linear-gradient(135deg, #2563eb 0%, #4f46e5 100%);
            padding: 12px 15px; border-radius: 12px; box-shadow: 0 4px 6px rgba(0,0,0,0.3);
            margin-bottom: 15px;">
    <div style="display: flex; align-items: center; gap: 10px;">
        <div style="background: rgba(255,255,255,0.2); padding: 8px; border-radius: 8px;">
            <span style="font-size: 20px;">🗄️</span>
        </div>
        <div>
            <h2 style="margin: 0; font-size: 16px; font-weight: 700; color: white;">ZARR FUSE (mock)</h2>
            <p style="margin: 0; font-size: 11px; color: #bfdbfe;">Data Platform (mock)</p>
        </div>
    </div>
</div>
""", sizing_mode="stretch_width")

    store_selector = pn.widgets.Select(
        name="📦 Store Name (mock)",
        value="Mock Store A",
        options=["Mock Store A", "Mock Store B", "Mock Store C"],
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
        STORE URL (mock)
    </div>
    <div style="font-size: 12px; color: #e2e8f0; font-family: monospace;">
        s3://mock-bucket/path/to/store.zarr (mock)
    </div>
</div>
""", sizing_mode="stretch_width")

    status_section = pn.pane.HTML("""
<div style="background: #0f172a; padding: 12px; border-radius: 8px; margin: 8px 0;">
    <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 8px;">
        <span style="font-size: 12px; color: #94a3b8; font-weight: 600;">SERVICE STATUS (mock)</span>
        <div style="display: flex; align-items: center; gap: 6px;">
            <span style="width: 8px; height: 8px; background: #10b981; border-radius: 50%;
                        box-shadow: 0 0 10px #10b981;"></span>
            <span style="font-size: 11px; color: #10b981; font-weight: 600;">Active (mock)</span>
        </div>
    </div>
    <div style="font-size: 10px; color: #64748b; display: flex; align-items: center; gap: 4px;">
        <span>🕐</span>
        <span>Updated (mock): """ + pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S") + """</span>
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

    tree_view = pn.pane.HTML("""
<div style="background: #1e293b; padding: 14px; border-radius: 8px; margin-top: 12px;">
    <div style="font-size: 13px; color: #f1f5f9; font-weight: 600; margin-bottom: 12px;
                border-bottom: 1px solid #334155; padding-bottom: 8px;">
        📁 DATA STRUCTURE (mock)
    </div>
    <div style="font-size: 11px; color: #cbd5e1;">
        <div style="margin-bottom: 6px;">
            <button style="background: #334155; border: none; color: #fbbf24; padding: 6px 10px;
                          border-radius: 6px; cursor: pointer; font-weight: 600; width: 100%;
                          text-align: left; font-size: 11px; transition: all 0.2s;"
                    onmouseover="this.style.background='#475569'"
                    onmouseout="this.style.background='#334155'">
                📂 temperature (mock)
            </button>
            <div style="margin-left: 20px; margin-top: 4px;">
                <button style="background: transparent; border: 1px solid #475569; color: #94a3b8;
                              padding: 4px 8px; border-radius: 4px; cursor: pointer; width: 100%;
                              text-align: left; font-size: 11px; margin-bottom: 3px; transition: all 0.2s;"
                        onmouseover="this.style.background='#334155'; this.style.borderColor='#64748b'"
                        onmouseout="this.style.background='transparent'; this.style.borderColor='#475569'">
                    📊 surface_temp (mock)
                </button>
                <button style="background: transparent; border: 1px solid #475569; color: #94a3b8;
                              padding: 4px 8px; border-radius: 4px; cursor: pointer; width: 100%;
                              text-align: left; font-size: 11px; margin-bottom: 3px; transition: all 0.2s;"
                        onmouseover="this.style.background='#334155'; this.style.borderColor='#64748b'"
                        onmouseout="this.style.background='transparent'; this.style.borderColor='#475569'">
                    📊 air_temp (mock)
                </button>
            </div>
        </div>
        <div style="margin-bottom: 6px;">
            <button style="background: #334155; border: none; color: #fbbf24; padding: 6px 10px;
                          border-radius: 6px; cursor: pointer; font-weight: 600; width: 100%;
                          text-align: left; font-size: 11px; transition: all 0.2s;"
                    onmouseover="this.style.background='#475569'"
                    onmouseout="this.style.background='#334155'">
                📂 pressure (mock)
            </button>
            <div style="margin-left: 20px; margin-top: 4px;">
                <button style="background: transparent; border: 1px solid #475569; color: #94a3b8;
                              padding: 4px 8px; border-radius: 4px; cursor: pointer; width: 100%;
                              text-align: left; font-size: 11px; margin-bottom: 3px; transition: all 0.2s;"
                        onmouseover="this.style.background='#334155'; this.style.borderColor='#64748b'"
                        onmouseout="this.style.background='transparent'; this.style.borderColor='#475569'">
                    📊 sea_level (mock)
                </button>
            </div>
        </div>
        <div>
            <button style="background: #334155; border: none; color: #fbbf24; padding: 6px 10px;
                          border-radius: 6px; cursor: pointer; font-weight: 600; width: 100%;
                          text-align: left; font-size: 11px; transition: all 0.2s;"
                    onmouseover="this.style.background='#475569'"
                    onmouseout="this.style.background='#334155'">
                📂 wind (mock)
            </button>
            <div style="margin-left: 20px; margin-top: 4px;">
                <button style="background: transparent; border: 1px solid #475569; color: #94a3b8;
                              padding: 4px 8px; border-radius: 4px; cursor: pointer; width: 100%;
                              text-align: left; font-size: 11px; margin-bottom: 3px; transition: all 0.2s;"
                        onmouseover="this.style.background='#334155'; this.style.borderColor='#64748b'"
                        onmouseout="this.style.background='transparent'; this.style.borderColor='#475569'">
                    📊 u_component (mock)
                </button>
                <button style="background: transparent; border: 1px solid #475569; color: #94a3b8;
                              padding: 4px 8px; border-radius: 4px; cursor: pointer; width: 100%;
                              text-align: left; font-size: 11px; margin-bottom: 3px; transition: all 0.2s;"
                        onmouseover="this.style.background='#334155'; this.style.borderColor='#64748b'"
                        onmouseout="this.style.background='transparent'; this.style.borderColor='#475569'">
                    📊 v_component (mock)
                </button>
            </div>
        </div>
    </div>
</div>
""", sizing_mode="stretch_width")

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
