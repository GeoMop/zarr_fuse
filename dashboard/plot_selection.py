"""Plot selection panel — table-based site/depth selector."""

import panel as pn


def build_plot_selection():
    """Temporary relocation: just wraps the existing depth controls."""
    depth_selector = pn.widgets.CheckBoxGroup(
        name="Depths (m)",
        options=[],
        value=[],
        inline=False,
        sizing_mode="stretch_width",
    )
    borehole_info = pn.pane.Markdown("### No borehole selected", sizing_mode="stretch_width")
    return depth_selector, borehole_info
