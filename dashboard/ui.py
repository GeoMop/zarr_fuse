import panel as pn
import numpy as np
from dashboard.sidebar import build_sidebar


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