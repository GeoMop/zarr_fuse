"""Test visible marker positions after asynchronous plot data arrives."""

import asyncio
from types import SimpleNamespace

import holoviews as hv
import pandas as pd
import pytest
from bokeh.models import GlyphRenderer, Scatter

from dashboard.multi_time_views import build_timeseries_views
from dashboard.test.test_right_timeseries import (
    _curves,
    _Document,
    _pan,
    _until,
    _windows,
)


@pytest.mark.parametrize("pane", ["left", "mid", "right"])
def test_indicator_markers_are_inside_the_visible_window(
    pane, setup_right, tmp_path, monkeypatch
):
    """Each pane places indicator markers inside its Bokeh x-range."""
    _, data, state, _ = setup_right
    monkeypatch.setattr(
        data.client, "_view_handle", lambda *_: SimpleNamespace(store_uri="source")
    )
    monkeypatch.setenv("ZF_TIMESERIES_OVERVIEW_DIR", str(tmp_path))
    data.client.generate_timeseries_overview("view", "/", "temp", tmp_path, bins=8)

    async def run():
        windows = _windows(data, _Document())
        left, mid, right, *_ = build_timeseries_views(data, {}, state, windows=windows)
        view = {"left": left, "mid": mid, "right": right}[pane]
        try:
            plot = hv.renderer("bokeh").get_plot(view)
            await _until(lambda: len(_curves(view)[0]) > 0)
            start, end = (
                pd.Timestamp(value)
                for value in (plot.state.x_range.start, plot.state.x_range.end)
            )
            markers = [
                renderer
                for renderer in plot.state.renderers
                if isinstance(renderer, GlyphRenderer)
                and isinstance(renderer.glyph, Scatter)
            ]
            assert markers
            for renderer in markers:
                times = pd.to_datetime(renderer.data_source.data["time"])
                assert (times >= start).all() and (times <= end).all()
            if pane == "left":
                summary_before = _curves(view)[0].dimension_values(0)
                _pan(view, "2024-01-03", "2024-01-04")
                await _until(lambda: windows["left"].changed.revision == 2)
                assert len(_curves(view)[0]) == 25
                _pan(view, start, end)
                assert windows["left"].changed.revision == 2
                assert (_curves(view)[0].dimension_values(0) == summary_before).all()
            state.clear()
            assert not len(_curves(view)[0])
        finally:
            for controller in windows.values():
                controller.close()

    asyncio.run(run())
