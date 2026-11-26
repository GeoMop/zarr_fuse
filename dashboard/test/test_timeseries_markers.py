"""Regression test: the indicator markers must stay inside the visible window.

On the initial render the Bokeh plot auto-computes its X range from the whole
series and the ``RangeX`` stream reports that range.  The marker window used
to follow the live range, which spread the five indicator markers over the
entire data range: in the month and day views they then fell outside the
visible window and were not drawn at all.  They only appeared after the user
moved the red line, because that re-anchored the markers to the view window.
"""

from __future__ import annotations

from typing import Any

import holoviews as hv
import numpy as np
import pandas as pd
import pytest
from bokeh.models import GlyphRenderer
from bokeh.models.glyphs import Scatter as ScatterGlyph

from dashboard.multi_time_views import build_timeseries_views
from dashboard.plot_selection import SelectionState

_TIMES = pd.date_range("2022-01-01", "2026-12-31", freq="D")
_VIEWS = ("left", "mid", "right")


class _FakeClient:
    """Minimal client stub: only the view config is needed for rendering."""

    def get_view(self, view_name: str) -> dict:
        return {
            "defaults": {"default_site": "BH-7"},
            "schema_display": {"entity_name": "Site"},
            "schema": {
                "fields": {"entity": "entity", "vertical": "vertical", "time": "time"}
            },
            "visualization": {
                "timeseries": {"middle_window_days": 30, "right_window_hours": 24}
            },
        }

    def get_variable_metadata(self, view_name: str, group_path: str, variable: str) -> dict:
        return {"unit": "degC"}


class _FakeData:
    view_name = "test_view"
    group_path = "/"
    display_variable = "temp"

    def __init__(self) -> None:
        self.client = _FakeClient()


def _build_views() -> dict[str, hv.DynamicMap]:
    """Build the three timeseries views for one site with a multi-year series."""
    values = np.sin(np.arange(len(_TIMES)) / 30.0) * 5 + 10
    state = SelectionState()
    state.add_site(
        entity_index=7,
        site_id="BH-7",
        depths=[0.0, 1.0],
        series=[values, values * 0.5],
        times=_TIMES,
    )
    data = _FakeData()
    map_state = {
        "lats": [47.0],
        "lons": [11.0],
        "marker_meta": [{"entity_index": 7, "site_id": "BH-7"}],
    }
    left, mid, right, _on_map_tap, _fetch_site = build_timeseries_views(
        data, map_state, state
    )
    return {"left": left, "mid": mid, "right": right}


def _as_timestamp(value: Any) -> pd.Timestamp:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return pd.Timestamp(value, unit="ms")
    return pd.Timestamp(value)


def _marker_timestamps(plot: Any) -> list[pd.Timestamp]:
    """Return the X position of every rendered indicator marker."""
    stamps: list[pd.Timestamp] = []
    for renderer in plot.state.renderers:
        if isinstance(renderer, GlyphRenderer) and isinstance(renderer.glyph, ScatterGlyph):
            columns = list(renderer.data_source.data.keys())
            stamps.extend(_as_timestamp(x) for x in renderer.data_source.data[columns[0]])
    return stamps


@pytest.mark.parametrize("view", _VIEWS)
def test_indicator_markers_are_inside_the_visible_window(view: str) -> None:
    dmaps = _build_views()
    dmap = dmaps[view]

    plot = hv.renderer("bokeh").get_plot(dmap)
    # The app performs a second evaluation on the initial load, by which time
    # the plot range is already reported by the RangeX stream.
    dmap.event()

    x_range = plot.state.x_range
    start = _as_timestamp(x_range.start)
    end = _as_timestamp(x_range.end)
    markers = _marker_timestamps(plot)

    assert markers, "no indicator markers were rendered"
    outside = [m for m in markers if not (start <= m <= end)]
    assert not outside, (
        f"{view}: {len(outside)} of {len(markers)} indicator markers are outside "
        f"the visible window ({start} .. {end})"
    )


def _build_two_site_views():
    values = np.sin(np.arange(len(_TIMES)) / 30.0) * 5 + 10
    stale_values = np.full(len(_TIMES), 999.0)
    state = SelectionState()
    state.add_site(
        entity_index=7,
        site_id="BH-7",
        depths=[0.0],
        series=[values],
        times=_TIMES,
    )
    state.add_site(
        entity_index=8,
        site_id="BH-8",
        depths=[0.0],
        series=[stale_values],
        times=_TIMES,
    )
    data = _FakeData()
    map_state = {
        "lats": [47.0, 47.0],
        "lons": [11.0, 11.0],
        "marker_meta": [
            {"entity_index": 7, "site_id": "BH-7"},
            {"entity_index": 8, "site_id": "BH-8"},
        ],
    }
    left, mid, right, _on_map_tap, _fetch_site = build_timeseries_views(
        data, map_state, state
    )
    return state, {"left": left, "mid": mid, "right": right}


def _numeric_values(plot: Any) -> list[float]:
    values: list[float] = []
    for renderer in plot.state.renderers:
        if not isinstance(renderer, GlyphRenderer):
            continue
        for column in renderer.data_source.data.values():
            array = np.asarray(column)
            if array.dtype.kind in "fiu":
                values.extend(float(value) for value in array.tolist())
    return values


def test_cleared_site_does_not_keep_stale_curves_markers_or_ylim() -> None:
    state, dmaps = _build_two_site_views()
    left = dmaps["left"]
    plot = hv.renderer("bokeh").get_plot(left)
    left.event()
    assert any(value == 999.0 for value in _numeric_values(plot))

    checked_before = state.checked_combinations()
    state.update_sites_data([(8, None)])

    assert state.checked_combinations() == checked_before

    for name, dmap in dmaps.items():
        plot = hv.renderer("bokeh").get_plot(dmap[...,])
        values = _numeric_values(plot)
        assert not any(value == 999.0 for value in values), name
        assert plot.state.y_range.end < 100, name
