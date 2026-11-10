"""Unit tests for the per-window borehole marker window resolution.

The markers shown on the timeseries curves must be spaced relative to the
visible X-range of the window they are drawn in (~5 markers per window).
``_resolve_marker_window`` intersects the live ``x_range`` (from the ``RangeX``
stream after pan/zoom) with the view's ``xlim`` and clamps the result to the
data bounds.  The intersection matters on the initial render, where the plot
range is auto-computed from the whole series and must not drag the markers
out of the visible window.
"""

from __future__ import annotations

import pandas as pd

from dashboard.multi_time_views import _resolve_marker_window


_TIMES = pd.date_range("2024-01-01", "2026-12-31", freq="D")
_MID_XLIM = (pd.Timestamp("2025-06-01"), pd.Timestamp("2025-07-01"))


class TestResolveMarkerWindow:
    def test_live_x_range_inside_window_is_used(self) -> None:
        x_range = (pd.Timestamp("2025-06-10"), pd.Timestamp("2025-06-12"))
        assert _resolve_marker_window(x_range, _MID_XLIM, _TIMES) == x_range

    def test_none_x_range_falls_back_to_xlim(self) -> None:
        assert _resolve_marker_window(None, _MID_XLIM, _TIMES) == _MID_XLIM

    def test_nan_x_range_falls_back_to_xlim(self) -> None:
        x_range = (pd.NaT, pd.NaT)
        assert _resolve_marker_window(x_range, _MID_XLIM, _TIMES) == _MID_XLIM

    def test_malformed_x_range_falls_back_to_xlim(self) -> None:
        x_range = (pd.NaT, "2025-07-01")
        assert _resolve_marker_window(x_range, _MID_XLIM, _TIMES) == _MID_XLIM

    def test_wide_live_range_is_clamped_to_the_view_window(self) -> None:
        # On the initial render the plot range is auto-computed from the whole
        # series; the markers must stay inside the view window.
        x_range = (pd.Timestamp("2020-01-01"), pd.Timestamp("2030-01-01"))
        assert _resolve_marker_window(x_range, _MID_XLIM, _TIMES) == _MID_XLIM

    def test_partially_overlapping_live_range_is_intersected(self) -> None:
        x_range = (pd.Timestamp("2025-05-01"), pd.Timestamp("2025-06-15"))
        assert _resolve_marker_window(x_range, _MID_XLIM, _TIMES) == (
            pd.Timestamp("2025-06-01"),
            pd.Timestamp("2025-06-15"),
        )

    def test_disjoint_live_range_keeps_the_live_range(self) -> None:
        # A pan outside the view window must still show the markers.
        x_range = (pd.Timestamp("2026-01-01"), pd.Timestamp("2026-01-05"))
        assert _resolve_marker_window(x_range, _MID_XLIM, _TIMES) == x_range

    def test_inverted_range_returns_none(self) -> None:
        x_range = (pd.Timestamp("2025-06-12"), pd.Timestamp("2025-06-10"))
        assert _resolve_marker_window(x_range, _MID_XLIM, _TIMES) is None

    def test_empty_times_returns_none(self) -> None:
        empty = pd.to_datetime([])
        assert _resolve_marker_window(None, _MID_XLIM, empty) is None
        assert _resolve_marker_window((pd.NaT, pd.NaT), _MID_XLIM, empty) is None

    def test_live_range_beyond_data_is_clipped_to_the_view_window(self) -> None:
        x_range = (pd.Timestamp("2025-06-10"), pd.Timestamp("2030-01-01"))
        assert _resolve_marker_window(x_range, _MID_XLIM, _TIMES) == (
            pd.Timestamp("2025-06-10"),
            pd.Timestamp("2025-07-01"),
        )
