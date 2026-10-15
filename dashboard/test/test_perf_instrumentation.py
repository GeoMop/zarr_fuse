"""Tests for the diagnostic stages added around selection notifications.

Covers the instrumentation introduced to explain the ``selection.update``
bottleneck inside ``borehole.select``:

* ``fill_site.layout_notify`` / ``fill_site.version_notify`` in
  ``SelectionState.fill_site`` (``dashboard/plot_selection.py``),
* ``borehole.stream_event`` around ``borehole_stream.event(...)`` in
  ``_do_deferred_fetch`` (``dashboard/multi_time_views.py``),
* the enlarged ``_MAX_STAGE_LINES`` stage budget of the operation summary.

The tests assert both sides of the instrumentation contract: the stages are
recorded on the active operation summary when profiling is on, and wrapping
the notifications in ``stage()`` blocks leaves the application behaviour
unchanged (identical state, callback order and return values).

Run: python -m pytest dashboard/test/test_perf_instrumentation.py -v
"""

from __future__ import annotations

import logging
import re

import pandas as pd
import pytest

from dashboard import perf_summary, profiling
from dashboard.multi_time_views import build_timeseries_views
from dashboard.plot_selection import SelectionState

# Stages expected in one profiled ``borehole.select`` summary (see PLAN.md).
EXPECTED_BOREHOLE_SELECT_STAGES = [
    "selection.update",
    "fill_site.layout_notify",
    "fill_site.version_notify",
    "table.rebuild",
    "create_timeseries_view",
    "borehole.stream_event",
]


@pytest.fixture
def perf_log(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Force profiling on and capture every ``dashboard.performance`` record."""
    monkeypatch.setattr(profiling, "_ENABLED", True)
    captured: list[str] = []

    class _CapturingHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            captured.append(record.getMessage())

    handler = _CapturingHandler()
    handler.setLevel(logging.DEBUG)
    profiling.logger.addHandler(handler)
    yield captured
    profiling.logger.removeHandler(handler)


@pytest.fixture(autouse=True)
def _reset_summary_stack() -> None:
    """Ensure no thread-local summary leaks between tests."""
    perf_summary._local.__dict__.pop("stack", None)
    yield
    perf_summary._local.__dict__.pop("stack", None)


def _stage_counts(lines: list[str]) -> dict[str, int]:
    """Return ``{stage: count}`` for every ``event=op.stage`` record."""
    counts: dict[str, int] = {}
    for line in lines:
        if not line.startswith("event=op.stage "):
            continue
        name = re.search(r" stage=(\S+) ", line)
        total = re.search(r"count=(\d+) ", line)
        assert name is not None and total is not None, line
        counts[name.group(1)] = int(total.group(1))
    return counts


def _run_fill(monkeypatch: pytest.MonkeyPatch, profiling_on: bool) -> dict[str, object]:
    """Register + fill + refill one site and snapshot the resulting state.

    With ``profiling_on`` the work runs inside an active operation summary so
    the ``stage()`` blocks record; otherwise they take the cheap no-op path.
    """
    monkeypatch.setattr(profiling, "_ENABLED", profiling_on)
    state = SelectionState()
    state.register_site(entity_index=7, site_id="BH-7")
    with perf_summary.measure_operation("borehole.select"):
        first = state.fill_site(
            entity_index=7,
            series=[[1.0, 2.0], [3.0, 4.0]],
            times=pd.to_datetime(["2020-01-01", "2020-01-02"]),
            depths=[0.0, 1.0],
        )
        second = state.fill_site(
            entity_index=7,
            series=[[1.0, 2.0], [3.0, 4.0]],
            times=pd.to_datetime(["2020-01-01", "2020-01-02"]),
            depths=[0.0, 1.0],
        )
    return {
        "first": first,
        "second": second,
        "version": state.version,
        "layout_version": state.layout_version,
        "checked": state.checked_combinations(),
        "valid_count": state._valid_count,
        "checked_count": state._checked_count,
        "site_depths": [list(s["depths"]) for s in state.sites],
        "site_times": [list(map(str, s["times"])) for s in state.sites],
    }


class TestFillSiteNotifyStages:
    """``fill_site`` must time both parameter notifications separately."""

    def test_records_layout_and_version_notify(self, perf_log: list[str]) -> None:
        state = SelectionState()
        state.register_site(entity_index=7, site_id="BH-7")
        with perf_summary.measure_operation("borehole.select"):
            ok = state.fill_site(
                entity_index=7,
                series=[[1.0, 2.0]],
                times=pd.to_datetime(["2020-01-01", "2020-01-02"]),
                depths=[0.0, 1.0],
            )

        assert ok is True
        counts = _stage_counts(perf_log)
        assert counts.get("fill_site.layout_notify") == 1
        assert counts.get("fill_site.version_notify") == 1

    def test_same_depth_fill_skips_layout_notify(self, perf_log: list[str]) -> None:
        state = SelectionState()
        state.register_site(entity_index=7, site_id="BH-7")
        state.fill_site(
            entity_index=7,
            series=[[1.0, 2.0]],
            times=pd.to_datetime(["2020-01-01", "2020-01-02"]),
            depths=[0.0, 1.0],
        )
        with perf_summary.measure_operation("borehole.select"):
            state.fill_site(
                entity_index=7,
                series=[[1.0, 2.0]],
                times=pd.to_datetime(["2020-01-01", "2020-01-02"]),
                depths=[0.0, 1.0],
            )

        counts = _stage_counts(perf_log)
        assert counts.get("fill_site.version_notify") == 1
        assert "fill_site.layout_notify" not in counts, "no layout change -> no notify"

    def test_unknown_site_records_no_stages(self, perf_log: list[str]) -> None:
        state = SelectionState()
        with perf_summary.measure_operation("borehole.select"):
            ok = state.fill_site(
                entity_index=99,
                series=[[1.0]],
                times=pd.to_datetime(["2020-01-01"]),
                depths=[0.0],
            )

        assert ok is False
        stages = [line for line in perf_log if line.startswith("event=op.stage ")]
        assert stages == [], f"no stages for a failed fill: {stages}"

    def test_silent_without_active_operation(self, perf_log: list[str]) -> None:
        state = SelectionState()
        state.register_site(entity_index=7, site_id="BH-7")
        state.fill_site(
            entity_index=7,
            series=[[1.0, 2.0]],
            times=pd.to_datetime(["2020-01-01", "2020-01-02"]),
            depths=[0.0, 1.0],
        )

        assert perf_log == [], f"probes must stay silent outside an operation: {perf_log}"
        assert perf_summary.current() is None

    def test_behaviour_identical_with_and_without_recording(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        baseline = _run_fill(monkeypatch, profiling_on=False)
        recorded = _run_fill(monkeypatch, profiling_on=True)
        assert baseline == recorded


class TestBoreholeSelectPipeline:
    """The deferred ``borehole.select`` flow must expose every new stage."""

    def _build_views(self):
        class _FakeClient:
            def __init__(self) -> None:
                self.timeseries_calls = 0

            def get_view(self, view_name: str) -> dict:
                return {
                    "defaults": {},
                    "schema_display": {"entity_name": "Site"},
                    "schema": {
                        "fields": {"entity": "entity", "vertical": "vertical", "time": "time"}
                    },
                    "visualization": {
                        "timeseries": {"middle_window_days": 30, "right_window_hours": 6}
                    },
                }

            def get_variable_metadata(self, view_name: str, group_path: str, variable: str):
                return {"unit": "degC"}

            def get_timeseries_data(
                self,
                view_name: str,
                group_path: str,
                lat: float,
                lon: float,
                variable: str,
                entity_index: int | None = None,
            ) -> dict:
                self.timeseries_calls += 1
                return {
                    "status": "ok",
                    "times": ["2020-01-01", "2020-01-02", "2020-01-03"],
                    "depths": [0.0, 1.0],
                    "series": [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]],
                    "borehole_index": int(entity_index or 0),
                    "borehole_name": "BH-7",
                }

        class _FakeData:
            view_name = "test_view"
            group_path = "/"
            display_variable = "temp"

            def __init__(self) -> None:
                self.client = _FakeClient()

        data = _FakeData()
        map_state = {
            "lats": [47.0],
            "lons": [11.0],
            "marker_meta": [{"entity_index": 7, "site_id": "BH-7"}],
        }
        state = SelectionState()
        return data, map_state, state, build_timeseries_views(data, map_state, state)

    def test_map_tap_records_pipeline_stages(self, perf_log: list[str]) -> None:
        data, _map_state, state, views = self._build_views()
        _left, _mid, _right, on_map_tap, _fetch_site = views

        # ``pn.state.curdoc`` is None in tests, so the deferred fetch runs
        # synchronously inside this call.
        selected = on_map_tap(x=11.0, y=47.0)

        assert selected == 7
        assert data.client.timeseries_calls == 1
        assert state.checked_combinations() == {("BH-7", 0.0), ("BH-7", 1.0)}

        counts = _stage_counts(perf_log)
        assert counts.get("selection.update") == 1
        assert counts.get("fill_site.layout_notify") == 1
        assert counts.get("fill_site.version_notify") == 1
        assert counts.get("borehole.stream_event") == 1
        summaries = [line for line in perf_log if line.startswith("event=op.summary ")]
        assert len(summaries) == 1 and "operation=borehole.select" in summaries[0]

    def test_map_tap_without_profiling_still_selects(self, monkeypatch) -> None:
        monkeypatch.setattr(profiling, "_ENABLED", False)
        _data, _map_state, state, views = self._build_views()
        _left, _mid, _right, on_map_tap, _fetch_site = views

        selected = on_map_tap(x=11.0, y=47.0)

        assert selected == 7
        assert state.checked_combinations() == {("BH-7", 0.0), ("BH-7", 1.0)}
        assert perf_summary.current() is None


def test_stage_budget_covers_expected_borehole_select_stages(perf_log: list[str]) -> None:
    """All expected ``borehole.select`` stages must fit into one summary."""
    assert perf_summary._MAX_STAGE_LINES == 16
    assert len(EXPECTED_BOREHOLE_SELECT_STAGES) <= perf_summary._MAX_STAGE_LINES

    with perf_summary.measure_operation("borehole.select"):
        for name in EXPECTED_BOREHOLE_SELECT_STAGES:
            with perf_summary.stage(name):
                pass

    counts = _stage_counts(perf_log)
    for name in EXPECTED_BOREHOLE_SELECT_STAGES:
        assert counts.get(name) == 1, f"stage {name} missing from {counts}"
