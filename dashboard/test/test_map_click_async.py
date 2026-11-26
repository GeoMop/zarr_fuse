"""Regression tests for asynchronous interactive map-click loading."""

from __future__ import annotations

import asyncio
import logging
import threading
from types import SimpleNamespace

import pandas as pd
import pytest

from dashboard import perf_summary, profiling
from dashboard.multi_time_views import MapClickOperation, build_timeseries_views
from dashboard.plot_selection import SelectionState

_TIMES = pd.date_range("2020-01-01", periods=3)


@pytest.fixture
def perf_log(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    monkeypatch.setattr(profiling, "_ENABLED", True)
    captured: list[str] = []

    class _CapturingHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            captured.append(record.getMessage())

    handler = _CapturingHandler()
    handler.setLevel(logging.DEBUG)
    profiling.logger.addHandler(handler)
    try:
        yield captured
    finally:
        profiling.logger.removeHandler(handler)


class _FakeClient:
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
        self.current_clusters: list[dict] = []


class _Gate:
    def __init__(self) -> None:
        self.entered = threading.Event()
        self.release = threading.Event()


class _BackgroundClient:
    def __init__(self, responses: dict[int, dict], gates: dict[int, _Gate] | None = None):
        self.responses = responses
        self.gates = gates or {}
        self.calls: list[int] = []
        self._active = 0
        self.max_active = 0
        self._lock = threading.Lock()

    def get_timeseries_data(
        self,
        view_name: str,
        group_path: str,
        lat: float,
        lon: float,
        variable: str,
        entity_index: int,
    ) -> dict:
        with self._lock:
            self.calls.append(int(entity_index))
            self._active += 1
            self.max_active = max(self.max_active, self._active)
        try:
            gate = self.gates.get(int(entity_index))
            if gate is not None:
                gate.entered.set()
                if not gate.release.wait(timeout=5):
                    raise AssertionError("test did not release map-click fetch")
            response = self.responses[int(entity_index)]
            if isinstance(response, BaseException):
                raise response
            return response
        finally:
            with self._lock:
                self._active -= 1


class _FakeDocument:
    def __init__(self) -> None:
        self.callbacks: list[object] = []

    def add_next_tick_callback(self, callback):
        self.callbacks.append(callback)
        return callback

    def add_timeout_callback(self, callback, timeout):
        raise AssertionError("interactive map clicks must not schedule a timeout")

    def run_next_tick(self) -> None:
        callback = self.callbacks.pop(0)
        callback()

    def drain(self) -> None:
        while self.callbacks:
            self.run_next_tick()


def _response(entity_index: int, value: float = 1.0) -> dict:
    return {
        "status": "success",
        "times": _TIMES,
        "depths": [0.0, 1.0],
        "series": [
            [value, value + 1, value + 2],
            [value * 2, value * 2 + 1, value * 2 + 2],
        ],
        "borehole_index": entity_index,
        "borehole_name": f"BH-{entity_index}",
    }


def _make_views(
    client: _BackgroundClient,
    generation,
    *,
    clusters: list[dict] | None = None,
    failures: list[list[int]] | None = None,
):
    data = _FakeData()
    data.current_clusters = clusters or []
    state = SelectionState()
    operation = MapClickOperation()
    map_state = {
        "lats": [47.0, 47.0],
        "lons": [11.0, 11.0],
        "marker_meta": [
            {"entity_index": 7, "site_id": "BH-7"},
            {"entity_index": 8, "site_id": "BH-8"},
        ],
    }
    views = build_timeseries_views(
        data,
        map_state,
        state,
        map_click_client=lambda: client,
        map_click_state=operation,
        map_click_generation=generation,
        map_click_failure=(failures.append if failures is not None else None),
    )
    return data, state, operation, views


async def _wait_for_event(event: threading.Event) -> None:
    await asyncio.wait_for(asyncio.to_thread(event.wait), timeout=2)


async def _wait_for_callbacks(doc: _FakeDocument, minimum: int) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 2
    while len(doc.callbacks) < minimum:
        if loop.time() > deadline:
            raise AssertionError(f"expected at least {minimum} document callbacks")
        await asyncio.sleep(0.001)


def test_interactive_click_returns_before_fetch_completes() -> None:
    async def scenario() -> None:
        gate = _Gate()
        client = _BackgroundClient({7: _response(7)}, {7: gate})
        _data, state, operation, views = _make_views(client, lambda: 0)
        doc = _FakeDocument()
        on_map_tap = views[3]

        selected = on_map_tap(11.0, 47.0, doc=doc)

        assert selected == 7
        assert operation.active is True
        assert state.sites[0]["series"] == []
        assert not gate.entered.is_set(), "fetch must not block the click callback"

        await _wait_for_event(gate.entered)
        gate.release.set()
        await _wait_for_callbacks(doc, 2)
        doc.drain()
        assert operation.active is False

    asyncio.run(scenario())


def test_cluster_registers_all_rows_and_fills_progressively() -> None:
    async def scenario() -> None:
        gates = {7: _Gate(), 8: _Gate()}
        client = _BackgroundClient({7: _response(7), 8: _response(8)}, gates)
        clusters = [{"lon": 11.0, "lat": 47.0, "entity_indices": [7, 8], "radius": 0.01}]
        _data, state, operation, views = _make_views(
            client, lambda: 0, clusters=clusters
        )
        doc = _FakeDocument()
        on_map_tap = views[3]

        selected = on_map_tap(11.0, 47.0, doc=doc)

        assert selected == 7
        assert [s["entity_index"] for s in state.sites] == [7, 8]
        assert all(s["series"] == [] for s in state.sites)

        await _wait_for_event(gates[7].entered)
        gates[7].release.set()
        await _wait_for_callbacks(doc, 1)
        doc.run_next_tick()

        assert state.sites[0]["series"] != []
        assert state.sites[1]["series"] == []
        assert operation.active is True

        await _wait_for_event(gates[8].entered)
        gates[8].release.set()
        await _wait_for_callbacks(doc, 2)
        doc.drain()

        assert all(s["series"] != [] for s in state.sites)
        assert operation.active is False
        assert client.max_active == 1
        assert client.calls == [7, 8]

    asyncio.run(scenario())


def test_repeated_click_is_ignored_while_active() -> None:
    async def scenario() -> None:
        gate = _Gate()
        client = _BackgroundClient({7: _response(7)}, {7: gate})
        _data, state, operation, views = _make_views(client, lambda: 0)
        doc = _FakeDocument()
        on_map_tap = views[3]

        assert on_map_tap(11.0, 47.0, doc=doc) == 7
        await _wait_for_event(gate.entered)

        assert on_map_tap(11.0, 47.0, doc=doc) is None
        assert client.calls == [7]
        assert len(state.sites) == 1
        assert operation.active is True

        gate.release.set()
        await _wait_for_callbacks(doc, 2)
        doc.drain()
        assert operation.active is False

    asyncio.run(scenario())


def test_reload_invalidates_inflight_request_and_replaces_client() -> None:
    async def scenario() -> None:
        old_gate = _Gate()
        old_client = _BackgroundClient({7: _response(7, value=10.0)}, {7: old_gate})
        new_client = _BackgroundClient({7: _response(7, value=20.0)})
        clients = [old_client]
        generation = [0]
        data = _FakeData()
        state = SelectionState()
        operation = MapClickOperation()
        map_state = {
            "lats": [47.0],
            "lons": [11.0],
            "marker_meta": [{"entity_index": 7, "site_id": "BH-7"}],
        }
        views = build_timeseries_views(
            data,
            map_state,
            state,
            map_click_client=lambda: clients[0],
            map_click_state=operation,
            map_click_generation=lambda: generation[0],
        )
        doc = _FakeDocument()
        on_map_tap = views[3]
        loading = SimpleNamespace(visible=False)

        assert on_map_tap(11.0, 47.0, loading=loading, doc=doc) == 7
        await _wait_for_event(old_gate.entered)

        generation[0] += 1
        loading.visible = True  # The foreground request now owns the spinner.
        clients[0] = new_client
        old_gate.release.set()
        await _wait_for_callbacks(doc, 1)
        doc.drain()

        assert state.sites[0]["series"] == [], "stale result must be discarded"
        assert operation.active is False
        assert loading.visible, "stale completion must leave the new operation's spinner alone"

        assert on_map_tap(11.0, 47.0, doc=doc) == 7
        await _wait_for_callbacks(doc, 2)
        doc.drain()

        assert state.sites[0]["series"] != []
        assert old_client.calls == [7]
        assert new_client.calls == [7]
        assert operation.active is False

    asyncio.run(scenario())


def test_result_is_applied_only_from_document_callback() -> None:
    async def scenario() -> None:
        gate = _Gate()
        client = _BackgroundClient({7: _response(7)}, {7: gate})
        _data, state, operation, views = _make_views(client, lambda: 0)
        doc = _FakeDocument()
        on_map_tap = views[3]

        on_map_tap(11.0, 47.0, doc=doc)
        await _wait_for_event(gate.entered)
        gate.release.set()
        await _wait_for_callbacks(doc, 1)

        assert state.sites[0]["series"] == [], "worker must not touch SelectionState"
        doc.run_next_tick()
        assert state.sites[0]["series"] != []

        await _wait_for_callbacks(doc, 1)
        doc.drain()
        assert operation.active is False

    asyncio.run(scenario())


def test_async_path_works_with_profiling_enabled(monkeypatch) -> None:
    monkeypatch.setattr(profiling, "_ENABLED", True)

    async def scenario() -> None:
        gate = _Gate()
        client = _BackgroundClient({7: _response(7)}, {7: gate})
        _data, state, operation, views = _make_views(client, lambda: 0)
        doc = _FakeDocument()
        on_map_tap = views[3]

        on_map_tap(11.0, 47.0, doc=doc)
        await _wait_for_event(gate.entered)
        gate.release.set()
        await _wait_for_callbacks(doc, 2)
        doc.drain()

        assert state.sites[0]["series"] != []
        assert operation.active is False

    asyncio.run(scenario())


def test_suspended_map_click_does_not_absorb_unrelated_activity(
    perf_log: list[str],
) -> None:
    async def scenario() -> None:
        gate = _Gate()
        client = _BackgroundClient({7: _response(7)}, {7: gate})
        _data, state, operation, views = _make_views(client, lambda: 0)
        doc = _FakeDocument()
        on_map_tap = views[3]

        on_map_tap(11.0, 47.0, doc=doc)
        await _wait_for_event(gate.entered)

        # The map-click coroutine is suspended in asyncio.to_thread().  This
        # activity must not land in the map-click operation's summary.
        with perf_summary.stage("unrelated.stage"):
            pass
        perf_summary.count("unrelated.count")

        gate.release.set()
        await _wait_for_callbacks(doc, 2)
        doc.drain()

        assert state.sites[0]["series"] != []
        assert operation.active is False

    asyncio.run(scenario())

    assert not any("unrelated.stage" in line for line in perf_log)
    assert not any("unrelated.count" in line for line in perf_log)
    assert any(
        "stage=borehole.select" in line and "status=ok" in line
        for line in perf_log
    )


@pytest.mark.parametrize("apply_failure", ["false", "exception"])
def test_apply_failure_is_reported_and_marks_operation_error(
    perf_log: list[str],
    monkeypatch: pytest.MonkeyPatch,
    apply_failure: str,
) -> None:
    async def scenario() -> None:
        gate = _Gate()
        client = _BackgroundClient({7: _response(7)}, {7: gate})
        failures: list[list[int]] = []
        _data, state, operation, views = _make_views(
            client, lambda: 0, failures=failures
        )
        if apply_failure == "false":
            monkeypatch.setattr(state, "fill_site", lambda *args, **kwargs: False)
        else:
            def _raise(*args, **kwargs):
                raise RuntimeError("apply boom")

            monkeypatch.setattr(state, "fill_site", _raise)

        doc = _FakeDocument()
        on_map_tap = views[3]

        on_map_tap(11.0, 47.0, doc=doc)
        await _wait_for_event(gate.entered)
        gate.release.set()
        await _wait_for_callbacks(doc, 2)
        doc.drain()

        assert failures == [[7]]
        assert state.sites[0]["series"] == []
        assert operation.active is False

    asyncio.run(scenario())

    assert any(
        "stage=borehole.select" in line and "status=error" in line
        for line in perf_log
    )
    assert not any(
        "stage=borehole.select" in line and "status=ok" in line
        for line in perf_log
    )


@pytest.mark.parametrize("failure", ["status", "exception"])
def test_fetch_error_reports_failure_and_cleans_up(failure: str) -> None:
    async def scenario() -> None:
        gate = _Gate()
        response = (
            {"status": "error", "reason": "boom"}
            if failure == "status"
            else RuntimeError("boom")
        )
        client = _BackgroundClient({7: response}, {7: gate})
        failures: list[list[int]] = []
        _data, state, operation, views = _make_views(
            client, lambda: 0, failures=failures
        )
        loading = SimpleNamespace(visible=False)
        doc = _FakeDocument()
        on_map_tap = views[3]

        on_map_tap(11.0, 47.0, loading=loading, doc=doc)
        await _wait_for_event(gate.entered)
        gate.release.set()
        await _wait_for_callbacks(doc, 1)
        doc.drain()

        assert failures == [[7]]
        assert operation.active is False
        assert loading.visible is False
        assert state.sites[0]["series"] == []

    asyncio.run(scenario())
