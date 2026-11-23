"""Regression tests for asynchronous variable switching in the dashboard."""

from __future__ import annotations

import asyncio
import threading
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from dashboard import composed, multi_time_views
from dashboard.plot_selection import SelectionState

_TIMES = pd.date_range("2020-01-01", periods=3)
_VARIABLE_OPTIONS = {"old": "u", "new": "u", "new2": "u"}


class _FakeWidget:
    def __init__(self, *args, **kwargs) -> None:
        self.objects = list(args)
        self.styles = dict(kwargs.get("styles") or {})
        self.visible = kwargs.get("visible", True)
        self.object = kwargs.get("object", "")
        self.options = kwargs.get("options", [])
        self.width = kwargs.get("width")
        self.name = kwargs.get("name")
        self._kwargs = kwargs
        self._value = kwargs.get("value")
        self._watchers: list[tuple[object, set[str]]] = []
        self._click_handlers: list[object] = []
        self.param = SimpleNamespace(watch=self._watch)

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, new_value) -> None:
        old_value = self._value
        self._value = new_value
        if old_value == new_value:
            return
        event = SimpleNamespace(new=new_value, old=old_value)
        for callback, names in list(self._watchers):
            if "value" in names:
                callback(event)

    def _watch(self, callback, names) -> None:
        self._watchers.append((callback, set(names)))

    def on_click(self, callback) -> None:
        self._click_handlers.append(callback)


class _FakeTemplate:
    def __init__(self, *args, **kwargs) -> None:
        self.nb_template = SimpleNamespace(globals={})
        self.panels: dict[str, object] = {}

    def add_panel(self, name: str, panel: object) -> None:
        self.panels[name] = panel


class _FakeClient:
    def __init__(self) -> None:
        self.views_path = Path("views.yaml")

    def get_views(self) -> dict:
        return {"A": {"source": {"uri": "a"}}, "B": {"source": {"uri": "b"}}}

    def get_view(self, view_name: str) -> dict:
        return {
            "source": {"uri": view_name},
            "defaults": {},
            "schema_display": {},
            "schema": {"fields": {}},
            "visualization": {
                "timeseries": {"middle_window_days": 30, "right_window_hours": 24}
            },
        }

    def get_structure(self, view_name: str) -> dict:
        return {}

    def get_variables(self, view_name: str, group_path: str) -> dict:
        return dict(_VARIABLE_OPTIONS)

    def get_variable_metadata(self, view_name: str, group_path: str, variable: str) -> dict:
        return {}

    def clear_cache(self) -> None:
        return None


class _FakeData:
    def __init__(self) -> None:
        self.client = _FakeClient()
        self.view_name = "A"
        self.group_path = "/"
        self.display_variable = ""


class _FakeDocument:
    def __init__(self) -> None:
        self.session_context = None
        self.callbacks: list[object] = []
        self.timeouts: list[tuple[object, int]] = []

    def add_next_tick_callback(self, callback):
        self.callbacks.append(callback)
        return callback

    def add_timeout_callback(self, callback, timeout):
        self.timeouts.append((callback, timeout))
        return callback

    def drain(self) -> None:
        while self.callbacks:
            callback = self.callbacks.pop(0)
            callback()


class _FakeTap:
    def __init__(self, x=None, y=None) -> None:
        self.x = x
        self.y = y
        self._watchers: list[object] = []
        self.param = SimpleNamespace(watch=self._watch)

    def _watch(self, callback, names) -> None:
        self._watchers.append(callback)

    def trigger(self) -> None:
        for callback in list(self._watchers):
            callback()


class _Gate:
    def __init__(self) -> None:
        self.entered = threading.Event()
        self.release = threading.Event()


def _payload(entity_index: int, value: float) -> dict:
    return {
        "status": "success",
        "times": _TIMES,
        "depths": [0.0, 1.0],
        "series": [[value, value + 1, value + 2], [value + 10, value + 11, value + 12]],
        "borehole_index": entity_index,
    }


def _build_dashboard(monkeypatch, fetch, *, real_map_clicks: bool = False) -> dict[str, object]:
    captured: dict[str, object] = {
        "clients": [],
        "calls": [],
        "refresh_count": 0,
        "map_build_count": 0,
        "tap_calls": [],
        "widgets": [],
    }
    data = _FakeData()
    doc = _FakeDocument()
    captured["data"] = data
    captured["doc"] = doc

    class _Widget(_FakeWidget):
        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)
            captured["widgets"].append(self)

    fake_pn = SimpleNamespace(
        state=SimpleNamespace(
            curdoc=doc,
            on_session_destroyed=lambda callback: None,
            onload=lambda callback: captured.__setitem__("populate", callback),
            add_timeout=lambda *args, **kwargs: None,
        ),
        Template=_FakeTemplate,
        Row=_Widget,
        Column=_Widget,
        indicators=SimpleNamespace(LoadingSpinner=_Widget),
        pane=SimpleNamespace(Markdown=_Widget, Alert=_Widget),
        widgets=SimpleNamespace(Select=_Widget, Button=_Widget),
    )

    def fake_tap(**kwargs):
        tap = _FakeTap(**kwargs)
        captured["tap_stream"] = tap
        return tap

    def fake_build_sidebar(*args, **kwargs):
        store_selector = _Widget(options={"A": "A", "B": "B"}, value="A")
        node_select = _Widget(options={"/": "/", "child": "/child"}, value="/")
        variable_selector = _Widget(options=[], value=None)
        captured["store_selector"] = store_selector
        captured["node_select"] = node_select
        captured["variable_selector"] = variable_selector
        captured["loading_indicator"] = kwargs["loading_indicator"]
        captured["timeseries_loading"] = kwargs["timeseries_loading"]
        captured["rendering_status"] = kwargs["rendering_status"]
        return (
            _Widget(),
            store_selector,
            node_select,
            variable_selector,
            _Widget(visible=False),
            _Widget(visible=False),
            _Widget(),
            _Widget(),
        )

    def fake_build_plot_selection_panel(*args, **kwargs):
        state = SelectionState()
        state.add_site(
            entity_index=7,
            site_id="BH-7",
            depths=[0.0, 1.0],
            series=[[1, 2, 3], [4, 5, 6]],
            times=_TIMES,
        )
        state.add_site(
            entity_index=8,
            site_id="BH-8",
            depths=[0.0, 1.0],
            series=[[7, 8, 9], [10, 11, 12]],
            times=_TIMES,
        )
        captured["selection_state"] = state
        return _Widget(), state

    def fake_build_map_view(*args, **kwargs):
        captured["map_build_count"] += 1
        return _Widget(), {
            "lats": [47.0, 47.0],
            "lons": [11.0, 11.0],
            "marker_meta": [
                {"entity_index": 7, "site_id": "BH-7"},
                {"entity_index": 8, "site_id": "BH-8"},
            ],
        }

    def fake_build_timeseries_views(*args, **kwargs):
        captured["refresh_count"] += 1
        if real_map_clicks:
            left, mid, right, on_map_tap, fetch_site = multi_time_views.build_timeseries_views(
                *args, **kwargs
            )
            captured["map_click_state"] = kwargs["map_click_state"]

            def interactive_tap(x, y, loading=None):
                if x is not None and y is not None:
                    return on_map_tap(x, y, loading=loading, doc=doc)

            return left, mid, right, interactive_tap, fetch_site

        def fake_on_map_tap(x, y, loading=None, doc=None):
            captured["tap_calls"].append((x, y))

        return (
            _Widget(),
            _Widget(),
            _Widget(),
            fake_on_map_tap,
            lambda *args, **kwargs: None,
        )

    def fake_local_client(views_path):
        client = SimpleNamespace(views_path=Path(views_path))
        if real_map_clicks:
            def get_timeseries_data(view_name, *, group_path, lat, lon, variable, entity_index):
                return fetch(client, view_name, group_path, variable, entity_index)

            client.get_timeseries_data = get_timeseries_data
        captured["clients"].append(client)
        return client

    monkeypatch.setattr(composed, "pn", fake_pn)
    monkeypatch.setattr(composed, "streams", SimpleNamespace(Tap=fake_tap))
    monkeypatch.setattr(composed, "find_view_file", lambda: Path("views.yaml"))
    monkeypatch.setattr(composed, "get_default_endpoint_name", lambda path: "A")
    monkeypatch.setattr(composed, "load_views", lambda path: {"A": object()})
    monkeypatch.setattr(composed, "load_data", lambda *args, **kwargs: data)
    monkeypatch.setattr(composed, "LocalClient", fake_local_client)
    monkeypatch.setattr(composed, "fetch_registered_site_payload", fetch)
    monkeypatch.setattr(
        composed,
        "load_view_config",
        lambda *args, **kwargs: SimpleNamespace(
            defaults=SimpleNamespace(display_variable="old")
        ),
    )
    monkeypatch.setattr(composed, "_placeholder_root", lambda *args, **kwargs: _Widget())
    monkeypatch.setattr(composed, "_set_root", lambda *args, **kwargs: None)
    monkeypatch.setattr(composed, "_set_root_error", lambda *args, **kwargs: None)
    monkeypatch.setattr(composed, "_map_pane", lambda content: content)
    monkeypatch.setattr(composed, "_line_pane", lambda content: content)
    monkeypatch.setattr(composed, "_report_error", lambda *args, **kwargs: None)
    monkeypatch.setattr(composed, "_flatten_nodes", lambda structure: [("root", "/")])
    monkeypatch.setattr(composed, "build_sidebar", fake_build_sidebar)
    monkeypatch.setattr(composed, "resolve_available_dimensions", lambda **kwargs: {})
    monkeypatch.setattr(composed, "build_plot_selection_panel", fake_build_plot_selection_panel)
    monkeypatch.setattr(composed, "build_map_view", fake_build_map_view)
    monkeypatch.setattr(composed, "build_timeseries_views", fake_build_timeseries_views)

    composed.build_dashboard()
    asyncio.run(captured["populate"]())
    captured["tap_calls"].clear()
    captured["clients"].clear()
    return captured


async def _wait_for(predicate, timeout: float = 2.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError("timed out waiting for async variable switch")
        await asyncio.sleep(0.001)


async def _wait_for_callbacks(doc: _FakeDocument, minimum: int) -> None:
    await _wait_for(lambda: len(doc.callbacks) >= minimum)


def _select(captured: dict[str, object], label: str) -> None:
    captured["variable_selector"].value = label


def test_background_fetch_applies_once_on_document_thread(monkeypatch) -> None:
    """Only the replacement plots render the newly loaded batch."""
    gate = _Gate()

    def fetch(client, view_name, group_path, variable, entity_index):
        captured["calls"].append((variable, entity_index))
        if variable == "new" and entity_index == 7:
            gate.entered.set()
            assert gate.release.wait(timeout=5)
        return _payload(entity_index, 10.0 + entity_index)

    captured = _build_dashboard(monkeypatch, fetch)
    state = captured["selection_state"]
    doc = captured["doc"]
    refresh_before = captured["refresh_count"]
    map_builds_before = captured["map_build_count"]
    old_plot_updates = []
    state.param.watch(old_plot_updates.append, "version")

    async def scenario() -> None:
        _select(captured, "new (u)")
        assert not gate.entered.is_set(), "switch callback must not block on fetch"

        await _wait_for(gate.entered.is_set)
        gate.release.set()
        await _wait_for_callbacks(doc, 1)

        assert [s["series"][0][0] for s in state.sites] == [1, 7], (
            "worker must not mutate SelectionState"
        )
        doc.drain()

    asyncio.run(scenario())

    assert [s["series"][0][0] for s in state.sites] == [17, 18]
    assert captured["refresh_count"] == refresh_before + 1
    assert captured["map_build_count"] == map_builds_before
    assert old_plot_updates == []
    assert captured["calls"] == [("new", 7), ("new", 8)]
    assert captured["clients"] and all(
        client is not captured["data"].client for client in captured["clients"]
    )
    assert all(client is captured["clients"][0] for client in captured["clients"])


def test_failed_site_is_cleared_without_losing_identity_or_checks(monkeypatch) -> None:
    def fetch(client, view_name, group_path, variable, entity_index):
        if entity_index == 8:
            return None
        return _payload(entity_index, 20.0)

    captured = _build_dashboard(monkeypatch, fetch)
    state = captured["selection_state"]
    doc = captured["doc"]
    checked_before = state.checked_combinations()
    depths_before = list(state.sites[1]["depths"])

    async def scenario() -> None:
        _select(captured, "new (u)")
        await _wait_for_callbacks(doc, 1)
        doc.drain()

    asyncio.run(scenario())

    assert state.sites[1]["series"] == []
    assert len(state.sites[1]["times"]) == 0
    assert list(state.sites[1]["depths"]) == depths_before
    assert state.checked_combinations() == checked_before
    assert [(s["entity_index"], s["site_id"]) for s in state.sites] == [
        (7, "BH-7"),
        (8, "BH-8"),
    ]
    status = next(
        widget for widget in captured["widgets"]
        if widget._kwargs.get("alert_type") == "danger"
    )
    assert "1 site" in status.object


@pytest.mark.parametrize("already_queued", [False, True])
def test_superseded_switch_result_is_discarded(monkeypatch, already_queued: bool) -> None:
    """Supersession covers running workers and already queued results."""
    gate = _Gate()

    def fetch(client, view_name, group_path, variable, entity_index):
        if variable == "new" and entity_index == 7 and not already_queued:
            gate.entered.set()
            assert gate.release.wait(timeout=5)
        return _payload(entity_index, 30.0 if variable == "new" else 40.0)

    captured = _build_dashboard(monkeypatch, fetch)
    state = captured["selection_state"]
    doc = captured["doc"]

    async def scenario() -> None:
        try:
            _select(captured, "new (u)")
            if already_queued:
                await _wait_for_callbacks(doc, 1)
            else:
                await _wait_for(gate.entered.is_set)

            _select(captured, "new2 (u)")
            if already_queued:
                doc.drain()
                assert [s["series"][0][0] for s in state.sites] == [1, 7]
                assert captured["loading_indicator"].visible
                assert captured["timeseries_loading"].visible
            await _wait_for_callbacks(doc, 1)
            doc.drain()
        finally:
            gate.release.set()

    asyncio.run(scenario())

    assert captured["data"].display_variable == "new2"
    assert [s["series"][0][0] for s in state.sites] == [40.0, 40.0]
    assert not captured["loading_indicator"].visible
    assert not captured["timeseries_loading"].visible
    assert len(captured["clients"]) == 2
    assert captured["clients"][0] is not captured["clients"][1]


def test_map_clicks_are_blocked_while_variable_switch_is_active(monkeypatch) -> None:
    gate = _Gate()

    def fetch(client, view_name, group_path, variable, entity_index):
        if entity_index == 7:
            gate.entered.set()
            assert gate.release.wait(timeout=5)
        return _payload(entity_index, 50.0)

    captured = _build_dashboard(monkeypatch, fetch)
    doc = captured["doc"]
    tap_stream = captured["tap_stream"]

    async def scenario() -> None:
        _select(captured, "new (u)")
        await _wait_for(gate.entered.is_set)

        tap_stream.x = 11.0
        tap_stream.y = 47.0
        tap_stream.trigger()
        assert captured["tap_calls"] == []

        gate.release.set()
        await _wait_for_callbacks(doc, 1)
        doc.drain()

        tap_stream.trigger()
        assert captured["tap_calls"] == [(11.0, 47.0)]

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("selector", "value"),
    [("store_selector", "B"), ("node_select", "/child"), ("variable_selector", "new (u)")],
)
def test_context_change_releases_map_click_guard(monkeypatch, selector: str, value: str) -> None:
    """A new context can load while an old click waits, and owns its guard and spinner."""
    old, new = _Gate(), _Gate()
    keeps_sites = selector == "variable_selector"

    def fetch(client, view_name, group_path, variable, entity_index):
        clients = captured["clients"]
        if keeps_sites and len(clients) > 1 and client is clients[1]:
            return _payload(entity_index, 15.0)
        gate = old if client is clients[0] else new
        gate.entered.set()
        assert gate.release.wait(timeout=5)
        return _payload(entity_index, 10.0 if gate is old else 20.0)

    captured = _build_dashboard(monkeypatch, fetch, real_map_clicks=True)
    state = captured["selection_state"]
    doc = captured["doc"]
    tap = captured["tap_stream"]
    tap.x, tap.y = 11.0, 47.0
    spinner = captured["timeseries_loading"]
    map_click = captured["map_click_state"]

    async def scenario() -> None:
        try:
            tap.trigger()
            await _wait_for(old.entered.is_set)
            captured[selector].value = value
            if keeps_sites:
                await _wait_for_callbacks(doc, 1)
            doc.drain()
            assert not spinner.visible
            assert not map_click.active
            if keeps_sites:
                assert state.sites[0]["series"][0][0] == 15.0
            else:
                assert state.sites == []

            tap.trigger()
            await _wait_for(new.entered.is_set)
            assert state.sites[0]["entity_index"] == 7
            assert captured["clients"][0] is not captured["clients"][-1]

            old.release.set()
            await _wait_for_callbacks(doc, 1)
            doc.drain()
            assert map_click.active
            assert spinner.visible
            if keeps_sites:
                assert state.sites[0]["series"][0][0] == 15.0
            else:
                assert state.sites[0]["series"] == []

            new.release.set()
            await _wait_for_callbacks(doc, 2)
            doc.drain()
            assert state.sites[0]["series"][0][0] == 20.0
            assert not map_click.active
            assert not spinner.visible
        finally:
            old.release.set()
            new.release.set()

    asyncio.run(scenario())


def test_map_fetch_failure_reports_integer_site_id(monkeypatch) -> None:
    """A real map-fetch failure produces an alert containing its integer site ID."""
    captured = _build_dashboard(
        monkeypatch, lambda *args: {"status": "error", "reason": "unavailable"}, real_map_clicks=True
    )
    doc = captured["doc"]
    tap = captured["tap_stream"]
    tap.x, tap.y = 11.0, 47.0

    async def scenario() -> None:
        tap.trigger()
        await _wait_for_callbacks(doc, 1)
        doc.drain()

    asyncio.run(scenario())
    status = captured["rendering_status"]
    assert status.visible
    assert "data not available at 1 site(s) (7)" in status.object
    assert not captured["map_click_state"].active
    assert not captured["timeseries_loading"].visible


def test_rapid_a_b_a_switch_applies_only_final_a(monkeypatch) -> None:
    """A queued result from the first A cannot apply once A is selected again."""
    first_a, b, final_a = _Gate(), _Gate(), _Gate()

    def fetch(client, view_name, group_path, variable, entity_index):
        if variable == "new2":
            gate, result = b, 30.0
        elif client is captured["clients"][0]:
            gate, result = first_a, 20.0
        else:
            gate, result = final_a, 40.0
        gate.entered.set()
        assert gate.release.wait(timeout=5)
        return _payload(entity_index, result)

    captured = _build_dashboard(monkeypatch, fetch)
    state = captured["selection_state"]
    doc = captured["doc"]
    initial_refreshes = captured["refresh_count"]

    async def scenario() -> None:
        try:
            _select(captured, "new (u)")
            await _wait_for(first_a.entered.is_set)
            first_a.release.set()
            await _wait_for_callbacks(doc, 1)

            _select(captured, "new2 (u)")
            await _wait_for(b.entered.is_set)
            _select(captured, "new (u)")
            await _wait_for(final_a.entered.is_set)
            doc.drain()
            assert [s["series"][0][0] for s in state.sites] == [1, 7]
            assert captured["refresh_count"] == initial_refreshes
            assert captured["timeseries_loading"].visible

            final_a.release.set()
            await _wait_for_callbacks(doc, 1)
            doc.drain()
            assert [s["series"][0][0] for s in state.sites] == [40.0, 40.0]
        finally:
            first_a.release.set()
            b.release.set()
            final_a.release.set()

    asyncio.run(scenario())
    doc.drain()
    assert captured["data"].display_variable == "new"
    assert [s["series"][0][0] for s in state.sites] == [40.0, 40.0]
    assert captured["refresh_count"] == initial_refreshes + 1
    assert not captured["timeseries_loading"].visible


@pytest.mark.parametrize(
    ("selector", "value", "view_name", "group_path"),
    [("store_selector", "B", "B", "/"), ("node_select", "/child", "A", "/child")],
)
def test_context_change_releases_switch_while_worker_is_blocked(
    monkeypatch, selector, value, view_name, group_path
) -> None:
    """A context change frees the new map without waiting for old I/O."""
    gate = _Gate()

    def fetch(client, view_name, group_path, variable, entity_index):
        captured["calls"].append(entity_index)
        gate.entered.set()
        assert gate.release.wait(timeout=5)
        return _payload(entity_index, 50.0)

    captured = _build_dashboard(monkeypatch, fetch)
    doc = captured["doc"]
    tap = captured["tap_stream"]

    async def scenario() -> None:
        try:
            _select(captured, "new (u)")
            await _wait_for(gate.entered.is_set)
            captured[selector].value = value
            doc.drain()
            assert not captured["timeseries_loading"].visible
            assert not captured["loading_indicator"].visible
            tap.x, tap.y = 11.0, 47.0
            tap.trigger()
            assert captured["tap_calls"] == [(11.0, 47.0)]
        finally:
            gate.release.set()

    asyncio.run(scenario())
    doc.drain()
    assert (captured["data"].view_name, captured["data"].group_path) == (view_name, group_path)
    assert captured["selection_state"].sites == []
    assert captured["calls"] == [7]


def test_switch_checks_depths_for_borehole_pending_from_map_click(monkeypatch) -> None:
    """The switch can complete the first fill after invalidating a map load."""
    captured = _build_dashboard(monkeypatch, lambda *args: _payload(args[-1], 50.0))
    state = captured["selection_state"]
    state.set_checked("BH-7", 0.0, False)
    state.register_site(9, "BH-9")
    checked_before = state.checked_combinations()
    doc = captured["doc"]

    async def scenario() -> None:
        _select(captured, "new (u)")
        await _wait_for_callbacks(doc, 1)
        doc.drain()

    asyncio.run(scenario())
    assert state.checked_combinations() == checked_before | {("BH-9", 0.0), ("BH-9", 1.0)}


def test_background_client_setup_failure_releases_loading_state(monkeypatch) -> None:
    """Client setup errors are reported and release both loading indicators."""
    captured = _build_dashboard(monkeypatch, lambda *args: None)

    def fail_to_create_client(views_path):
        raise OSError("client setup failed")

    monkeypatch.setattr(composed, "LocalClient", fail_to_create_client)
    doc = captured["doc"]

    async def scenario() -> None:
        _select(captured, "new (u)")
        await _wait_for_callbacks(doc, 1)
        doc.drain()

    asyncio.run(scenario())
    assert not captured["loading_indicator"].visible
    assert not captured["timeseries_loading"].visible
    status = next(widget for widget in captured["widgets"] if widget._kwargs.get("alert_type") == "danger")
    assert status.visible
    assert "client setup failed" in status.object
