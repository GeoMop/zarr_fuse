"""Dataset switching must not reuse a stale background map-click client."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from dashboard import composed
from dashboard.plot_selection import SelectionState


class _FakeWidget:
    def __init__(self, *args, **kwargs) -> None:
        self.objects = list(args)
        self.styles = dict(kwargs.get("styles") or {})
        self.visible = kwargs.get("visible", True)
        self.object = kwargs.get("object", "")
        self.options = kwargs.get("options", [])
        self.width = kwargs.get("width")
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
        for callback, names in self._watchers:
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
        self.cleared = 0

    def get_views(self) -> dict:
        return {"A": {"source": {"uri": "a"}}, "B": {"source": {"uri": "b"}}}

    def get_view(self, view_name: str) -> dict:
        return {
            "source": {"uri": view_name},
            "defaults": {},
            "schema_display": {},
            "schema": {"fields": {}},
            "visualization": {},
        }

    def get_structure(self, view_name: str) -> dict:
        return {}

    def get_variables(self, view_name: str, group_path: str) -> dict:
        return {}

    def get_variable_metadata(self, view_name: str, group_path: str, variable: str) -> dict:
        return {}

    def clear_cache(self) -> None:
        self.cleared += 1


class _FakeData:
    def __init__(self) -> None:
        self.client = _FakeClient()
        self.view_name = "A"
        self.group_path = "/"
        self.display_variable = ""


class _FakeLocalClient:
    def __init__(self, views_path: Path) -> None:
        self.views_path = Path(views_path)
        self.cache: dict[str, object] = {}


class _FakeTap:
    def __init__(self, x=None, y=None) -> None:
        self.x = x
        self.y = y
        self._watchers: list[object] = []
        self.param = SimpleNamespace(watch=self._watch)

    def _watch(self, callback, names) -> None:
        self._watchers.append(callback)

    def trigger(self) -> None:
        for callback in self._watchers:
            callback()


def _build_dashboard(monkeypatch) -> dict[str, object]:
    captured: dict[str, object] = {}
    background_clients: list[_FakeLocalClient] = []
    data = _FakeData()
    tap_calls: list[tuple[object, object]] = []
    captured["tap_calls"] = tap_calls

    fake_pn = SimpleNamespace(
        state=SimpleNamespace(
            curdoc=None,
            on_session_destroyed=lambda callback: None,
            onload=lambda callback: captured.__setitem__("populate", callback),
            add_timeout=lambda *args, **kwargs: None,
        ),
        Template=_FakeTemplate,
        Row=_FakeWidget,
        Column=_FakeWidget,
        Modal=_FakeWidget,
        indicators=SimpleNamespace(LoadingSpinner=_FakeWidget),
        pane=SimpleNamespace(Markdown=_FakeWidget, Alert=_FakeWidget),
        widgets=SimpleNamespace(Select=_FakeWidget, Button=_FakeWidget),
    )

    def fake_tap(**kwargs):
        tap = _FakeTap(**kwargs)
        captured["tap_stream"] = tap
        return tap

    def fake_build_sidebar(*args, **kwargs):
        controller_content = _FakeWidget()
        store_selector = _FakeWidget(
            options={"A": "A", "B": "B"},
            value="A",
        )
        node_select = _FakeWidget(options={"/": "/"}, value="/")
        variable_selector = _FakeWidget(options=[], value=None)
        variable_metadata = _FakeWidget(visible=False)
        node_hint = _FakeWidget(visible=False)
        store_info = _FakeWidget()
        reload_button = _FakeWidget()
        captured["store_selector"] = store_selector
        return (
            controller_content,
            store_selector,
            node_select,
            variable_selector,
            variable_metadata,
            node_hint,
            store_info,
            reload_button,
        )

    def fake_build_plot_selection_panel(*args, **kwargs):
        return _FakeWidget(), SelectionState()

    def fake_build_map_view(data, tap_stream, *, map_payload=None):
        return _FakeWidget(), {
            "lats": [47.0],
            "lons": [11.0],
            "marker_meta": [{"entity_index": 7, "site_id": "BH-7"}],
        }

    def fake_build_timeseries_views(
        data, map_state, selection_state=None, render_spinner=None, **kwargs
    ):
        captured["map_click_client"] = kwargs["map_click_client"]

        def fake_on_map_tap(x, y, loading=None, doc=None):
            tap_calls.append((x, y))
            return None

        return (
            _FakeWidget(),
            _FakeWidget(),
            _FakeWidget(),
            fake_on_map_tap,
            lambda *args, **kwargs: None,
        )

    def fake_local_client(views_path):
        client = _FakeLocalClient(views_path)
        background_clients.append(client)
        return client

    monkeypatch.setattr(composed, "pn", fake_pn)
    monkeypatch.setattr(composed, "streams", SimpleNamespace(Tap=fake_tap))
    monkeypatch.setattr(composed, "find_view_file", lambda: Path("views.yaml"))
    monkeypatch.setattr(composed, "get_default_endpoint_name", lambda path: "A")
    monkeypatch.setattr(composed, "load_views", lambda path: {"A": object(), "B": object()})
    monkeypatch.setattr(composed, "load_data", lambda *args, **kwargs: data)
    monkeypatch.setattr(composed, "LocalClient", fake_local_client)
    monkeypatch.setattr(composed, "_placeholder_root", lambda *args, **kwargs: _FakeWidget())
    monkeypatch.setattr(composed, "_set_root", lambda *args, **kwargs: None)
    monkeypatch.setattr(composed, "_set_root_error", lambda *args, **kwargs: None)
    monkeypatch.setattr(composed, "_map_pane", lambda content: content)
    monkeypatch.setattr(composed, "_line_pane", lambda content: content)
    monkeypatch.setattr(composed, "_report_error", lambda *args, **kwargs: None)
    monkeypatch.setattr(composed, "_flatten_nodes", lambda structure: [("root", "/")])
    monkeypatch.setattr(composed, "build_sidebar", fake_build_sidebar)
    monkeypatch.setattr(composed, "resolve_available_dimensions", lambda **kwargs: {})
    monkeypatch.setattr(
        composed, "build_plot_selection_panel", fake_build_plot_selection_panel
    )
    monkeypatch.setattr(composed, "build_map_view", fake_build_map_view)
    monkeypatch.setattr(composed, "build_timeseries_views", fake_build_timeseries_views)

    composed.build_dashboard()
    return captured


def test_dataset_switch_replaces_background_client(monkeypatch) -> None:
    captured = _build_dashboard(monkeypatch)
    asyncio.run(captured["populate"]())

    factory = captured["map_click_client"]
    store_selector = captured["store_selector"]

    client_a = factory()
    client_a.cache["A"] = "obsolete"

    store_selector.value = "B"
    client_b = factory()
    assert client_b is not client_a
    assert client_b.cache == {}

    store_selector.value = "A"
    client_a_again = factory()
    assert client_a_again is not client_a
    assert client_a_again is not client_b
    assert client_a_again.cache == {}


@pytest.mark.parametrize(
    ("x", "y"),
    [(None, None), (11.0, None), (None, 47.0)],
)
def test_incomplete_interactive_tap_does_not_request_default_site(
    monkeypatch, x, y
) -> None:
    captured = _build_dashboard(monkeypatch)
    asyncio.run(captured["populate"]())

    tap_stream = captured["tap_stream"]
    tap_calls = captured["tap_calls"]
    assert tap_calls == []

    tap_stream.x = x
    tap_stream.y = y
    tap_stream.trigger()

    assert tap_calls == []
