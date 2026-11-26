"""Regression tests for progressive first-session dashboard loading."""

from __future__ import annotations

import asyncio
import logging
import threading
from pathlib import Path
from time import perf_counter
from types import SimpleNamespace

import pytest
from bokeh.document import Document
from panel.io.state import set_curdoc
from zarr.storage import LocalStore

from dashboard import composed, profiling
from dashboard.data import DashboardData
from dashboard.plot_selection import SelectionState
from dashboard.test.test_right_timeseries import _curves


class _Read:
    def __init__(self, result):
        self.result = result
        self.entered = threading.Event()
        self.release = threading.Event()
        self.release.set()
        self.calls = []

    def __call__(self, client, *args, **kwargs):
        self.calls.append((client, threading.get_ident(), args, kwargs))
        self.entered.set()
        assert self.release.wait(5), "test did not release the read"
        if isinstance(self.result, Exception):
            raise self.result
        return self.result

    async def wait(self) -> None:
        assert await asyncio.to_thread(self.entered.wait, 2), "background read did not start"


@pytest.fixture
def startup(monkeypatch):
    view = {
        "source": {"uri": "local"},
        "defaults": {"default_site": "BH-8", "display_variable": "temp"},
        "schema_display": {"entity_name": "Site"},
        "schema": {"fields": {"entity": "entity", "vertical": "depth", "time": "time"}},
        "visualization": {
            "map": {"title": "Local map", "point_size": 5},
            "overlay": {"enabled": False, "tile_url": None},
            "timeseries": {"middle_window_days": 30, "right_window_hours": 24},
        },
    }
    reads = SimpleNamespace(
        structure=_Read({"children": [{"name": "sites", "path": "/sites"}]}),
        variables=_Read({"temp": "degC", "other": "K"}),
        metadata=_Read({"name": "temp", "unit": "degC"}),
        map=_Read({
            "lat": [47.0, 47.1], "lon": [11.0, 11.1], "values": [1, 2],
            "entities": ["BH-7", "BH-8"],
            "marker_meta": [{"entity_index": 7, "site_id": "BH-7"}, {"entity_index": 8, "site_id": "BH-8"}],
        }),
        site=_Read({
            "borehole_index": 8, "borehole_name": "BH-8", "depths": [0.0],
            "times": ["2025-01-01", "2025-01-02"], "series": [[2.0, 3.0]],
        }),
    )

    class Client:
        def __init__(self, views_path):
            self.views_path = views_path

        def get_views(self):
            return {"local": view}

        def get_view(self, _view_name):
            return view

        get_structure = lambda self, *args, **kwargs: reads.structure(self, *args, **kwargs)
        get_variables = lambda self, *args, **kwargs: reads.variables(self, *args, **kwargs)
        get_variable_metadata = lambda self, *args, **kwargs: reads.metadata(self, *args, **kwargs)
        get_map_data = lambda self, *args, **kwargs: reads.map(self, *args, **kwargs)
        get_timeseries_site_metadata = lambda self, *args, **kwargs: reads.site(self, *args, **kwargs)

    path = Path("views.yaml")
    data = DashboardData("local", "/configured", "temp", Client(path))
    callbacks, panes, selections, controls = [], [], [], []
    thread = threading.get_ident()
    set_root = composed._set_root
    build_table = composed.build_plot_selection_panel
    build_map = composed.build_map_view
    build_sidebar = composed.build_sidebar
    add_site = SelectionState.add_site

    def attach(root, content, label=""):
        assert threading.get_ident() == thread
        panes.append(label)
        set_root(root, content, label)

    def table(*args, **kwargs):
        assert threading.get_ident() == thread
        panel, state = build_table(*args, **kwargs)
        selections.append(state)
        return panel, state

    def map_view(*args, **kwargs):
        assert threading.get_ident() == thread
        return build_map(*args, **kwargs)

    def sidebar(*args, **kwargs):
        content, store, group, variable, metadata, hint, info, reload = build_sidebar(*args, **kwargs)
        controls.extend((store, group, variable, reload))
        return content, store, group, variable, metadata, hint, info, reload

    def add_initial_site(self, *args, **kwargs):
        assert threading.get_ident() == thread
        return add_site(self, *args, **kwargs)

    monkeypatch.setattr(composed, "find_view_file", lambda: path)
    monkeypatch.setattr(composed, "get_default_endpoint_name", lambda _: "local")
    monkeypatch.setattr(composed, "load_views", lambda _: {"local": view})
    monkeypatch.setattr(composed, "load_view_config", lambda *_: SimpleNamespace(
        defaults=SimpleNamespace(display_variable="temp")
    ))
    monkeypatch.setattr(composed, "load_data", lambda *args, **kwargs: data)
    monkeypatch.setattr(composed, "LocalClient", Client)
    monkeypatch.setattr(composed.pn.state, "onload", callbacks.append)
    monkeypatch.setattr(composed, "_set_root", attach)
    monkeypatch.setattr(composed, "build_plot_selection_panel", table)
    monkeypatch.setattr(composed, "build_map_view", map_view)
    monkeypatch.setattr(composed, "build_sidebar", sidebar)
    monkeypatch.setattr(SelectionState, "add_site", add_initial_site)
    return SimpleNamespace(
        data=data, reads=reads, callbacks=callbacks, panes=panes, selections=selections, thread=thread,
        controls=controls,
    )


def test_startup_overlaps_reads_and_attaches_ready_windows(startup) -> None:
    """The shell and table precede slow reads, with the map available before the default series."""
    reads, panes = startup.reads, startup.panes
    for read in (reads.structure, reads.variables, reads.map, reads.metadata, reads.site):
        read.release.clear()

    async def run() -> None:
        doc = Document()
        with set_curdoc(doc):
            template = composed.build_dashboard()
            template.server_doc(doc, location=False)
            assert set(template._render_items) == {
                "controller", "top_left", "top_right", "bottom_left", "bottom_mid", "bottom_right",
                "logs_modal",
            }
            assert doc.roots
            assert panes == []
            load = asyncio.create_task(startup.callbacks[0]())
            try:
                await reads.structure.wait()
                assert not reads.map.entered.is_set()
                assert not reads.variables.entered.is_set()
                reads.structure.release.set()
                await reads.variables.wait()
                await reads.map.wait()
                assert "top_right" in panes
                assert "top_left" not in panes
                assert len(startup.controls) == 4 and all(control.disabled for control in startup.controls)
                assert reads.map.calls[0][0] is not reads.variables.calls[0][0]
                reads.variables.release.set()
                await reads.metadata.wait()
                assert not reads.site.entered.is_set()
                reads.map.release.set()
                await reads.site.wait()
                assert "top_left" in panes
                assert "bottom_left" not in panes
                assert reads.site.calls[0][3]["entity_index"] == 8
                assert reads.site.calls[0][3]["group_path"] == "/sites"
                assert reads.site.calls[0][3]["variable"] == "temp"
            finally:
                for read in (reads.structure, reads.variables, reads.map, reads.metadata, reads.site):
                    read.release.set()
                await load

    asyncio.run(run())
    assert panes == ["top_right", "top_left", "bottom_left", "bottom_mid", "bottom_right"]
    assert startup.selections[0].sites[0]["site_id"] == "BH-8"
    assert startup.selections[0].checked_combinations() == {("BH-8", 0.0)}
    assert all(not control.disabled for control in startup.controls)
    for read in (reads.structure, reads.variables, reads.map, reads.metadata, reads.site):
        assert all(thread != startup.thread for _, thread, _, _ in read.calls)


@pytest.mark.parametrize("delay_overview", [False, True])
def test_startup_loads_each_pane_without_a_raw_history_scan(
    startup, setup_right, tmp_path, monkeypatch, delay_overview
):
    """The dashboard starts with metadata, two bounded reads and a saved overview."""
    ds, source, _, store = setup_right
    client = source.client
    monkeypatch.setattr(
        client, "_view_handle", lambda *_: SimpleNamespace(store_uri="source")
    )
    monkeypatch.setenv("ZF_TIMESERIES_OVERVIEW_DIR", str(tmp_path))
    grouped = tmp_path / "generation-source.zarr"
    ds.to_zarr(grouped, group="sites", consolidated=False)
    monkeypatch.setattr(
        client, "_overview_source", lambda handle: (handle.store_uri, LocalStore(grouped, read_only=True))
    )
    client.generate_timeseries_overview("local", "/sites", "temp", tmp_path, bins=8)
    store.reads.clear()
    fake_client = startup.data.client
    startup.reads.map.result["marker_meta"][1]["entity_index"] = 0
    monkeypatch.setattr(
        fake_client, "get_timeseries_site_metadata", client.get_timeseries_site_metadata
    )
    monkeypatch.setattr(
        fake_client,
        "get_timeseries_overview_data",
        client.get_timeseries_overview_data,
        raising=False,
    )
    monkeypatch.setattr(
        fake_client,
        "get_timeseries_window_data",
        client.get_timeseries_window_data,
        raising=False,
    )
    monkeypatch.setattr(
        fake_client,
        "get_timeseries_data",
        lambda *a, **k: pytest.fail("legacy history reader"),
        raising=False,
    )
    overview_started, release = threading.Event(), threading.Event()
    if not delay_overview:
        release.set()
    views = []
    wrap_pane = composed._line_pane

    def blocked_overview(*args, **kwargs):
        overview_started.set()
        assert release.wait(5)
        return client.get_timeseries_overview_data(*args, **kwargs)

    def attach_view(view):
        views.append(view)
        return wrap_pane(view)

    monkeypatch.setattr(fake_client, "get_timeseries_overview_data", blocked_overview)
    monkeypatch.setattr(composed, "_line_pane", attach_view)

    async def run():
        doc = Document()
        with set_curdoc(doc):
            started = perf_counter()
            template = composed.build_dashboard()
            template.server_doc(doc, location=False)
            await startup.callbacks[0]()
            shell_ready = perf_counter() - started
            left, mid, right = views
            ready = {}
            try:
                async with asyncio.timeout(8):
                    while len(ready) < 3:
                        for callback in doc.session_callbacks:
                            if type(callback).__name__ in {
                                "TimeoutCallback",
                                "NextTickCallback",
                            }:
                                callback.callback()
                        for name, view in zip(("left", "mid", "right"), views):
                            if name not in ready and len(_curves(view)[0]):
                                ready[name] = perf_counter() - started
                        if {"mid", "right"} <= ready.keys():
                            release.set()
                        await asyncio.sleep(0.01)
            finally:
                release.set()
                for callback in list(doc.session_destroyed_callbacks):
                    # Only the application callback needs to run on this unscheduled test document.
                    if getattr(callback, "__name__", "") == "_on_session_destroyed":
                        callback(None)
            assert overview_started.is_set()
            if delay_overview:
                assert ready["left"] >= max(ready["mid"], ready["right"])
            counts = [len(_curves(view)[0]) for view in (left, mid, right)]
            assert counts[0] <= 4 * 8 + 5
            assert counts[1] > counts[2] > 0
            reads, size = store.chunk_reads("temp")
            keys = {key for key, _ in store.reads if key.startswith("temp/c/")}
            full = 2 * ((ds.sizes["time"] + 15) // 16)
            assert 0 < len(keys) < full
            assert startup.selections[0].sites[0]["series"] == []
            print(
                f"local startup delayed_overview={delay_overview} shell={shell_ready:.3f}s first={ready} samples={counts} "
                f"measurement_GETs={reads} bytes={size} distinct_chunks={len(keys)}/{full}"
            )
            store.reads.clear()
            index = ds.indexes["time"]
            client.get_timeseries_window_data(
                "local",
                "/sites",
                variable="temp",
                entity_index=0,
                start=index[0],
                end=index[-1],
            )
            print(
                f"local full-history comparison measurement_GETs_and_bytes={store.chunk_reads('temp')}"
            )

    asyncio.run(run())


@pytest.mark.parametrize(
    ("failed_read", "ready_pane"),
    [
        ("structure", "bottom_right"),
        ("map", "bottom_right"),
        ("metadata", "bottom_right"),
        ("site", "bottom_right"),
    ],
)
def test_startup_contains_read_failures(startup, failed_read, ready_pane) -> None:
    """A failed startup read leaves the other usable windows available."""
    read = getattr(startup.reads, failed_read)
    read.result = OSError(f"{failed_read} unavailable")
    if failed_read == "map":
        startup.reads.site.result = {"status": "error", "reason": "No coordinates"}

    async def run() -> None:
        with set_curdoc(Document()):
            composed.build_dashboard()
            await startup.callbacks[0]()

    asyncio.run(run())
    assert ready_pane in startup.panes
    assert "top_right" in startup.panes
    if failed_read == "site":
        assert "top_left" in startup.panes and startup.selections[0].sites == []


@pytest.mark.parametrize("configured_var", ["", "missing"])
def test_startup_uses_available_variable_for_map_and_default_site(startup, configured_var) -> None:
    """The catalogue fallback supplies both panes even if the configured variable is absent."""
    startup.data.display_variable = configured_var

    async def run() -> None:
        with set_curdoc(Document()):
            composed.build_dashboard()
            await startup.callbacks[0]()

    asyncio.run(run())
    assert startup.data.display_variable == "temp"
    assert startup.reads.map.calls[-1][3]["variable"] == "temp"
    assert startup.reads.site.calls[0][3]["variable"] == "temp"
    assert {"top_left", "bottom_left", "bottom_mid", "bottom_right"} <= set(startup.panes)


def test_startup_logs_one_initial_load_total_after_population(startup, monkeypatch, caplog) -> None:
    """The build and population stages share a single end-to-end operation."""
    monkeypatch.setattr(profiling, "_ENABLED", True)
    monkeypatch.setattr(profiling, "_handler_ready", True)
    monkeypatch.setattr(profiling.logger, "propagate", True)
    caplog.set_level(logging.INFO, logger=profiling.logger.name)

    async def run() -> None:
        with set_curdoc(Document()):
            composed.build_dashboard()
            assert all("dashboard.initial_load " not in message for message in caplog.messages)
            assert all("operation=dashboard.initial_load" not in message for message in caplog.messages)
            await startup.callbacks[0]()

    asyncio.run(run())
    records = []
    seen = set()
    for message in caplog.messages:
        if not message.startswith(("stage=build_dashboard ", "stage=populate_dashboard ", "stage=dashboard.initial_load ")):
            continue
        record = dict(field.split("=", 1) for field in message.split())
        key = (record.get("stage"), record.get("op"), record.get("status"))
        if key in seen:
            continue
        seen.add(key)
        records.append(record)
    assert [record["stage"] for record in records] == ["build_dashboard", "populate_dashboard", "dashboard.initial_load"]
    assert records[0]["op"] == records[1]["op"] == records[2]["op"]
    assert records[2]["status"] == "ok"
    assert all("operation=dashboard.initial_load" not in message for message in caplog.messages)
