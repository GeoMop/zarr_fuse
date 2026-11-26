"""Exercise the right pane's windowed reads without a live server or S3."""

import asyncio
from pathlib import Path
from threading import Event, get_ident
from types import SimpleNamespace

import holoviews as hv
import numpy as np
import pandas as pd
import panel as pn
import pytest
import xarray as xr
from bokeh.document import Document
from bokeh.models import GlyphRenderer, Line, Scatter
from holoviews import streams
from zarr.storage import LocalStore

from dashboard.config import SchemaConfig, SchemaFieldsConfig
from dashboard.data import LocalClient
from dashboard.multi_time_views import build_timeseries_views
from dashboard.plot_selection import SelectionState
from dashboard.right_timeseries import RightWindow
from dashboard.scripts.benchmark_timeseries import CountingStore


class _Document:
    """Drive document callbacks on the test event loop."""

    def add_timeout_callback(self, callback, timeout):
        return asyncio.get_running_loop().call_later(timeout / 1000, callback)

    def remove_timeout_callback(self, handle):
        handle.cancel()

    def add_next_tick_callback(self, callback):
        return asyncio.get_running_loop().call_soon(callback)

    def remove_next_tick_callback(self, handle):
        handle.cancel()


@pytest.fixture
def setup_right(tmp_path, monkeypatch):
    hv.extension("bokeh")
    times = pd.date_range("2024-01-01", periods=169, freq="h").delete(145)
    times = times.insert(148, times[147])
    values = np.arange(len(times) * 4, dtype=np.float32).reshape(len(times), 2, 2)
    values[151, 0, 0] = np.nan
    ds = xr.Dataset(
        {"temp": (("depth", "time", "entity"), values.transpose(2, 0, 1))},
        coords={"time": times, "entity": ["A", "B"], "depth": [0.0, 3.0]},
    )
    path = tmp_path / "test.zarr"
    ds.to_zarr(path, consolidated=False, encoding={"temp": {"chunks": (1, 16, 1)}})
    store = CountingStore(LocalStore(path, read_only=True))
    client = LocalClient(Path("unused.yaml"))
    fields = SchemaFieldsConfig("lat", "lon", "time", "entity", "depth")
    schema = SchemaConfig(file="", fields=fields)
    monkeypatch.setattr(
        client, "_view_config", lambda *_: SimpleNamespace(schema=schema)
    )
    monkeypatch.setattr(
        client,
        "_get_group",
        lambda *_: SimpleNamespace(dataset=xr.open_zarr(store, consolidated=False)),
    )
    monkeypatch.setattr(client, "get_variable_metadata", lambda *_: {})
    monkeypatch.setattr(
        client,
        "get_view",
        lambda *_: {
            "defaults": {},
            "schema_display": {"entity_name": "Site"},
            "schema": {
                "fields": {"time": "time", "entity": "entity", "vertical": "depth"}
            },
            "visualization": {
                "timeseries": {"middle_window_days": 30, "right_window_hours": 24}
            },
        },
    )
    state = SelectionState()
    state.add_site(0, "A", [0.0, 3.0], [np.full(len(times), -1.0)] * 2, times)
    data = SimpleNamespace(
        client=client, view_name="view", group_path="/", display_variable="temp"
    )
    with store:
        yield ds, data, state, store


async def _until(predicate):
    async with asyncio.timeout(8):
        while not predicate():
            await asyncio.sleep(0.01)


def _curves(view):
    return [element for element in view[()].values() if isinstance(element, hv.Curve)]


def _plot_stream(view, kind):
    return next(stream for stream in view.streams if isinstance(stream, kind))


def _pan(view, start, end):
    _plot_stream(view, streams.RangeX).event(
        x_range=(pd.Timestamp(start), pd.Timestamp(end))
    )
    return _curves(view)


def test_initial_day_uses_window_reader_and_preserves_raw_values(
    setup_right, monkeypatch
):
    """The right pane draws only day samples; the other panes still draw legacy input."""
    ds, data, state, store = setup_right
    read = data.client.get_timeseries_window_data
    calls = []

    def track(*args, **kwargs):
        calls.append((get_ident(), kwargs))
        return read(*args, **kwargs)

    monkeypatch.setattr(data.client, "get_timeseries_window_data", track)

    async def run():
        controller = RightWindow(_Document(), data)
        left, mid, right, *_ = build_timeseries_views(
            data, {}, state, right_window=controller
        )
        try:
            plot = hv.renderer("bokeh").get_plot(right)
            assert not len(_curves(right)[0])
            assert "Loading window" in plot.state.title.text
            await _until(lambda: len(_curves(right)[0]) > 0)
            thread, request = calls[0]
            assert thread != get_ident()
            start, end = request["start"], request["end"]
            assert end - start == pd.Timedelta(days=1)
            expected = ds.temp.sel(time=slice(start, end)).isel(entity=0, depth=0)
            curve = _curves(right)[0]
            np.testing.assert_array_equal(
                curve.dimension_values(0), expected.time.values
            )
            np.testing.assert_array_equal(curve.dimension_values(1), expected.values)
            assert np.isnan(expected.values).any()
            assert len(calls) == 1
            assert store.chunk_reads("temp")[0] == 4
            glyphs = [r for r in plot.state.renderers if isinstance(r, GlyphRenderer)]
            curves = [r for r in glyphs if isinstance(r.glyph, Line) and r.visible]
            markers = [r for r in glyphs if isinstance(r.glyph, Scatter) and r.visible]
            assert len(curves) == len(markers) == 2
            for marker in markers:
                stamps = pd.to_datetime(marker.data_source.data["time"])
                assert (stamps >= start).all() and (stamps <= end).all()
            for legacy in (left, mid):
                assert len(_curves(legacy)[0]) == ds.sizes["time"]
                assert (_curves(legacy)[0].dimension_values(1) == -1).all()
        finally:
            controller.close()

    asyncio.run(run())


def test_zoom_pan_reset_reuses_covered_window_and_tracks_shared_center(
    setup_right, monkeypatch
):
    """Zoom within coverage, debounce uncovered pans, and keep the shared center and reset."""
    ds, data, state, _ = setup_right
    reader = data.client.get_timeseries_window_data
    calls = []

    def record(*args, **kwargs):
        calls.append((kwargs["start"], kwargs["end"]))
        return reader(*args, **kwargs)

    monkeypatch.setattr(data.client, "get_timeseries_window_data", record)

    async def run():
        controller = RightWindow(_Document(), data)
        left, mid, right, *_ = build_timeseries_views(
            data, {}, state, right_window=controller
        )
        try:
            plot = hv.renderer("bokeh").get_plot(right)
            await _until(lambda: len(_curves(right)[0]) > 0)
            home_start, home_end = calls[0]
            reset_start, reset_end = (
                plot.state.x_range.reset_start,
                plot.state.x_range.reset_end,
            )
            zoom_start = home_start + pd.Timedelta("3h")
            zoom_end = home_end - pd.Timedelta("3h")
            curve = _pan(right, zoom_start, zoom_end)[0]
            assert len(curve) == len(ds.sel(time=slice(zoom_start, zoom_end)).time)
            assert len(calls) == 1
            assert controller._timeout is None

            for day in (2, 3, 4):
                _pan(right, f"2024-01-0{day}T00:00", f"2024-01-0{day}T12:00")
            await _until(lambda: len(_curves(right)[0]) > 0)
            assert calls[1:] == [
                (pd.Timestamp("2024-01-04"), pd.Timestamp("2024-01-04T12:00"))
            ]
            assert plot.state.x_range.reset_start == reset_start
            assert plot.state.x_range.reset_end == reset_end

            # Zoom out past both edges of the 12-hour window just loaded.
            expanded = (
                pd.Timestamp("2024-01-03T22:00"),
                pd.Timestamp("2024-01-04T14:00"),
            )
            _pan(right, *expanded)
            await _until(lambda: len(_curves(right)[0]) > 0)
            assert calls[-1] == expanded

            _pan(right, home_start, home_end)
            await _until(lambda: len(_curves(right)[0]) > 0)
            assert calls[-1] == calls[0]
            center = pd.Timestamp("2024-01-03T09:00")
            _plot_stream(left, streams.Tap).event(x=center)
            await _until(lambda: len(_curves(right)[0]) > 0)
            assert calls[-1] == (
                center - pd.Timedelta("12h"),
                center + pd.Timedelta("12h"),
            )
            for view in (left, mid, right):
                assert [
                    line.data
                    for line in view[()].values()
                    if isinstance(line, hv.VLine)
                ] == [center]
        finally:
            controller.close()

    asyncio.run(run())


@pytest.mark.parametrize(
    "supersede", ["pan", "variable", "dataset", "reload", "selection"]
)
def test_in_flight_result_cannot_publish_after_supersession(
    setup_right, monkeypatch, supersede
):
    """An obsolete read can finish on its thread without updating the pane."""
    _, data, state, _ = setup_right
    reader = data.client.get_timeseries_window_data
    entered, release, finished = Event(), Event(), Event()
    calls = []

    def block(*args, **kwargs):
        calls.append(kwargs)
        if not entered.is_set():
            entered.set()
            try:
                assert release.wait(8)
                return reader(*args, **kwargs)
            finally:
                finished.set()
        return reader(*args, **kwargs)

    monkeypatch.setattr(data.client, "get_timeseries_window_data", block)

    async def run():
        controller = RightWindow(_Document(), data)
        _, _, right, *_ = build_timeseries_views(
            data, {}, state, right_window=controller
        )
        try:
            _curves(right)
            await _until(entered.is_set)
            if supersede == "pan":
                _pan(right, "2024-01-03", "2024-01-03T12:00")
                await _until(lambda: controller._timeout is None)
                assert len(calls) == 1
            elif supersede == "selection":
                state.clear()
                _curves(right)
            else:
                controller.close()
                if supersede == "variable":
                    data.display_variable = "other"
                elif supersede == "dataset":
                    data.view_name = "other"
                else:
                    data.client.clear_cache()
            release.set()
            await _until(finished.is_set)
            if supersede == "pan":
                await _until(lambda: controller.changed.revision == 1)
                assert len(calls) == 2
                assert _curves(right)[0].dimension_values(0)[0] == np.datetime64(
                    "2024-01-03"
                )
            else:
                if supersede == "selection":
                    await _until(lambda: controller._active is None)
                assert controller.changed.revision == 0
                assert not len(_curves(right)[0])
        finally:
            release.set()
            controller.close()

    asyncio.run(run())


@pytest.mark.parametrize("covered", [True, False])
def test_slow_read_is_reused_or_skips_obsolete_sites(setup_right, monkeypatch, covered):
    """Zoom into an active read or supersede it without waiting for obsolete sites."""
    ds, data, state, _ = setup_right
    times = ds.indexes["time"]
    state.add_site(1, "B", [0.0, 3.0], [np.zeros(len(times))] * 2, times)
    entered, release = Event(), Event()
    call_order = []
    reader = data.client.get_timeseries_window_data

    def record(*args, **kwargs):
        bounds = (kwargs["start"], kwargs["end"])
        call_order.append((bounds, kwargs["entity_index"]))
        if not entered.is_set():
            entered.set()
            assert release.wait(8)
        return reader(*args, **kwargs)

    monkeypatch.setattr(data.client, "get_timeseries_window_data", record)

    async def run():
        controller = RightWindow(_Document(), data)
        _, _, right, *_ = build_timeseries_views(
            data, {}, state, right_window=controller
        )
        try:
            _curves(right)
            await _until(entered.is_set)
            home, _ = call_order[0]
            if covered:
                first, last = home
                target = (first + pd.Timedelta(hours=1), last - pd.Timedelta(hours=1))
            else:
                target = (pd.Timestamp("2024-01-03"), pd.Timestamp("2024-01-03T06:00"))
            _pan(right, *target)
            await _until(lambda: controller._timeout is None)
            assert call_order == [(home, 0)]
            release.set()
            await _until(lambda: controller.changed.revision == 1)
            expected = (
                [(home, 0), (home, 1)]
                if covered
                else [(home, 0), (target, 0), (target, 1)]
            )
            assert call_order == expected
            samples = ds.sel(time=slice(*target)).indexes["time"]
            np.testing.assert_array_equal(
                _curves(right)[0].dimension_values(0), samples
            )
        finally:
            release.set()
            controller.close()

    asyncio.run(run())


def test_replacing_right_pane_detaches_selection_and_plot_callbacks(setup_right):
    """Dispose the right-only watcher and let Panel detach its plot subscribers."""
    _, data, state, _ = setup_right
    controller = RightWindow(None, data)
    _, _, right, *_ = build_timeseries_views(data, {}, state, right_window=controller)
    selection = _plot_stream(right, streams.Params)
    layout = pn.Column(pn.pane.HoloViews(right))
    layout.get_root(Document())
    assert selection._watchers
    assert controller.changed.subscribers
    controller.close()
    layout.objects = []
    assert not selection._watchers
    assert not controller.changed.subscribers
    assert not _plot_stream(right, streams.RangeX).subscribers


def test_close_removes_queued_document_delivery(setup_right):
    """Closing after worker completion removes the pending update from the document."""
    _, data, state, _ = setup_right

    class DeferredDocument(_Document):
        next_ticks = None

        def add_next_tick_callback(self, callback):
            self.next_ticks = callback
            return callback

        def remove_next_tick_callback(self, handle):
            assert self.next_ticks is handle
            self.next_ticks = None

    async def run():
        document = DeferredDocument()
        controller = RightWindow(document, data)
        _, _, right, *_ = build_timeseries_views(
            data, {}, state, right_window=controller
        )
        try:
            _curves(right)
            await _until(lambda: document.next_ticks is not None)
        finally:
            controller.close()
        assert document.next_ticks is None
        assert controller.changed.revision == 0

    asyncio.run(run())


def test_teardown_releases_watcher_after_bokeh_destroys_document(setup_right):
    """Session cleanup can run after the document callback dispatcher is destroyed."""
    _, data, state, _ = setup_right
    document = Document()
    controller = RightWindow(document, data)
    _, _, right, *_ = build_timeseries_views(data, {}, state, right_window=controller)
    _curves(right)
    assert controller._timeout is not None
    document.callbacks.destroy()
    controller.close(session_destroyed=True)
    assert controller._timeout is None
    assert not _plot_stream(right, streams.Params)._watchers


def test_selection_empty_window_and_error_stay_local(setup_right, monkeypatch):
    """Keep valid depths and sites, and render gaps and read errors without history values."""
    ds, data, state, _ = setup_right
    reader = data.client.get_timeseries_window_data
    calls = []

    def record(*args, **kwargs):
        calls.append(kwargs)
        if kwargs["start"] == pd.Timestamp("2024-01-03"):
            raise OSError("fixture read failure")
        return reader(*args, **kwargs)

    monkeypatch.setattr(data.client, "get_timeseries_window_data", record)

    async def run():
        controller = RightWindow(_Document(), data)
        left, _, right, *_ = build_timeseries_views(
            data, {}, state, right_window=controller
        )
        try:
            plot = hv.renderer("bokeh").get_plot(right)
            await _until(lambda: len(_curves(right)) == 2)
            state.set_checked("A", 0.0, False)
            assert [curve.label for curve in _curves(right)] == ["A @ 3.00"]
            assert len(calls) == 1
            times = ds.indexes["time"]
            state.add_site(1, "B", [0.0, 3.0], [np.zeros(len(times))] * 2, times)
            await _until(lambda: len(_curves(right)) == 3)
            assert [
                (call["entity_index"], call["depth_indices"]) for call in calls[-2:]
            ] == [
                (0, (1,)),
                (1, (0, 1)),
            ]
            state.remove_site(0)
            assert [curve.label for curve in _curves(right)] == ["B @ 0.00", "B @ 3.00"]
            _pan(right, "2024-01-02T00:10", "2024-01-02T00:20")
            await _until(lambda: "Loading window" not in plot.state.title.text)
            assert not len(_curves(right)[0])
            assert "failed" not in plot.state.title.text
            _pan(right, "2024-01-03", "2024-01-03T05:00")
            await _until(lambda: "Window load failed" in plot.state.title.text)
            assert len(_curves(left)[0]) == len(times)
        finally:
            controller.close()

    asyncio.run(run())
