"""Schedule and publish windowed reads for the right time-series pane."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from threading import Event

import numpy as np
import pandas as pd
from holoviews import streams

from dashboard.perf_summary import count, measure_operation
from dashboard.profiling import operation

logger = logging.getLogger(__name__)
Bounds = tuple[pd.Timestamp, pd.Timestamp]
TraceKey = tuple[int, int]
Traces = dict[TraceKey, tuple[pd.DatetimeIndex, np.ndarray]]
_WindowChanged = streams.Stream.define("RightWindowChanged", revision=0)


@dataclass(frozen=True)
class SiteWindow:
    """Depth positions requested for one entity."""

    entity: int
    depths: tuple[int, ...]
    single_trace: bool


@dataclass(frozen=True)
class _Request:
    bounds: Bounds
    sites: tuple[SiteWindow, ...]

    def covers(self, other: _Request) -> bool:
        start, end = self.bounds
        requested_start, requested_end = other.bounds
        if not start <= requested_start <= requested_end <= end:
            return False
        depths = {site.entity: set(site.depths) for site in self.sites}
        return all(
            set(site.depths) <= depths.get(site.entity, set()) for site in other.sites
        )


class RightWindow:
    """Keep one loaded window and coalesce requests while a read is in flight."""

    def __init__(self, doc, data) -> None:
        self.doc = doc
        self.data = data
        self.context = (data.view_name, data.group_path, data.display_variable)
        self.client = data.client
        self.changed = _WindowChanged()
        self._selection: streams.Params | None = None
        self._closed = False
        self._wanted: _Request | None = None
        self._loaded: _Request | None = None
        self._traces: Traces = {}
        self._error = False
        self._timeout = None
        self._delivery = None
        self._active: tuple[_Request, Event, asyncio.Task] | None = None

    def watch_selection(self, selection_state) -> streams.Params:
        """Create the right-only watcher that must be released when the pane retires."""
        self._selection = streams.Params(selection_state, parameters=["version"])
        return self._selection

    def _is_current(self) -> bool:
        return not self._closed and self.context == (
            self.data.view_name,
            self.data.group_path,
            self.data.display_variable,
        )

    def request(
        self,
        bounds: Bounds,
        sites: tuple[SiteWindow, ...],
        time_dim: str,
        depth_dim: str | None,
    ) -> tuple[Traces, str]:
        """Queue the latest uncovered window and return available traces and status."""
        if not self._is_current():
            return {}, ""
        if not sites:
            self.clear_selection()
            return {}, ""
        request = _Request(bounds, sites)
        covered = self._loaded is not None and self._loaded.covers(request)
        if request != self._wanted:
            self._wanted = request
            self._error = False
            self._cancel_timeout()
            active_covers = False
            if self._active is not None:
                active, stop, _ = self._active
                active_covers = not stop.is_set() and active.covers(request)
                if not active_covers:
                    stop.set()
            if covered:
                with measure_operation("timeseries.right_window.covered"):
                    count("timeseries.right_window.coverage_hit")
            elif not active_covers and self.doc is not None:
                self._timeout = self.doc.add_timeout_callback(
                    lambda: self._start(time_dim, depth_dim), 75
                )
        if self.doc is None:
            return {}, ""
        if covered:
            return self._traces, ""
        return {}, "Window load failed" if self._error else "Loading window…"

    def _cancel_timeout(self) -> None:
        if self._timeout is not None:
            self.doc.remove_timeout_callback(self._timeout)
            self._timeout = None

    def close(self, *, session_destroyed: bool = False) -> None:
        """Retire timeouts and delivery when the pane is replaced or destroyed."""
        self._closed = True
        if session_destroyed:
            # Bokeh has already removed its scheduled callbacks and destroyed
            # the document's callback dispatcher before invoking this hook.
            self._timeout = None
        self.clear_selection()
        if self._selection is not None:
            self._selection.unwatch()
            self._selection = None
        if self._delivery is not None:
            handle, op = self._delivery
            if not session_destroyed:
                self.doc.remove_next_tick_callback(handle)
            op.finish(status="cancelled")
            self._delivery = None
        if self._active is not None:
            _, _, task = self._active
            task.cancel()
            self._active = None
        self._loaded = None
        self._traces = {}
        self.doc = None

    def clear_selection(self) -> None:
        """Withdraw the request when no sites supply a time range."""
        self._wanted = None
        self._cancel_timeout()
        if self._active is not None:
            _, stop, _ = self._active
            stop.set()

    def _start(self, time_dim: str, depth_dim: str | None) -> None:
        self._timeout = None
        request = self._wanted
        if (
            not self._is_current()
            or request is None
            or self._active is not None
            or (self._loaded is not None and self._loaded.covers(request))
        ):
            return
        stop = Event()
        task = asyncio.create_task(self._load(request, time_dim, depth_dim, stop))
        self._active = (request, stop, task)

    def _read(
        self, request: _Request, time_dim: str, depth_dim: str | None, stop: Event
    ) -> Traces:
        """Materialize each selected site in the worker thread."""
        view, group, variable = self.context
        start, end = request.bounds
        traces: Traces = {}
        with measure_operation("timeseries.right_window.read"):
            for site in request.sites:
                if stop.is_set():
                    break
                array = self.client.get_timeseries_window_data(
                    view,
                    group,
                    variable=variable,
                    entity_index=site.entity,
                    depth_indices=None if site.single_trace else site.depths,
                    start=start,
                    end=end,
                )
                times = pd.DatetimeIndex(array.indexes[time_dim])
                if depth_dim is not None and depth_dim in array.dims:
                    values = array.transpose(time_dim, depth_dim).values
                else:
                    values = array.transpose(time_dim).values[:, None]
                for column, depth in enumerate(site.depths):
                    traces[(site.entity, depth)] = (times, values[:, column])
        return traces

    async def _load(
        self, request: _Request, time_dim: str, depth_dim: str | None, stop: Event
    ) -> None:
        op = operation("timeseries.right_window", sites=len(request.sites))
        with op.activate():
            try:
                traces = await asyncio.to_thread(
                    self._read, request, time_dim, depth_dim, stop
                )
            except asyncio.CancelledError:
                op.finish(status="cancelled")
                return
            except Exception:
                logger.exception("Right time-series window read failed")
                traces = None
        if not self._is_current():
            op.finish(status="cancelled")
            return

        def apply() -> None:
            self._delivery = None
            self._active = None
            if not self._is_current():
                op.finish(status="cancelled")
                return
            wanted = self._wanted
            if (
                wanted is not None
                and not stop.is_set()
                and request.covers(wanted)
                and (traces is not None or request == wanted)
            ):
                self._error = traces is None
                if traces is not None:
                    self._loaded, self._traces = request, traces
                with measure_operation("timeseries.right_window.update"):
                    self.changed.event(revision=self.changed.revision + 1)
                op.finish(status="error" if self._error else "ok")
            else:
                op.finish(status="cancelled")
                if self._timeout is None:
                    self._start(time_dim, depth_dim)

        self._delivery = (self.doc.add_next_tick_callback(op.wrap(apply)), op)
