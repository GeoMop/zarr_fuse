"""Aggregate fine-grained dashboard probes into one summary per user operation.

The dashboard's hot paths (timeseries fetches, selection-state mutations, plot
redraws, config/schema reads) run many times inside a single user operation.
Logging each one produces hundreds of records, so instead this module records
cheap in-memory durations and counters into the *active* ``OperationSummary``
and emits at most a handful of aggregated records when the operation ends.

Design notes
------------
- ``dashboard/profiling.py`` is intentionally left unchanged: this layer only
  uses its public ``log_event`` / ``profiling_enabled`` API.
- Probes are near no-ops when profiling is disabled or when no operation is
  active: no clock is read and nothing is formatted until ``emit``.
- Summaries are tracked per thread (``threading.local``). Wrap synchronous user
  operations with :func:`measure_operation`; for deferred callbacks that share
  the thread, use :func:`begin`/:func:`end` or :func:`measure_operation` inside
  the callback (``Operation.wrap`` already restores the profiling context).

Emitted records (via ``logging.getLogger("dashboard.performance")``)::

    event=op.summary op=7 session=... operation=variable.switch total_ms=812.4
    event=op.stage op=7 stage=refresh_views count=1 total_ms=640.2 max_ms=640.2
    event=op.counters op=7 plot.redraws=6 config.cache_hit=3
"""

from __future__ import annotations

import contextlib
import threading
import time
from collections.abc import Iterator
from typing import Any

from dashboard.profiling import log_event, profiling_enabled

# Keep one operation's output bounded (target: a handful of records per operation).
_MAX_STAGE_LINES = 16
_MAX_COUNTERS = 12

_local = threading.local()


class OperationSummary:
    """Accumulates stage durations and counters for one user operation.

    Only :meth:`emit` formats or logs anything; the hot-path methods just touch
    a dict. Instances are created by :func:`begin` and are not thread-safe on
    their own (see the module docstring for the threading model).
    """

    __slots__ = ("_counts", "_stages", "_start", "fields", "name")

    def __init__(self, name: str, fields: dict[str, Any]) -> None:
        self.name = name
        self.fields = fields
        self._stages: dict[str, list[float]] = {}
        self._counts: dict[str, int] = {}
        self._start = time.perf_counter()

    def add_stage(self, name: str, seconds: float) -> None:
        """Record one occurrence of ``name`` lasting ``seconds``."""
        bucket = self._stages.get(name)
        if bucket is None:
            self._stages[name] = [seconds, seconds, 1.0]
        else:
            bucket[0] += seconds
            bucket[1] = max(bucket[1], seconds)
            bucket[2] += 1.0

    def count(self, name: str, n: int = 1) -> None:
        """Add ``n`` to the named counter."""
        if n:
            self._counts[name] = self._counts.get(name, 0) + n

    def emit(self) -> None:
        """Write the bounded summary through ``dashboard.profiling``."""
        total_ms = (time.perf_counter() - self._start) * 1000.0
        log_event(
            "op.summary",
            operation=self.name,
            total_ms=round(total_ms, 1),
            **self.fields,
        )
        ranked = sorted(
            self._stages.items(), key=lambda item: item[1][0], reverse=True
        )
        for stage, (total_s, max_s, count) in ranked[:_MAX_STAGE_LINES]:
            log_event(
                "op.stage",
                stage=stage,
                count=int(count),
                total_ms=round(total_s * 1000.0, 1),
                max_ms=round(max_s * 1000.0, 1),
            )
        if self._counts:
            counters = dict(
                sorted(self._counts.items(), key=lambda item: item[1], reverse=True)
            )
            if len(counters) > _MAX_COUNTERS:
                counters = dict(list(counters.items())[:_MAX_COUNTERS])
            log_event("op.counters", **counters)


def _stack() -> list[OperationSummary]:
    stack = _local.__dict__.get("stack")
    if stack is None:
        stack = []
        _local.stack = stack
    return stack


def _active() -> list[OperationSummary] | None:
    """Return the current thread's summary stack, or ``None`` when empty."""
    return _local.__dict__.get("stack")


def current() -> OperationSummary | None:
    """Return the innermost active summary on this thread, if any."""
    stack = _local.__dict__.get("stack")
    return stack[-1] if stack else None


def begin(name: str, **fields: Any) -> OperationSummary | None:
    """Start a summary; returns ``None`` (and records nothing) if disabled."""
    if not profiling_enabled():
        return None
    summary = OperationSummary(name, fields)
    _stack().append(summary)
    return summary


def end() -> None:
    """Finish the innermost active summary and emit it, if any."""
    stack = _local.__dict__.get("stack")
    if stack:
        stack.pop().emit()


@contextlib.contextmanager
def measure_operation(name: str, **fields: Any) -> Iterator[OperationSummary | None]:
    """Measure one user operation, emitting a summary on exit."""
    summary = begin(name, **fields)
    try:
        yield summary
    finally:
        if summary is not None:
            end()


def add_stage(name: str, seconds: float) -> None:
    """Record a stage duration on the active summary (cheap no-op otherwise)."""
    stack = _local.__dict__.get("stack")
    if stack:
        stack[-1].add_stage(name, seconds)


def count(name: str, n: int = 1) -> None:
    """Increment a counter on the active summary (cheap no-op otherwise)."""
    stack = _local.__dict__.get("stack")
    if stack:
        stack[-1].count(name, n)


@contextlib.contextmanager
def stage(name: str) -> Iterator[None]:
    """Time a block and record it on the active summary.

    Unlike ``profiling.time_stage`` this never logs on its own, so it is safe
    to use inside frequently called functions, loops and reactive callbacks.
    """
    stack = _local.__dict__.get("stack")
    if not stack:
        yield
        return
    summary = stack[-1]
    start = time.perf_counter()
    try:
        yield
    finally:
        summary.add_stage(name, time.perf_counter() - start)


class Timer:
    """Manual start/stop timer that records only into the active summary.

    Use for functions with early returns where a ``with`` block would force a
    large re-indentation; create at entry and call :meth:`stop` before each
    return. When no summary is active it reads no clock.
    """

    __slots__ = ("_name", "_start")

    def __init__(self, name: str) -> None:
        self._name = name
        self._start = time.perf_counter() if _active() else None

    def stop(self) -> None:
        """Record the elapsed time once; subsequent calls are ignored."""
        if self._start is None:
            return
        add_stage(self._name, time.perf_counter() - self._start)
        self._start = None
