"""Opt-in, structured performance measurements for the Zarr Fuse dashboard.

Enable *before importing this module* (normally before launching the server)::

    DASHBOARD_PROFILE=1 zf-dashboard

All measurements go to the separate ``dashboard.performance`` logger. When
profiling is disabled, no clocks, context tags or log messages are produced.
The enabled flag is deliberately fixed at import time for minimal overhead.

Typical synchronous operation::

    with session(session_id):
        with operation("dashboard.initial_load"):
            with time_stage("data.open_store"):
                open_store()

For a deferred Panel/Bokeh callback, *do not* close the operation when merely
scheduling it. Instead, create it now and finish it in the final callback::

    with session(session_id):
        op = operation("borehole.add", site=site_id)
        doc.add_timeout_callback(op.wrap(load_borehole, finish=True), 100)

``op.wrap`` reactivates the original operation and session tags when the
callback runs, even in a different thread. For general callbacks, use
``bind_context(callback)`` while the desired tags are active. Ordinary
``contextvars`` scopes alone do not reliably survive independently scheduled
callbacks.

The operation duration measures *server-side elapsed wall time*, including
explicitly deferred waits. It does NOT measure when the browser finishes
rendering; measure client-side latency separately. Nested times overlap and
must not be summed as independent contributions.
"""

from __future__ import annotations

import contextvars
import functools
import inspect
import itertools
import json
import logging
import os
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any, Self, TypeVar, cast

logger = logging.getLogger("dashboard.performance")

_ENABLED = os.getenv("DASHBOARD_PROFILE", "").strip().lower() in {
    "1", "true", "yes", "on", "y", "t"
}
_HANDLER_NAME = "dashboard.performance.console"
_handler_lock = threading.Lock()
_handler_ready = False
_op_counter = itertools.count(1)
_op_var: contextvars.ContextVar[int | None] = contextvars.ContextVar(
    "dashboard_perf_op", default=None
)
_session_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "dashboard_perf_session", default=None
)

F = TypeVar("F", bound=Callable[..., Any])


def profiling_enabled() -> bool:
    """Whether profiling was enabled when this module was imported."""
    return _ENABLED


def _ensure_handler() -> None:
    """Install and configure one console handler, safely across threads."""
    global _handler_ready
    if _handler_ready:
        return
    with _handler_lock:
        if _handler_ready:
            return
        # Check existing handlers in case this module was reloaded.
        if not any(handler.name == _HANDLER_NAME for handler in logger.handlers):
            handler = logging.StreamHandler()
            handler.set_name(_HANDLER_NAME)
            handler.setFormatter(logging.Formatter("[perf] %(message)s"))
            logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
        _handler_ready = True


def _context_fields() -> dict[str, Any]:
    fields: dict[str, Any] = {}
    op_id = _op_var.get()
    if op_id is not None:
        fields["op"] = op_id
    session_id = _session_var.get()
    if session_id is not None:
        fields["session"] = session_id
    return fields


def _format_value(value: Any) -> str:
    """Quote strings when necessary, escaping line breaks for log parsing."""
    if isinstance(value, str):
        if value and all(c.isalnum() or c in "._-/:+" for c in value):
            return value
        return json.dumps(value, ensure_ascii=False)
    return str(value).replace("\n", "\\n").replace("\r", "\\r")


def _suffix(fields: dict[str, Any]) -> str:
    return "".join(f" {key}={_format_value(value)}" for key, value in fields.items())


def log_stage(name: str, duration_s: float, **fields: Any) -> None:
    """Log a measured stage (seconds in, milliseconds out).

    Explicit ``op`` and ``session`` fields override the active context. This
    lets a timer retain its original identity when closed in another callback.
    Prefer the reserved field names ``op``, ``session`` and ``status`` only for
    instrumentation metadata.
    """
    if not _ENABLED:
        return
    _ensure_handler()
    merged = {**_context_fields(), **fields}
    logger.info(
        "stage=%s duration_ms=%.3f%s", name, duration_s * 1000, _suffix(merged)
    )


def log_event(name: str, **fields: Any) -> None:
    """Log a point-in-time event or a counter (e.g. count=3)."""
    if not _ENABLED:
        return
    _ensure_handler()
    logger.info("event=%s%s", name, _suffix({**_context_fields(), **fields}))


def log_message(message: str) -> None:
    """Log a preformatted counters report, with any active correlation tags."""
    if not _ENABLED:
        return
    _ensure_handler()
    logger.info("%s%s", message, _suffix(_context_fields()))


@contextmanager
def _use_context(op_id: int | None, session_id: str | None) -> Iterator[None]:
    """Temporarily set BOTH tags, restoring them even after exceptions."""
    op_token = _op_var.set(op_id)
    session_token = _session_var.set(session_id)
    try:
        yield
    finally:
        _session_var.reset(session_token)
        _op_var.reset(op_token)


@contextmanager
def session(session_id: str) -> Iterator[None]:
    """Tag measurements in the block with a Panel session identifier.

    For callbacks running after this block, use ``bind_context`` or
    ``Operation.wrap`` to restore the identifying tags.
    """
    if not _ENABLED:
        yield
        return
    token = _session_var.set(str(session_id))
    try:
        yield
    finally:
        _session_var.reset(token)


def bind_context(callback: F) -> F:
    """Bind the current *profiling* tags to a deferred sync/async callback.

    Unlike raw ``contextvars.copy_context``, this copies only the dashboard
    operation and session IDs, avoiding unrelated request-local state. The
    context is restored for the entire call, including any awaits.
    """
    if not _ENABLED:
        return callback
    op_id, session_id = _op_var.get(), _session_var.get()

    if inspect.iscoroutinefunction(callback):
        @functools.wraps(callback)
        async def async_bound(*args: Any, **kwargs: Any) -> Any:
            with _use_context(op_id, session_id):
                return await callback(*args, **kwargs)
        return cast(F, async_bound)

    @functools.wraps(callback)
    def bound(*args: Any, **kwargs: Any) -> Any:
        with _use_context(op_id, session_id):
            return callback(*args, **kwargs)
    return cast(F, bound)


class StageTimer:
    """One-shot elapsed wall timer; usable as a context manager or manually.

    Correlation tags are captured when the timer is *created*, not when it is
    logged, so a delayed ``log`` call cannot accidentally inherit new tags.
    """

    __slots__ = (
        "_name", "_fields", "_start", "_logged", "_lock", "_op_id", "_session_id"
    )

    def __init__(self, name: str, **fields: Any) -> None:
        self._name = name
        self._fields = fields if _ENABLED else {}
        self._start = time.perf_counter() if _ENABLED else None
        self._op_id = _op_var.get() if _ENABLED else None
        self._session_id = _session_var.get() if _ENABLED else None
        self._logged = False
        self._lock = threading.Lock() if _ENABLED else None

    def elapsed(self) -> float:
        """Elapsed seconds (zero if profiling is disabled)."""
        return 0.0 if self._start is None else time.perf_counter() - self._start

    def log(self, *, status: str = "ok", **fields: Any) -> None:
        """Log elapsed time once, including any extra fields."""
        if self._start is None:
            return
        assert self._lock is not None
        with self._lock:
            if self._logged:
                return
            duration = time.perf_counter() - self._start
            self._logged = True
        with _use_context(self._op_id, self._session_id):
            log_stage(self._name, duration, **{**self._fields, **fields, "status": status})

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        self.log(status="error" if exc_type is not None else "ok")
        return False


def time_stage(name: str, **fields: Any) -> StageTimer:
    """Begin a measured stage; close with ``with`` or ``.log()``."""
    return StageTimer(name, **fields)


class Operation:
    """One-shot user operation, optionally spanning multiple callbacks.

    ``with operation(...)`` measures a synchronous scope. For deferred work,
    keep the operation object alive, wrap callbacks in ``op.wrap(...)`` or
    ``with op.activate():``, and call ``op.finish()`` *after the final callback*.
    ``activate()`` does not finish the operation. ``finish()`` is idempotent.
    """

    __slots__ = (
        "_label", "_op_id", "_session_id", "_fields", "_start",
        "_finished", "_lock", "_scope_stack",
    )

    def __init__(self, label: str, *, op_id: int, **fields: Any) -> None:
        self._label = label
        self._op_id = op_id
        self._session_id = _session_var.get() if _ENABLED else None
        self._fields = fields
        self._start = time.perf_counter() if _ENABLED else None
        self._finished = False
        self._lock = threading.Lock() if _ENABLED else None
        # Each execution context has its own stack: callbacks may activate an
        # operation concurrently; they must never share a contextvar token.
        self._scope_stack: contextvars.ContextVar[tuple[Any, ...]] | None = (
            contextvars.ContextVar(f"dashboard_perf_op_scopes_{op_id}", default=())
            if _ENABLED else None
        )

    @property
    def op_id(self) -> int:
        return self._op_id

    def elapsed(self) -> float:
        """Elapsed seconds from operation creation (zero when disabled)."""
        return 0.0 if self._start is None else time.perf_counter() - self._start

    @contextmanager
    def activate(self) -> Iterator[Operation]:
        """Restore operation AND originating session tags for a code block.

        Does not stop the operation timer; safe to use in distinct callbacks.
        """
        if not _ENABLED:
            yield self
            return
        with _use_context(self._op_id, self._session_id):
            yield self

    def finish(self, *, status: str = "ok", **fields: Any) -> None:
        """Report total elapsed time once, usually in the last callback."""
        if self._start is None:
            return
        assert self._lock is not None
        with self._lock:
            if self._finished:
                return
            duration = time.perf_counter() - self._start
            self._finished = True
        tags = {**self._fields, **fields, "op": self._op_id, "status": status}
        if self._session_id is not None:
            tags["session"] = self._session_id
        with _use_context(self._op_id, self._session_id):
            log_stage(self._label, duration, **tags)

    def log(self, **fields: Any) -> None:
        """Compatibility alias for ``finish`` (accepts ``status=...``)."""
        self.finish(**fields)

    def wrap(self, callback: F, *, finish: bool = False) -> F:
        """Run a deferred callback with this operation's tags restored.

        Set ``finish=True`` ONLY for the final callback of an operation. The
        total includes the delay between operation creation and that callback.
        Both sync and async callback functions are supported.
        """
        if not _ENABLED:
            return callback

        if inspect.iscoroutinefunction(callback):
            @functools.wraps(callback)
            async def async_wrapped(*args: Any, **kwargs: Any) -> Any:
                with self.activate():
                    try:
                        result = await callback(*args, **kwargs)
                    except BaseException:
                        if finish:
                            self.finish(status="error")
                        raise
                    else:
                        if finish:
                            self.finish(status="ok")
                        return result
            return cast(F, async_wrapped)

        @functools.wraps(callback)
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            with self.activate():
                try:
                    result = callback(*args, **kwargs)
                except BaseException:
                    if finish:
                        self.finish(status="error")
                    raise
                else:
                    if finish:
                        self.finish(status="ok")
                    return result
        return cast(F, wrapped)

    def __enter__(self) -> Self:
        if not _ENABLED:
            return self
        assert self._scope_stack is not None
        scope = self.activate()
        scope.__enter__()
        self._scope_stack.set(self._scope_stack.get() + (scope,))
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        if not _ENABLED:
            return False
        assert self._scope_stack is not None
        stack = self._scope_stack.get()
        if not stack:
            raise RuntimeError("Operation.__exit__ called without __enter__")
        self._scope_stack.set(stack[:-1])
        try:
            if len(stack) == 1:
                self.finish(status="error" if exc_type is not None else "ok")
        finally:
            stack[-1].__exit__(exc_type, exc, tb)
        return False


def operation(label: str, **fields: Any) -> Operation:
    """Create a timed operation with a unique ID in this Python process."""
    return Operation(label, op_id=next(_op_counter), **fields)


def timed(name: str | None = None) -> Callable[[F], F]:
    """Decorate a synchronous or asynchronous function as one measured stage."""
    def decorator(func: F) -> F:
        stage = name or func.__qualname__

        if inspect.iscoroutinefunction(func):
            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                if not _ENABLED:
                    return await func(*args, **kwargs)
                with time_stage(stage):
                    return await func(*args, **kwargs)
            return cast(F, async_wrapper)

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            if not _ENABLED:
                return func(*args, **kwargs)
            with time_stage(stage):
                return func(*args, **kwargs)
        return cast(F, wrapper)

    return decorator
