"""Opt-in, lightweight debug diagnostics for the Zarr Fuse dashboard.

Enable *before importing this module* (normally before launching the server)::

    DASHBOARD_DEBUG=1 zf-dashboard

Repetitive diagnostic messages (``[config]``, ``[timeseries]``, ``[ylim]``, ...)
are emitted through the separate ``dashboard.debug`` logger at ``DEBUG`` level
and are disabled by default. As in :mod:`dashboard.profiling`, the enabled flag
is fixed at import time for minimal overhead, and ``[perf]`` output controlled
by ``DASHBOARD_PROFILE`` is unaffected.

Two rules keep the disabled path cheap:

- Messages use ``%``-style placeholders, so their arguments are only formatted
  when a record is actually emitted::

      debug("[ylim] view=%s ylim=%s", view, ylim)

- Anything more expensive than passing a reference (filesystem checks,
  ``numpy`` reductions, loops that build a string) is guarded so it is skipped
  entirely while debugging is disabled::

      if debug_enabled():
          debug("[ylim] n_visible=%s", int(np.sum(...)))
"""

from __future__ import annotations

import logging
import os
import threading

logger = logging.getLogger("dashboard.debug")

_ENABLED = os.getenv("DASHBOARD_DEBUG", "").strip().lower() in {
    "1", "true", "yes", "on", "y", "t"
}
_HANDLER_NAME = "dashboard.debug.console"
_handler_lock = threading.Lock()
_handler_ready = False


def debug_enabled() -> bool:
    """Whether debug diagnostics were enabled when this module was imported."""
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
            handler.setFormatter(logging.Formatter("%(message)s"))
            logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
        _handler_ready = True


def debug(message: str, *args: object) -> None:
    """Log a ``DEBUG`` diagnostic message; a no-op while debugging is disabled.

    ``message`` uses ``%``-style placeholders so ``args`` are only formatted
    when the record is emitted.
    """
    if not _ENABLED:
        return
    _ensure_handler()
    logger.debug(message, *args)
