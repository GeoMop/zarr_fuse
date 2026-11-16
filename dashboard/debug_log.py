"""Lightweight debug diagnostics for the Zarr Fuse dashboard.

Repetitive diagnostic messages (``[config]``, ``[timeseries]``, ``[ylim]``, ...)
are emitted through the separate ``dashboard.debug`` logger at ``DEBUG`` level.
The diagnostics are always enabled, independently of
:mod:`dashboard.profiling`.

Messages use ``%``-style placeholders so their arguments are only formatted
when a record is actually emitted::

    debug("[ylim] view=%s ylim=%s", view, ylim)
"""

from __future__ import annotations

import logging
import threading

logger = logging.getLogger("dashboard.debug")

_HANDLER_NAME = "dashboard.debug.console"
_handler_lock = threading.Lock()
_handler_ready = False


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
    """Log a ``DEBUG`` diagnostic message.

    ``message`` uses ``%``-style placeholders so ``args`` are only formatted
    when the record is emitted.
    """
    _ensure_handler()
    logger.debug(message, *args)
