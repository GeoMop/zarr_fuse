"""Opt-in measurement of outgoing Bokeh document patches.

Enable it like the rest of the profiling layer, before the server starts::

    DASHBOARD_PROFILE=1 zf-dashboard

When profiling is disabled neither the Bokeh ``Message`` class nor its ``send``
method is touched, so the disabled path keeps zero overhead.

``Message.send`` serializes the header/metadata/content lazily and returns the
total number of characters written to the websocket. Timing it therefore covers
JSON serialization *and* the websocket write, which is exactly the server-side
part of a document update. Only ``PATCH-DOC`` messages are reported; the initial
``PULL-DOC-REPLY`` is not part of the progressive fill.

Emitted records (via ``dashboard.profiling.log_event``)::

    event=patch.send bytes=4194304 duration_ms=812.4 t=1712345678.9

``bytes`` is the total wire size of the message (header + metadata + content),
``duration_ms`` the serialize+write time and ``t`` a wall-clock timestamp that
can be correlated with browser/DevTools timings.
"""

from __future__ import annotations

import time

from bokeh.protocol.message import Message

from dashboard.profiling import log_event, profiling_enabled

_ORIGINAL_SEND = Message.send
_installed = False


async def _timed_send(self: Message, conn) -> int:
    """Time a ``PATCH-DOC`` send and report its size; defer other messages."""
    if type(self).msgtype != "PATCH-DOC":
        return await _ORIGINAL_SEND(self, conn)
    start = time.perf_counter()
    sent = await _ORIGINAL_SEND(self, conn)
    log_event(
        "patch.send",
        bytes=sent,
        duration_ms=round((time.perf_counter() - start) * 1000, 3),
        t=round(time.time(), 3),
    )
    return sent


def install() -> None:
    """Patch ``Message.send`` once, but only when profiling is enabled."""
    global _installed
    if _installed or not profiling_enabled():
        return
    Message.send = _timed_send
    _installed = True
    log_event("patch_probe.installed")
