"""In-process capture of dashboard terminal output for the log viewers.

This module records stdout and stderr in a bounded in-memory ring buffer, masks
obvious secrets, and exposes snapshots for the modal and the standalone
``/logs`` page.

The dashboard prints diagnostics (``[populate]``, ``[fetch_ts]``, ...) to stdout and
``dashboard.debug`` / ``dashboard.performance`` write through ``logging`` handlers bound
to stderr. :func:`install` replaces both streams with a transparent tee, so the terminal
output is unchanged while completed lines are also kept in a bounded ring buffer that the
``Server logs`` view renders.

Install it once, at the very top of :mod:`dashboard.serve_dashboard`, before importing
dashboard modules that create logging handlers::

    from dashboard.log_buffer import install as install_log_buffer
    install_log_buffer()

The buffer is process-global. Secrets read from the environment are masked before a line
is stored, so the same lines can be shown to every dashboard user.
"""

from __future__ import annotations

import os
import re
import sys
import threading
import time
from collections import deque
from typing import TextIO

MAX_LINES = 5000

_SECRET_ENV_VARS = (
    "S3_ACCESS_KEY",
    "S3_SECRET_KEY",
    "ZF_S3_ACCESS_KEY",
    "ZF_S3_SECRET_KEY",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
)
_URI_CREDENTIALS = re.compile(r"(://[^/\s:@]+:)[^@\s/]+@")

_lines: deque[tuple[int, str, str]] = deque(maxlen=MAX_LINES)
_next_seq = 0
_lines_lock = threading.Lock()

_install_lock = threading.Lock()
_installed = False


def mask_secrets(text: str) -> str:
    """Return ``text`` with credential values replaced by ``***``."""
    for name in _SECRET_ENV_VARS:
        value = os.getenv(name, "").strip()
        if len(value) >= 4:
            text = text.replace(value, "***")
    return _URI_CREDENTIALS.sub(r"\1***@", text)


def append(text: str, ts: str | None = None) -> None:
    """Store one already-decoded line in the ring buffer."""
    global _next_seq
    line = mask_secrets(text.rstrip("\r"))
    stamp = time.strftime("%H:%M:%S") if ts is None else ts
    with _lines_lock:
        _next_seq += 1
        _lines.append((_next_seq, stamp, line))


def entries_since(since: int = 0) -> tuple[list[tuple[int, str, str]], int]:
    """Return buffered ``(seq, timestamp, line)`` entries newer than ``since``.

    The second value is the newest sequence number, which callers pass back as
    ``since`` on their next poll. Sequence numbers are monotonic for the process.
    """
    with _lines_lock:
        snapshot = list(_lines)
        next_seq = _next_seq
    return [entry for entry in snapshot if entry[0] > since], next_seq


def lines(limit: int | None = None) -> list[tuple[str, str]]:
    """Return a snapshot of ``(timestamp, line)`` pairs, optionally only the last ``limit``."""
    rows, _ = entries_since()
    if limit is not None and limit >= 0:
        rows = rows[-limit:] if limit else []
    return [(stamp, line) for _, stamp, line in rows]


def count() -> int:
    """Number of buffered lines."""
    with _lines_lock:
        return len(_lines)


def clear() -> None:
    """Drop all buffered lines."""
    with _lines_lock:
        _lines.clear()


class _StreamTee:
    """Forward a text stream unchanged while capturing completed lines."""

    def __init__(self, original: TextIO) -> None:
        self._original = original
        self._pending = ""
        self._lock = threading.Lock()

    def write(self, text: str | bytes) -> object:
        if isinstance(text, (bytes, bytearray)):
            text = bytes(text).decode("utf-8", "replace")
        result = self._original.write(text)
        with self._lock:
            pending = (self._pending + text).replace("\r\n", "\n")
            chunks = pending.split("\n")
            self._pending = chunks.pop()
            if "\r" in self._pending:
                completed, _, self._pending = self._pending.rpartition("\r")
                chunks.extend(completed.split("\r"))
        for chunk in chunks:
            append(chunk)
        return result

    def flush(self) -> object:
        return self._original.flush()

    def __getattr__(self, name: str) -> object:
        return getattr(self._original, name)


def install() -> bool:
    """Tee ``sys.stdout`` and ``sys.stderr`` into the buffer; return whether it was installed.

    Calling it repeatedly is safe and the second call is a no-op.
    """
    global _installed
    with _install_lock:
        if _installed:
            return False
        stdout, stderr = sys.stdout, sys.stderr
        if stdout is not None and not isinstance(stdout, _StreamTee):
            sys.stdout = _StreamTee(stdout)  # type: ignore[assignment]
        if stderr is not None and not isinstance(stderr, _StreamTee):
            sys.stderr = _StreamTee(stderr)  # type: ignore[assignment]
        _installed = True
        return True
