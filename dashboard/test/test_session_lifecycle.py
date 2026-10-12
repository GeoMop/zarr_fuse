"""Integration tests for the dashboard server session lifecycle.

These tests boot the real ``python -m dashboard.serve_dashboard`` entrypoint
as a subprocess on a loopback port and drive it over the Bokeh WebSocket
protocol.  The server creates/reuses a session at handshake time
(``create_session_if_needed`` in ``bokeh/server/views/ws.py``), so connecting
with a session token is enough to observe the lifecycle; the tests assert
against the ``[session]`` terminal logs that:

* a reconnect while the session is still alive resumes it (no rebuild);
* after ``unused_session_lifetime`` the session is discarded and a new one is
  built for the same session id.

The server runs with Bokeh-standard defaults (15000 ms unused-session
lifetime, 17000 ms cleanup scan, 300 s token).  Because the discard only
happens on the next cleanup sweep, the affected test waits up to 60 s.  If the
dashboard server cannot start in the current environment (missing view config,
missing credentials), the module skips instead of failing.
"""

from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest
from bokeh.util.token import generate_jwt_token
from tornado.websocket import websocket_connect

REPO_ROOT = Path(__file__).resolve().parents[2]


class _ServerLog:
    """Continuously drains a subprocess stdout pipe and keeps every line."""

    def __init__(self, proc: subprocess.Popen) -> None:
        self._lines: list[str] = []
        self._thread = threading.Thread(
            target=self._read, args=(proc.stdout,), daemon=True
        )
        self._thread.start()

    def _read(self, stream) -> None:
        for raw_line in stream:
            self._lines.append(raw_line.rstrip("\n"))

    def count(self, substring: str) -> int:
        return sum(1 for line in self._lines if substring in line)

    def contains(self, substring: str) -> bool:
        return self.count(substring) > 0

    def snapshot(self) -> str:
        return "\n".join(self._lines)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _env_with(port: int) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "SERVE_BIND": "127.0.0.1",
            "SERVE_PORT": str(port),
            # stdout is a pipe here; without this the subprocess block-buffers
            # its prints and our readiness/log assertions never see them.
            "PYTHONUNBUFFERED": "1",
        }
    )
    return env


def _wait_for(log: _ServerLog, proc: subprocess.Popen, substring: str,
              timeout: float) -> None:
    """Poll the captured server output until ``substring`` appears."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if log.contains(substring):
            return
        if proc.poll() is not None:
            raise RuntimeError(
                f"dashboard server exited early (rc={proc.returncode}):\n"
                f"{log.snapshot()}"
            )
        time.sleep(0.05)
    raise TimeoutError(
        f"timeout waiting for {substring!r} in server output:\n{log.snapshot()}"
    )


async def _ws_roundtrip(url: str, session_id: str) -> None:
    """Open one websocket to the app and wait for the server-side ACK.

    The server creates (or reuses) the session named ``session_id`` while
    accepting the handshake, so no document round-trip is required.
    """
    ws_url = url.replace("http://", "ws://").replace("https://", "wss://")
    ws_url = f"{ws_url.rstrip('/')}/ws?bokeh-protocol-version=1.0"
    token = generate_jwt_token(session_id, signed=False)
    conn = await websocket_connect(
        ws_url, subprotocols=["bokeh", token], connect_timeout=30
    )
    try:
        ack = await asyncio.wait_for(conn.read_message(), timeout=30)
        if ack is None:
            raise RuntimeError(
                f"websocket closed before ACK for session id={session_id!r}"
            )
    finally:
        conn.close()


def _open_ws(url: str, session_id: str, log: _ServerLog, proc: subprocess.Popen):
    """Open a session websocket, tolerating the brief startup race."""
    deadline = time.time() + 30
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            asyncio.run(_ws_roundtrip(url, session_id))
            return
        except Exception as exc:  # noqa: BLE001 - retry transient startup errors
            last_error = exc
            if proc.poll() is not None:
                raise RuntimeError(
                    f"dashboard server exited early (rc={proc.returncode}):\n"
                    f"{log.snapshot()}"
                ) from exc
            time.sleep(0.5)
    raise TimeoutError(
        f"websocket connect failed for session id={session_id!r} "
        f"(last error: {last_error}):\n{log.snapshot()}"
    ) from last_error


@pytest.fixture(scope="module")
def dashboard_server():
    """Boot dashboard.serve_dashboard once and share it across the module."""
    port = _free_port()
    proc = subprocess.Popen(
        [sys.executable, "-m", "dashboard.serve_dashboard"],
        cwd=REPO_ROOT,
        env=_env_with(port),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    log = _ServerLog(proc)
    try:
        _wait_for(log, proc, "Launching server at", timeout=120)
    except (RuntimeError, TimeoutError) as exc:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
        pytest.skip(f"dashboard server unavailable in this environment: {exc}")

    url = f"http://127.0.0.1:{port}/"
    try:
        yield {"url": url, "proc": proc, "log": log}
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


class TestSessionLifecycle:
    def test_reconnect_within_lifetime_resumes_live_session(self, dashboard_server):
        """Reconnecting an id that still exists must not rebuild the dashboard."""
        url = dashboard_server["url"]
        proc = dashboard_server["proc"]
        log = dashboard_server["log"]
        session_id = "t1resume"

        _open_ws(url, session_id, log, proc)
        _wait_for(log, proc, f"build_dashboard started session_id={session_id}",
                  timeout=120)

        # Fast reconnect, far below the 2s unused-session lifetime.
        _open_ws(url, session_id, log, proc)

        # The original session was served again, not rebuilt.
        assert log.count(
            f"build_dashboard started session_id={session_id}"
        ) == 1

    def test_reconnect_after_discard_rebuilds_session(self, dashboard_server):
        """After the unused-session lifetime, the saved session is discarded
        and the same id is served from a freshly built session."""
        url = dashboard_server["url"]
        proc = dashboard_server["proc"]
        log = dashboard_server["log"]
        session_id = "t2discard"

        _open_ws(url, session_id, log, proc)
        _wait_for(log, proc, f"build_dashboard started session_id={session_id}",
                  timeout=120)

        # Wait for the 15s standard lifetime + cleanup sweep to discard it.
        _wait_for(log, proc, f"session destroyed session_id={session_id}",
                  timeout=60)

        _open_ws(url, session_id, log, proc)
        _wait_for(log, proc, f"build_dashboard started session_id={session_id}",
                  timeout=120)

        assert log.count(
            f"build_dashboard started session_id={session_id}"
        ) == 2