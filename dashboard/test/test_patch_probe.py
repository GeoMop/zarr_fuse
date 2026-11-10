"""Tests for the opt-in outgoing patch probe.

Run: python -m pytest dashboard/test/test_patch_probe.py -v
"""

from __future__ import annotations

import asyncio
import logging

import pytest
from bokeh.protocol.message import Message

from dashboard import patch_probe, profiling

_ORIGINAL_MESSAGE_SEND = Message.send


@pytest.fixture
def perf_log(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Force profiling on and capture every ``dashboard.performance`` record."""
    monkeypatch.setattr(profiling, "_ENABLED", True)
    captured: list[str] = []

    class _CapturingHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            captured.append(record.getMessage())

    handler = _CapturingHandler()
    handler.setLevel(logging.DEBUG)
    profiling.logger.addHandler(handler)
    yield captured
    profiling.logger.removeHandler(handler)


@pytest.fixture(autouse=True)
def _restore_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    """Undo the class-level patch and the install flag after every test."""
    monkeypatch.setattr(patch_probe, "_installed", False)
    yield
    Message.send = _ORIGINAL_MESSAGE_SEND


class _PatchDocMessage:
    msgtype = "PATCH-DOC"


class _AckMessage:
    msgtype = "ACK"


def test_install_is_noop_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(profiling, "_ENABLED", False)

    patch_probe.install()

    assert Message.send is _ORIGINAL_MESSAGE_SEND
    assert not patch_probe._installed


def test_install_patches_once(perf_log: list[str]) -> None:
    patch_probe.install()
    first = Message.send
    patch_probe.install()

    assert Message.send is first
    assert first is patch_probe._timed_send


def test_patch_doc_send_is_logged(
    perf_log: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _fake_send(self: object, conn: object) -> int:
        return 1234

    monkeypatch.setattr(patch_probe, "_ORIGINAL_SEND", _fake_send)
    patch_probe.install()

    result = asyncio.run(Message.send(_PatchDocMessage(), object()))

    assert result == 1234
    line = next(line for line in perf_log if "event=patch.send" in line)
    assert "bytes=1234" in line
    assert "duration_ms=" in line
    assert " t=" in line


def test_other_messages_are_not_logged(
    perf_log: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _fake_send(self: object, conn: object) -> int:
        return 42

    monkeypatch.setattr(patch_probe, "_ORIGINAL_SEND", _fake_send)
    patch_probe.install()

    result = asyncio.run(Message.send(_AckMessage(), object()))

    assert result == 42
    assert not [line for line in perf_log if "event=patch.send" in line]
