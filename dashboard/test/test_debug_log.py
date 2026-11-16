"""Tests for the dashboard opt-in debug logging utility.

``DASHBOARD_DEBUG`` fixes the enabled flag at import time, so these tests flip
``debug_log._ENABLED`` directly and attach a capturing handler to the
``dashboard.debug`` logger to assert on records without depending on console
output. They also verify that the ``[config]`` diagnostics are wired to the
logger and that the disabled path skips expensive preparation.

Run: python -m pytest dashboard/test/test_debug_log.py -v
"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest
import yaml

from dashboard import config, debug_log


@pytest.fixture
def debug_capture(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Force debug on and capture every ``dashboard.debug`` record."""
    monkeypatch.setattr(debug_log, "_ENABLED", True)
    monkeypatch.setattr(debug_log, "_handler_ready", False)
    captured: list[str] = []

    class _CapturingHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            captured.append(record.getMessage())

    handler = _CapturingHandler()
    handler.setLevel(logging.DEBUG)
    debug_log.logger.addHandler(handler)
    debug_log.logger.setLevel(logging.DEBUG)
    debug_log.logger.propagate = False
    yield captured
    debug_log.logger.removeHandler(handler)
    for existing in list(debug_log.logger.handlers):
        if existing.name == debug_log._HANDLER_NAME:
            debug_log.logger.removeHandler(existing)


class _Boom:
    def __repr__(self) -> str:
        raise AssertionError("debug arguments must not be formatted while disabled")


def _console_handlers() -> list[logging.Handler]:
    return [h for h in debug_log.logger.handlers if h.name == debug_log._HANDLER_NAME]


def test_disabled_is_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    """While disabled, debug() emits nothing and installs no console handler."""
    monkeypatch.setattr(debug_log, "_ENABLED", False)
    monkeypatch.setattr(debug_log, "_handler_ready", False)
    captured: list[str] = []

    class _CapturingHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            captured.append(record.getMessage())

    handler = _CapturingHandler()
    debug_log.logger.addHandler(handler)
    try:
        debug_log.debug("[config] not emitted")
    finally:
        debug_log.logger.removeHandler(handler)

    assert captured == []
    assert not debug_log._handler_ready
    assert _console_handlers() == []


def test_enabled_emits_debug(debug_capture: list[str]) -> None:
    """When enabled, debug() emits a DEBUG record with lazy %-formatting."""
    debug_log.debug("[config] x=%s y=%s", 5, "five")

    assert debug_capture == ["[config] x=5 y=five"]


def test_disabled_does_not_format_args(monkeypatch: pytest.MonkeyPatch) -> None:
    """Disabled debug() must not format arguments (no expensive __repr__)."""
    monkeypatch.setattr(debug_log, "_ENABLED", False)

    debug_log.debug("[ylim] n_visible=%s", _Boom())


def test_handler_installed_once(debug_capture: list[str]) -> None:
    """Repeated debug() calls install the console handler only once."""
    debug_log.debug("first")
    debug_log.debug("second")

    assert debug_capture == ["first", "second"]
    assert len(_console_handlers()) == 1


def test_find_view_file_logs_to_debug_logger(
    debug_capture: list[str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The [config] find_view_file diagnostic routes through dashboard.debug."""
    views_path = tmp_path / "zf_view.yaml"
    views_path.write_text("{}\n", encoding="utf-8")
    monkeypatch.setenv(config.VIEWS_ENV_VAR, str(views_path))

    assert config.find_view_file() == views_path.resolve()
    assert any(
        line.startswith("[config] find_view_file: from ENV ZF_VIEW_PATH=")
        for line in debug_capture
    )


def _write_schema(tmp_path: Path) -> Path:
    schema_path = tmp_path / "schema.yaml"
    schema_path.write_text(yaml.safe_dump({}), encoding="utf-8")
    return schema_path


def test_read_schema_display_skips_expense_when_disabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The schema_path.exists() probe is skipped entirely while disabled."""
    calls: list[tuple[str, tuple]] = []
    monkeypatch.setattr(config, "debug_enabled", lambda: False)
    monkeypatch.setattr(config, "debug", lambda msg, *args: calls.append((msg, args)))

    config._read_schema_display(_write_schema(tmp_path), None, None, None, None)

    assert calls == []


def test_read_schema_display_logs_when_enabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With debugging on, the schema display probe is emitted once."""
    schema_path = _write_schema(tmp_path)
    calls: list[tuple[str, tuple]] = []
    monkeypatch.setattr(config, "debug_enabled", lambda: True)
    monkeypatch.setattr(config, "debug", lambda msg, *args: calls.append((msg, args)))

    config._read_schema_display(schema_path, None, None, None, None)

    assert len(calls) == 1
    message, args = calls[0]
    assert message.startswith("[config] _read_schema_display: opening")
    assert args == (schema_path, True)
