"""Tests for the dashboard debug logging utility.

The ``dashboard.debug`` records are always enabled; these tests attach a
capturing handler and assert on records without depending on console output.
They also verify that the ``[config]`` diagnostics are wired to the logger.

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
    """Capture every ``dashboard.debug`` record."""
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


def _console_handlers() -> list[logging.Handler]:
    return [h for h in debug_log.logger.handlers if h.name == debug_log._HANDLER_NAME]


def test_debug_emits_record(debug_capture: list[str]) -> None:
    """debug() emits a DEBUG record with lazy %-formatting."""
    debug_log.debug("[config] x=%s y=%s", 5, "five")

    assert debug_capture == ["[config] x=5 y=five"]


def test_debug_ignores_legacy_env_flag(
    debug_capture: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The removed DASHBOARD_DEBUG flag no longer suppresses diagnostics."""
    monkeypatch.setenv("DASHBOARD_DEBUG", "0")

    debug_log.debug("[config] always emitted")

    assert debug_capture == ["[config] always emitted"]


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


def test_read_schema_display_logs(debug_capture: list[str], tmp_path: Path) -> None:
    """The schema display probe is emitted once."""
    schema_path = _write_schema(tmp_path)

    config._read_schema_display(schema_path, None, None, None, None)

    assert any(
        line.startswith("[config] _read_schema_display: opening")
        for line in debug_capture
    )
