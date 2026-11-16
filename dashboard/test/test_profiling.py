"""Tests for the dashboard performance profiling utility.

The enabled flag is fixed at import time by ``DASHBOARD_PROFILE``, so these
tests flip ``profiling._ENABLED`` directly and attach a capturing handler to
the ``dashboard.performance`` logger to assert on records without depending on
console output.

Run: python -m pytest dashboard/test/test_profiling.py -v
"""

from __future__ import annotations

import asyncio
import logging
import re
import threading

import pytest

from dashboard import perf_summary, profiling


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


def _line_for(lines: list[str], stage: str) -> str:
    matches = [line for line in lines if f"stage={stage} " in line]
    assert matches, f"no record for stage {stage!r} in {lines}"
    return matches[0]


def test_disabled_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(profiling, "_ENABLED", False)
    assert not profiling.profiling_enabled()
    profiling.log_stage("silent", 1.0)
    profiling.log_event("silent.event")
    profiling.log_message("counters report")


def test_timed_sync_reports_ok(perf_log: list[str]) -> None:
    @profiling.timed("get_map_data")
    def _fast() -> int:
        return 7

    assert _fast() == 7

    line = _line_for(perf_log, "get_map_data")
    assert re.search(r"stage=get_map_data duration_ms=\d+\.\d+ status=ok", line), line


def test_timed_sync_status_error(perf_log: list[str]) -> None:
    @profiling.timed("fetch_fail")
    def _boom() -> None:
        raise RuntimeError("nope")

    with pytest.raises(RuntimeError):
        _boom()

    line = _line_for(perf_log, "fetch_fail")
    assert re.search(r"stage=fetch_fail duration_ms=\d+\.\d+ status=error", line), line


def test_timed_async_awaits_until_completion(perf_log: list[str]) -> None:
    @profiling.timed("demo")
    async def _slow() -> int:
        await asyncio.sleep(0.05)
        return 42

    assert asyncio.run(_slow()) == 42

    line = _line_for(perf_log, "demo")
    match = re.search(r"stage=demo duration_ms=(\d+\.\d+) status=ok", line)
    assert match, line
    assert float(match.group(1)) >= 40.0


def test_timed_async_status_error(perf_log: list[str]) -> None:
    @profiling.timed("async_fail")
    async def _boom() -> None:
        raise ValueError("bad")

    with pytest.raises(ValueError):
        asyncio.run(_boom())

    line = _line_for(perf_log, "async_fail")
    assert re.search(r"stage=async_fail duration_ms=\d+\.\d+ status=error", line), line


def test_operation_tags_nested_stages(perf_log: list[str]) -> None:
    with profiling.operation("borehole.add", site="L5-42L") as op:
        profiling.log_stage("data.get_timeseries", 0.001)
        profiling.log_event("selection.fill_site")
        with profiling.operation("inner", detail="nested"):
            profiling.log_stage("data.read_zarr", 0.0005)

    outer = f"op={op.op_id}"
    assert outer in _line_for(perf_log, "data.get_timeseries")
    fill = [line for line in perf_log if line.startswith("event=selection.fill_site")]
    assert len(fill) == 1 and outer in fill[0]
    close = [line for line in perf_log if "stage=borehole.add " in line]
    assert len(close) == 1 and outer in close[0]
    inner_close = [line for line in perf_log if "stage=inner " in line]
    assert len(inner_close) == 1 and outer not in inner_close[0]
    inner_op = re.search(r"op=(\d+)", inner_close[0])
    assert inner_op is not None
    read = _line_for(perf_log, "data.read_zarr")
    assert f"op={inner_op.group(1)}" in read

    profiling.log_stage("outside", 0.001)
    assert re.search(r"\bop=\d+", _line_for(perf_log, "outside")) is None


def test_operation_status_error(perf_log: list[str]) -> None:
    with pytest.raises(KeyError), profiling.operation("borehole.add", site="X"):
        raise KeyError("x")

    close = [line for line in perf_log if "stage=borehole.add " in line]
    assert len(close) == 1
    assert "status=error" in close[0], close


def test_operation_activate_and_finish(perf_log: list[str]) -> None:
    op = profiling.operation("data.reload", view="v1")
    with op.activate():
        profiling.log_stage("reload.cache.clear", 0.001)
    op.finish(view="v1", group="/")

    tagged = _line_for(perf_log, "reload.cache.clear")
    assert f"op={op.op_id}" in tagged, tagged
    close = _line_for(perf_log, "data.reload")
    assert f"op={op.op_id}" in close and "view=v1" in close, close


def test_operation_wrap_finish_in_callback(perf_log: list[str]) -> None:
    with profiling.session("s-9"):
        op = profiling.operation("borehole.select", site="L5-42L")
        wrapped = op.wrap(_deferred_callback, finish=True)

    wrapped()

    close = _line_for(perf_log, "borehole.select")
    assert f"op={op.op_id}" in close, close
    assert "site=L5-42L" in close, close
    assert "session=s-9" in close, close


def _deferred_callback() -> int:
    return 1


def test_operation_wrap_survives_exception(perf_log: list[str]) -> None:
    op = profiling.operation("fetch_fail", site="X")

    def _boom() -> None:
        raise RuntimeError("nope")

    wrapped = op.wrap(_boom, finish=True)
    with pytest.raises(RuntimeError):
        wrapped()

    close = _line_for(perf_log, "fetch_fail")
    assert "status=error" in close, close


def test_session_tags_scope(perf_log: list[str]) -> None:
    with profiling.session("s-7"):
        profiling.log_stage("build", 0.1)
        tagged = _line_for(perf_log, "build")

    profiling.log_stage("later", 0.1)
    later = _line_for(perf_log, "later")

    assert "session=s-7" in tagged, tagged
    assert re.search(r"session=s-\d+", later) is None, later


def test_operation_tracked_across_await(perf_log: list[str]) -> None:
    async def _work() -> None:
        with profiling.operation("async.op", site="S1"):
            await asyncio.sleep(0.01)
            profiling.log_stage("data.fetch", 0.001)

    asyncio.run(_work())

    op = _line_for(perf_log, "data.fetch")
    match = re.search(r"op=(\d+)", op)
    assert match, op
    closed = _line_for(perf_log, "async.op")
    assert f"op={match.group(1)}" in closed, closed


def test_ensure_handler_attaches_once_concurrently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(profiling, "_ENABLED", True)
    profiling._handler_ready = False
    profiling.logger.handlers = [
        h for h in profiling.logger.handlers if not isinstance(h, logging.StreamHandler)
    ]

    barrier = threading.Barrier(16)

    def _emit() -> None:
        barrier.wait()
        profiling.log_stage("thread", 0.001)

    threads = [threading.Thread(target=_emit) for _ in range(16)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    handlers = [
        h for h in profiling.logger.handlers if isinstance(h, logging.StreamHandler)
    ]
    assert len(handlers) == 1


# --------------------------------------------------------------------------- #
# per-operation aggregation (dashboard.perf_summary)
# --------------------------------------------------------------------------- #


@pytest.fixture(autouse=True)
def _reset_summary_stack() -> None:
    """Ensure no thread-local summary leaks between tests."""
    perf_summary._local.__dict__.pop("stack", None)
    yield
    perf_summary._local.__dict__.pop("stack", None)


def _summary_lines(lines: list[str], event: str) -> list[str]:
    return [line for line in lines if line.startswith(f"event={event} ")]


def test_summary_aggregates_stage_and_counters(perf_log: list[str]) -> None:
    with perf_summary.measure_operation("variable.switch", variable="temp"):
        for _ in range(3):
            with perf_summary.stage("ts.read"):
                pass
        perf_summary.count("cache_hit", 2)
        perf_summary.count("cache_hit")

    headers = _summary_lines(perf_log, "op.summary")
    assert len(headers) == 1
    assert "operation=variable.switch" in headers[0]
    assert "variable=temp" in headers[0]
    assert "total_ms=" in headers[0]

    stages = [line for line in _summary_lines(perf_log, "op.stage") if " stage=ts.read " in line]
    assert len(stages) == 1, stages
    assert "count=3" in stages[0] and "total_ms=" in stages[0] and "max_ms=" in stages[0]

    counters = _summary_lines(perf_log, "op.counters")
    assert len(counters) == 1 and "cache_hit=3" in counters[0]


def test_summary_bounds_stage_lines(perf_log: list[str]) -> None:
    with perf_summary.measure_operation("busy"):
        for index in range(20):
            with perf_summary.stage(f"s{index}"):
                pass

    assert len(_summary_lines(perf_log, "op.stage")) == perf_summary._MAX_STAGE_LINES


def test_timer_records_once(perf_log: list[str]) -> None:
    with perf_summary.measure_operation("op"):
        timer = perf_summary.Timer("work")
        timer.stop()
        timer.stop()

    stages = [line for line in _summary_lines(perf_log, "op.stage") if " stage=work " in line]
    assert len(stages) == 1 and "count=1" in stages[0]


def test_nested_summaries_emit_separately(perf_log: list[str]) -> None:
    with perf_summary.measure_operation("outer"):
        perf_summary.add_stage("a", 0.001)
        with perf_summary.measure_operation("inner"):
            perf_summary.add_stage("b", 0.001)
        perf_summary.add_stage("c", 0.001)

    headers = _summary_lines(perf_log, "op.summary")
    names = [line for line in headers if "operation=outer" in line or "operation=inner" in line]
    assert {line.split("operation=")[1].split(" ")[0] for line in names} == {"outer", "inner"}
    assert perf_summary.current() is None


def test_probes_outside_operation_are_silent(perf_log: list[str]) -> None:
    with perf_summary.stage("x"):
        pass
    perf_summary.add_stage("y", 0.1)
    perf_summary.count("z")

    assert perf_log == []


def test_summary_disabled_is_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(profiling, "_ENABLED", False)
    with perf_summary.measure_operation("op"):
        with perf_summary.stage("x"):
            pass
        perf_summary.count("c")

    assert perf_summary.current() is None