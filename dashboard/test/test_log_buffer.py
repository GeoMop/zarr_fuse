import io
import sys

import pytest

from dashboard import log_buffer


def test_tee_forwards_and_captures() -> None:
    original = io.StringIO()
    tee = log_buffer._StreamTee(original)

    tee.write("one\n")
    tee.write("two\nt")
    tee.write("hree\n")

    assert original.getvalue() == "one\ntwo\nthree\n"
    assert [line for _, line in log_buffer.lines()] == ["one", "two", "three"]


def test_partial_line_is_buffered_until_newline() -> None:
    tee = log_buffer._StreamTee(io.StringIO())

    tee.write("no newline yet")
    assert log_buffer.lines() == []

    tee.write("\n")
    assert [line for _, line in log_buffer.lines()] == ["no newline yet"]


def test_carriage_returns_split_progress_lines() -> None:
    tee = log_buffer._StreamTee(io.StringIO())

    tee.write("10%\r50%\r")
    tee.write("100%\n")

    assert [line for _, line in log_buffer.lines()] == ["10%", "50%", "100%"]


def test_tee_delegates_unknown_attributes() -> None:
    original = io.StringIO()
    tee = log_buffer._StreamTee(original)

    assert tee.encoding == original.encoding
    assert tee.closed is False
    assert tee.writable() is True


def test_ring_buffer_keeps_only_the_last_lines() -> None:
    for index in range(log_buffer.MAX_LINES + 10):
        log_buffer.append(f"line {index}", ts="00:00:00")

    assert log_buffer.count() == log_buffer.MAX_LINES
    snapshot = log_buffer.lines()
    assert snapshot[0][1] == "line 10"
    assert snapshot[-1][1] == f"line {log_buffer.MAX_LINES + 9}"


def test_lines_limit_returns_the_tail() -> None:
    for index in range(5):
        log_buffer.append(f"line {index}", ts="00:00:00")

    assert [line for _, line in log_buffer.lines(2)] == ["line 3", "line 4"]
    assert log_buffer.lines(0) == []


def test_timestamps_are_recorded() -> None:
    log_buffer.append("hello", ts="12:34:56")
    log_buffer.append("now")

    snapshot = log_buffer.lines()
    assert snapshot[0] == ("12:34:56", "hello")
    assert snapshot[1][0].count(":") == 2


def test_secrets_are_masked(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("S3_SECRET_KEY", "super-secret-value")
    monkeypatch.setenv("S3_ACCESS_KEY", "minioadmin")

    log_buffer.append("key=super-secret-value and minioadmin")
    log_buffer.append("s3://user:hunter2@bucket.example/path")

    assert [line for _, line in log_buffer.lines()] == [
        "key=*** and ***",
        "s3://user:***@bucket.example/path",
    ]


def test_entries_since_returns_only_new_lines() -> None:
    log_buffer.append("one", ts="00:00:00")
    log_buffer.append("two", ts="00:00:01")

    rows, next_seq = log_buffer.entries_since(0)

    assert [text for _, _, text in rows] == ["one", "two"]
    assert [stamp for _, stamp, _ in rows] == ["00:00:00", "00:00:01"]

    rows, same_seq = log_buffer.entries_since(rows[0][0])

    assert [text for _, _, text in rows] == ["two"]
    assert same_seq == next_seq
    assert log_buffer.entries_since(next_seq)[0] == []


def test_entries_since_respects_the_ring_cap() -> None:
    for index in range(log_buffer.MAX_LINES + 10):
        log_buffer.append(f"line {index}", ts="00:00:00")

    rows, next_seq = log_buffer.entries_since(0)

    assert len(rows) == log_buffer.MAX_LINES
    assert rows[0][2] == "line 10"
    assert rows[-1][2] == f"line {log_buffer.MAX_LINES + 9}"
    assert next_seq == rows[-1][0]


def test_clear_drops_buffered_lines() -> None:
    log_buffer.append("one")
    log_buffer.append("two")

    log_buffer.clear()

    assert log_buffer.count() == 0


def test_install_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    stdout, stderr = io.StringIO(), io.StringIO()
    monkeypatch.setattr(sys, "stdout", stdout)
    monkeypatch.setattr(sys, "stderr", stderr)
    monkeypatch.setattr(log_buffer, "_installed", False)

    assert log_buffer.install() is True
    assert log_buffer.install() is False

    print("installed print")
    sys.stdout.flush()
    assert "installed print" in stdout.getvalue()
    assert any(line == "installed print" for _, line in log_buffer.lines())


def test_mask_secrets_ignores_short_values(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("S3_ACCESS_KEY", "ab")
    monkeypatch.setenv("S3_SECRET_KEY", "")

    assert log_buffer.mask_secrets("ab cd") == "ab cd"
