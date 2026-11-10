from dashboard import log_buffer
from dashboard.logs_page import LOG_ROUTES, build_payload, render_page


def test_payload_contains_lines_colours_and_cursor() -> None:
    log_buffer.append("hello [populate]", ts="01:02:03")
    log_buffer.append("Traceback (most recent call last):", ts="01:02:04")

    payload = build_payload(0)

    assert payload["count"] == 2
    assert payload["lines"][0] == ["01:02:03", "hello [populate]", "zf-pop"]
    assert payload["lines"][1][2] == "zf-err"
    assert payload["next"] == log_buffer.entries_since(0)[1]


def test_payload_is_incremental() -> None:
    log_buffer.append("one")
    first = build_payload(0)
    log_buffer.append("two")

    second = build_payload(first["next"])

    assert [line[1] for line in second["lines"]] == ["two"]
    assert second["next"] > first["next"]


def test_page_contains_live_polling_and_client_side_escaping() -> None:
    page = render_page()

    assert "/logs.json" in page
    assert "textContent" in page
    assert "zf-log-body" in page
    assert "zf-log-scroll" in page
    assert "setInterval" in page


def test_log_routes() -> None:
    patterns = [pattern for pattern, _ in LOG_ROUTES]

    assert r"/logs" in patterns
    assert r"/logs\.json" in patterns
