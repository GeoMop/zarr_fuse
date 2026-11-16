from dashboard import log_buffer
from dashboard.log_format import LOG_CSS
from dashboard.log_view import LogView, build_logs_link
from dashboard.logs_page import LOGS_TAB_URL


def _view() -> LogView:
    return LogView(active=False)


def test_empty_state() -> None:
    view = _view()

    assert "No output captured yet" in view.body.object


def test_renders_timestamp_escaped_text_and_colour() -> None:
    log_buffer.append("<script>x</script> [populate] hello", ts="01:02:03")

    view = _view()

    assert "01:02:03" in view.body.object
    assert "&lt;script&gt;" in view.body.object
    assert 'class="zf-pop"' in view.body.object


def test_error_lines_are_red() -> None:
    log_buffer.append("Traceback (most recent call last):", ts="01:02:03")

    view = _view()

    assert 'class="zf-err"' in view.body.object


def test_runtime_error_lines_are_red() -> None:
    log_buffer.append("RuntimeError: boom", ts="01:02:03")

    view = _view()

    assert 'class="zf-err"' in view.body.object


def test_renders_only_the_tail() -> None:
    for index in range(400):
        log_buffer.append(f"line {index}", ts="00:00:00")

    view = _view()

    assert "line 399" in view.body.object
    assert "line 99" not in view.body.object


def test_inactive_view_has_no_callback() -> None:
    view = _view()

    assert view._callback is None


def test_set_active_false_stops_the_callback() -> None:
    view = _view()

    class Callback:
        stopped = False

        def stop(self) -> None:
            self.stopped = True

    callback = Callback()
    view._callback = callback

    view.set_active(False)

    assert callback.stopped is True
    assert view._callback is None


def test_refresh_updates_tail_and_keeps_scroll_client_side() -> None:
    log_buffer.append("before", ts="00:00:00")
    view = _view()
    log_buffer.append("after", ts="00:00:01")

    view.refresh()

    assert "after" in view.body.object
    assert "MutationObserver" in view._scroller.object
    assert ".zf-log-scroll" in view._scroller.object
    assert ".zf-log-body" in view._scroller.object
    assert "shadowRoot" in view._scroller.object


def test_logs_link_is_a_plain_text_trigger() -> None:
    link = build_logs_link()

    assert link.label == "Server logs"
    assert link.height == 28
    assert link.css_classes == ["zf-logs-link"]


def test_header_links_to_the_standalone_log_page() -> None:
    view = _view()

    assert f'href="{LOGS_TAB_URL}"' in view.label.object
    assert 'target="_blank"' in view.label.object


def test_header_and_body_use_the_shared_stylesheet() -> None:
    view = _view()

    assert LOG_CSS in view.label.stylesheets
    assert LOG_CSS in view.body.stylesheets
    assert view.body.css_classes == ["zf-log-body"]
    assert view.styles["overflow"] == "hidden"
    assert view.body_container.styles["overflow-y"] == "auto"
