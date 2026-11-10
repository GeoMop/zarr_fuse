import pytest

from dashboard import composed


def test_build_failure_returns_crash_page(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail():
        raise RuntimeError("store unavailable")

    monkeypatch.setattr(composed, "build_dashboard", fail)

    page = composed.build_dashboard_or_error()

    text = "".join(str(getattr(obj, "object", "")) for obj in page.objects)
    assert "store unavailable" in text
    assert "Traceback" in text
    assert 'href="/logs"' in text
    assert 'target="_blank"' in text
