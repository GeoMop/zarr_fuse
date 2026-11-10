import pytest

from dashboard import log_buffer


@pytest.fixture(autouse=True)
def clean_log_buffer() -> None:
    log_buffer.clear()
    yield
    log_buffer.clear()
