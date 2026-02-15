import os
import pytest
import logging
from pathlib import Path

@pytest.fixture
def smart_tmp_path(request):
    # Use persistent workdir for local/dev runs
    script_dir = Path(__file__).parent
    workdir = script_dir / "workdir"
    workdir.mkdir(parents=True, exist_ok=True)
    yield workdir



def attach_caplog_handler_to(logger, caplog):
    """Attach the internal pytest caplog handler to the given logger."""
    handler = caplog.handler
    # Ensure the logger level allows capturing
    logger.setLevel(logging.NOTSET)
    logger.addHandler(handler)
    # Optionally set propagate=True so logs still flow upward
    logger.propagate = True
    return handler

@pytest.fixture
def attach_logger(caplog):
    def _attach(logger):
        return attach_caplog_handler_to(logger, caplog)
    return _attach