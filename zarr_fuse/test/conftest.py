import os
import pytest
import logging
from pathlib import Path

from dotenv import load_dotenv


def _repo_secret_env_files() -> list[Path]:
    """Return supported local secret env files in preferred lookup order."""
    repo_root = Path(__file__).resolve().parents[2]
    return [
        repo_root / ".secrets_env",
    ]


@pytest.fixture(scope="session")
def load_repo_secret_env() -> Path | None:
    """Load repo-local secret environment variables for tests when available."""
    for env_file in _repo_secret_env_files():
        if env_file.exists():
            load_dotenv(env_file, override=False)
            return env_file
    return None


@pytest.fixture
def secret_getenv(load_repo_secret_env):
    """Return a strict environment lookup helper for secret-backed tests."""

    sentinel = object()

    def _getenv(name: str, default=sentinel):
        value = os.getenv(name)
        if value is not None:
            return value
        if default is not sentinel:
            return default
        raise AssertionError(f"Missing required test environment variable: {name}")

    return _getenv


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
