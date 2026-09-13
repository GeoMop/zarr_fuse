import os
import pytest

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
def s3_queue_config(load_repo_secret_env, monkeypatch) -> dict:
    """
    S3 connection config for queue tests against the real (CESNET) endpoint.
    Skips when the S3 secrets are not available.
    """
    if not (os.getenv("ZF_S3_ACCESS_KEY") and os.getenv("ZF_S3_SECRET_KEY")):
        pytest.skip(
            "S3 secrets not available (ZF_S3_ACCESS_KEY/ZF_S3_SECRET_KEY); "
            "provide them via .secrets_env in the repo root"
        )

    endpoint_url = os.getenv("ZF_S3_ENDPOINT_URL", "https://s3.cl4.du.cesnet.cz")
    monkeypatch.setenv("ZF_S3_ENDPOINT_URL", endpoint_url)

    return {
        "bucket_name": os.getenv("ZF_S3_BUCKET_NAME", "test-zarr-storage"),
        "endpoint_url": endpoint_url,
    }
