import os
import time
import json
import tempfile
from pathlib import Path

import boto3
import yaml
from tornado.web import RequestHandler, HTTPError

ACCESS_KEY = os.getenv("ZF_S3_ACCESS_KEY")
SECRET_KEY = os.getenv("ZF_S3_SECRET_KEY")
ENDPOINT_URL = os.getenv("ZF_S3_ENDPOINT_URL")

# Externalize bucket/prefix from environment or defaults
BUCKET_NAME = os.getenv("TILE_BUCKET", "app-databuk-test-service")
PREFIX = os.getenv("TILE_PREFIX", "test_tiles/")
DEFAULT_EXPIRES_IN = 300
EXPIRY_BUFFER_SECONDS = 30


def _resolve_endpoints_path() -> Path:
    env_path = os.getenv("ENDPOINTS_PATH")
    if env_path:
        return Path(env_path)
    return (
        Path(__file__).resolve().parent.parent
        / "app"
        / "databuk"
        / "config"
        / "endpoints.yaml"
    )


def _cache_dir_from_endpoints() -> str | None:
    endpoint_name = os.getenv("HV_DASHBOARD_ENDPOINT")
    if not endpoint_name:
        return None

    endpoints_path = _resolve_endpoints_path()
    if not endpoints_path.exists():
        return None

    try:
        with endpoints_path.open("r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}
    except Exception:
        return None

    endpoint = config.get(endpoint_name)
    if not isinstance(endpoint, dict):
        return None

    visualization = endpoint.get("visualization", {})
    overlay = visualization.get("overlay", {}) if isinstance(visualization, dict) else {}
    cache_dir = overlay.get("cache_dir") if isinstance(overlay, dict) else None

    # Backward compatibility for older endpoint configs.
    if not isinstance(cache_dir, str) or not cache_dir.strip():
        tile_build = endpoint.get("tile_build", {})
        if isinstance(tile_build, dict):
            cache_dir = tile_build.get("cache_dir")

    if not isinstance(cache_dir, str) or not cache_dir.strip():
        return None

    expanded = os.path.expandvars(os.path.expanduser(cache_dir.strip()))
    candidate = Path(expanded)
    if candidate.is_absolute():
        return str(candidate)

    # Relative paths are resolved against the project base dir (parent of config dir).
    base_dir = endpoints_path.parent.parent
    return str(base_dir / candidate)


# Cache location precedence:
# 1) ZF_CACHE_DIR env var
# 2) visualization.overlay.cache_dir in endpoints.yaml for selected endpoint
# 3) OS temp directory
CACHE_DIR = Path(
    os.getenv("ZF_CACHE_DIR")
    or _cache_dir_from_endpoints()
    or tempfile.gettempdir()
)
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_FILE = CACHE_DIR / "tile_url_cache.json"

s3 = boto3.client(
    "s3",
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    endpoint_url=ENDPOINT_URL,
)


def load_cache() -> dict:
    if not CACHE_FILE.exists():
        return {}

    try:
        with CACHE_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return {}

    now = time.time()
    return {
        k: v for k, v in data.items()
        if v.get("expires_at", 0) > now
    }


def save_cache(data: dict) -> None:
    tmp = CACHE_FILE.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    tmp.replace(CACHE_FILE)


cache: dict[str, dict] = load_cache()


def tile_id(z: int, x: int, y: int) -> str:
    return f"{z}/{x}/{y}"


def tile_key(z: int, x: int, y: int) -> str:
    return f"{PREFIX}{z}/{x}/{y}.png"


def get_tile_url(z: int, x: int, y: int, expires_in: int = DEFAULT_EXPIRES_IN) -> str:
    tid = tile_id(z, x, y)
    now = time.time()

    item = cache.get(tid)
    if item and item.get("expires_at", 0) > now:
        return item["url"]

    key = tile_key(z, x, y)

    url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": BUCKET_NAME, "Key": key},
        ExpiresIn=expires_in,
    )

    cache[tid] = {
        "url": url,
        "expires_at": now + expires_in - EXPIRY_BUFFER_SECONDS,
    }
    save_cache(cache)
    return url


class S3TileHandler(RequestHandler):
    def get(self, z: str, x: str, y: str):
        try:
            z_i, x_i, y_i = int(z), int(x), int(y)
            url = get_tile_url(z_i, x_i, y_i)
        except Exception as e:
            raise HTTPError(404, f"Could not resolve tile {z}/{x}/{y}: {e}")

        self.redirect(url, permanent=False)