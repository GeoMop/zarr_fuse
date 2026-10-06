import os
import time
import json
import tempfile
from pathlib import Path
from urllib.parse import urlparse

import boto3
from tornado.web import RequestHandler, HTTPError

from dashboard.config import find_view_file, get_default_endpoint_name, overlay_enabled, schema_endpoint_url

ACCESS_KEY = os.getenv("ZF_S3_ACCESS_KEY")
SECRET_KEY = os.getenv("ZF_S3_SECRET_KEY")

# Externalize bucket/prefix from environment or defaults
BUCKET_NAME = os.getenv("TILE_BUCKET", "app-databuk-test-service")
PREFIX = os.getenv("TILE_PREFIX", "test_tiles/")
DEFAULT_EXPIRES_IN = 300
EXPIRY_BUFFER_SECONDS = 30


try:
    VIEWS_PATH = find_view_file()
except FileNotFoundError:
    VIEWS_PATH = None

DEFAULT_VIEW_NAME = (
    get_default_endpoint_name(VIEWS_PATH)
    if VIEWS_PATH is not None
    else None
)


def _overlay_source_from_view(view_name: str) -> tuple[str, str] | None:
    """Resolve (bucket, prefix) from the view's overlay source_uri.

    Returns ``None`` when the uri is missing, malformed, or not an ``s3://``
    URI. When present it takes full precedence over TILE_BUCKET/TILE_PREFIX.
    """
    if not view_name or VIEWS_PATH is None:
        return None
    if not VIEWS_PATH.exists():
        return None

    try:
        from dashboard.config import _parse_view_config
        config = _parse_view_config(VIEWS_PATH)
        view = config.get(view_name)
    except Exception:
        return None

    if not isinstance(view, dict):
        return None

    visualization = view.get("visualization", {})
    overlay = visualization.get("overlay", {}) if isinstance(visualization, dict) else {}
    uri = overlay.get("source_uri") if isinstance(overlay, dict) else None
    if not isinstance(uri, str) or not uri.strip():
        return None

    parsed = urlparse(uri.strip())
    if parsed.scheme != "s3" or not parsed.netloc:
        return None

    bucket = parsed.netloc
    prefix = parsed.path.strip("/")
    return bucket, (prefix + "/" if prefix else "")

def _cache_dir_from_view(view_name: str) -> str | None:
    if not view_name or VIEWS_PATH is None:
        return None

    views_path = VIEWS_PATH
    if not views_path.exists():
        return None

    try:
        from dashboard.config import _parse_view_config
        config = _parse_view_config(views_path)
    except Exception:
        return None

    view = config.get(view_name)
    if not isinstance(view, dict):
        return None

    visualization = view.get("visualization", {})
    overlay = visualization.get("overlay", {}) if isinstance(visualization, dict) else {}
    cache_dir = overlay.get("cache_dir") if isinstance(overlay, dict) else None

    # Backward compatibility for older view configs.
    if not isinstance(cache_dir, str) or not cache_dir.strip():
        tile_build = view.get("tile_build", {})
        if isinstance(tile_build, dict):
            cache_dir = tile_build.get("cache_dir")

    if not isinstance(cache_dir, str) or not cache_dir.strip():
        return None

    expanded = os.path.expandvars(os.path.expanduser(cache_dir.strip()))
    candidate = Path(expanded)
    if candidate.is_absolute():
        return str(candidate)

    # Relative paths are resolved against the project base dir (parent of config dir).
    base_dir = views_path.parent.parent
    return str(base_dir / candidate)

def _view_settings(view_name: str) -> tuple[str, str, Path, object] | None:
    if VIEWS_PATH is None or not view_name or not overlay_enabled(VIEWS_PATH, view_name):
        return None

    source = _overlay_source_from_view(view_name)
    bucket, prefix = source or (BUCKET_NAME, PREFIX)
    endpoint_url = schema_endpoint_url(VIEWS_PATH, view_name)
    cache_dir = Path(
        os.getenv("ZF_CACHE_DIR")
        or _cache_dir_from_view(view_name)
        or tempfile.gettempdir()
    )
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / f"{view_name}_tile_url_cache.json"
    client = boto3.client(
        "s3",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        endpoint_url=endpoint_url,
    )
    return bucket, prefix, cache_file, client


def load_cache(cache_file: Path) -> dict:
    if not cache_file.exists():
        return {}

    try:
        with cache_file.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return {}

    now = time.time()
    return {
        k: v for k, v in data.items()
        if v.get("expires_at", 0) > now
    }


def save_cache(cache_file: Path, data: dict) -> None:
    tmp = cache_file.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    tmp.replace(cache_file)


def tile_id(z: int, x: int, y: int) -> str:
    return f"{z}/{x}/{y}"


def tile_key(prefix: str, z: int, x: int, y: int) -> str:
    return f"{prefix}{z}/{x}/{y}.png"


def get_tile_url(z: int, x: int, y: int, view_name: str,
                 expires_in: int = DEFAULT_EXPIRES_IN) -> str:
    settings = _view_settings(view_name)
    if settings is None:
        raise HTTPError(404, "Overlay is disabled")

    bucket, prefix, cache_file, s3 = settings
    cache = load_cache(cache_file)

    tid = tile_id(z, x, y)
    now = time.time()

    item = cache.get(tid)
    if item and item.get("expires_at", 0) > now:
        return item["url"]

    key = tile_key(prefix, z, x, y)

    url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expires_in,
    )

    cache[tid] = {
        "url": url,
        "expires_at": now + expires_in - EXPIRY_BUFFER_SECONDS,
    }
    save_cache(cache_file, cache)
    return url


class S3TileHandler(RequestHandler):
    def get(self, z: str, x: str, y: str):
        try:
            z_i, x_i, y_i = int(z), int(x), int(y)
            view_name = self.get_argument("view", DEFAULT_VIEW_NAME)
            url = get_tile_url(z_i, x_i, y_i, view_name)
        except HTTPError:
            raise
        except Exception as e:
            raise HTTPError(404, f"Could not resolve tile {z}/{x}/{y}: {e}")

        self.redirect(url, permanent=False)
