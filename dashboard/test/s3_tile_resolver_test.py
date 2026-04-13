# Should be tested and fully integrated to the dashboard, but this is the general structure for the tile building configuration. The actual implementation of tile building and serving would require additional code to generate tiles from the source image and serve them via a web server or similar mechanism.
import json
import os
import time
from pathlib import Path

import boto3

from dashboard.config.dashboard_config import get_endpoint_config


CONFIG_ROOT = Path(__file__).resolve().parent.parent
ENDPOINTS_PATH = CONFIG_ROOT / "config" / "endpoints.yaml"


def _load_tile_runtime_config():
    endpoint_name = os.getenv("HV_DASHBOARD_ENDPOINT")
    if not endpoint_name:
        raise ValueError("HV_DASHBOARD_ENDPOINT is required")

    endpoint = get_endpoint_config(ENDPOINTS_PATH, endpoint_name)

    overlay_config = endpoint.visualization.overlay
    if not overlay_config.enabled:
        raise ValueError(f"Overlay is disabled for endpoint '{endpoint_name}'")

    bucket_name = os.getenv("S3_TILE_BUCKET_NAME")
    prefix = os.getenv("S3_TILE_PREFIX", "")
    cache_file = os.getenv(
        "S3_TILE_CACHE_FILE",
        str(Path(__file__).with_name(f"{endpoint_name}_tile_url_cache.json")),
    )

    access_key = os.getenv("S3_ACCESS_KEY", "")
    secret_key = os.getenv("S3_SECRET_KEY", "")
    endpoint_url = os.getenv("S3_ENDPOINT_URL", "")

    if not bucket_name:
        raise ValueError("S3_TILE_BUCKET_NAME is required")
    if not access_key:
        raise ValueError("S3_ACCESS_KEY is required")
    if not secret_key:
        raise ValueError("S3_SECRET_KEY is required")
    if not endpoint_url:
        raise ValueError("S3_ENDPOINT_URL is required")

    return {
        "bucket_name": bucket_name,
        "prefix": prefix.strip("/"),
        "cache_file": cache_file,
        "access_key": access_key,
        "secret_key": secret_key,
        "endpoint_url": endpoint_url,
    }


RUNTIME_CONFIG = _load_tile_runtime_config()

bucket_name = RUNTIME_CONFIG["bucket_name"]
prefix = RUNTIME_CONFIG["prefix"]
cache_file = RUNTIME_CONFIG["cache_file"]

s3 = boto3.client(
    "s3",
    aws_access_key_id=RUNTIME_CONFIG["access_key"],
    aws_secret_access_key=RUNTIME_CONFIG["secret_key"],
    endpoint_url=RUNTIME_CONFIG["endpoint_url"],
)


def load_cache() -> dict:
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_cache(cache: dict) -> None:
    tmp_file = cache_file + ".tmp"
    with open(tmp_file, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)
    os.replace(tmp_file, cache_file)


def tile_key(z: int, x: int, y: int) -> str:
    if prefix:
        return f"{prefix}/{z}/{x}/{y}.png"
    return f"{z}/{x}/{y}.png"


def tile_id(z: int, x: int, y: int) -> str:
    return f"{z}/{x}/{y}"


cache = load_cache()


def get_tile_url(z: int, x: int, y: int, expires_in: int = 30) -> str:
    tid = tile_id(z, x, y)
    now = time.time()

    item = cache.get(tid)
    if item:
        print(f"now        : {now}")
        print(f"expires_at : {item['expires_at']}")

    if item and item["expires_at"] > now:
        print(f"cache hit for {tid}")
        return item["url"]

    key = tile_key(z, x, y)

    url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket_name, "Key": key},
        ExpiresIn=expires_in,
    )

    cache[tid] = {
        "url": url,
        "expires_at": now + expires_in - 5,
    }
    save_cache(cache)

    print(f"generated new URL for {tid}")
    return url


print(get_tile_url(19, 285754, 179009))