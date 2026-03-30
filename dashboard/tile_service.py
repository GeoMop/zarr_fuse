import os
import time
import json
from pathlib import Path

import boto3
from tornado.web import RequestHandler, HTTPError

ACCESS_KEY = os.getenv("ZF_S3_ACCESS_KEY")
SECRET_KEY = os.getenv("ZF_S3_SECRET_KEY")
ENDPOINT_URL = os.getenv("ZF_S3_ENDPOINT_URL")

BUCKET_NAME = "app-databuk-test-service"
PREFIX = "test_tiles/"
DEFAULT_EXPIRES_IN = 300
EXPIRY_BUFFER_SECONDS = 30

CACHE_FILE = Path(__file__).resolve().parent / "tile_url_cache.json"

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