import json
import os
import time
from pathlib import Path
from urllib.parse import urlparse

import boto3
import requests
from flask import Flask, redirect, abort, Response

# -------------------------
# Config
# -------------------------
ACCESS_KEY = ""
SECRET_KEY = ""
ENDPOINT_URL = ""    

BUCKET_NAME = "app-databuk-test-service"
PREFIX = "test_tiles/"
CACHE_FILE = Path(__file__).resolve().parent / "tile_url_cache.json"

HOST = "127.0.0.1"
PORT = 8000

# choose one:
# "redirect" = browser gets redirected to signed S3 URL
# "proxy"    = this server downloads tile from S3 and returns image bytes
MODE = "redirect"

DEFAULT_EXPIRES_IN = 300
EXPIRY_BUFFER_SECONDS = 30

# -------------------------
# App + S3 client
# -------------------------
app = Flask(__name__)

s3 = boto3.client(
    "s3",
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    endpoint_url=ENDPOINT_URL,
)

cache: dict[str, dict] = {}


# -------------------------
# Cache helpers
# -------------------------
def load_cache() -> dict:
    if CACHE_FILE.exists():
        with CACHE_FILE.open("r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_cache(data: dict) -> None:
    tmp = CACHE_FILE.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    tmp.replace(CACHE_FILE)


# -------------------------
# Tile helpers
# -------------------------
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


# -------------------------
# Routes
# -------------------------
@app.get("/<int:z>/<int:x>/<int:y>.png")
def serve_tile(z: int, x: int, y: int):
    try:
        url = get_tile_url(z, x, y)
    except Exception as e:
        abort(404, f"Could not resolve tile {z}/{x}/{y}: {e}")

    if MODE == "redirect":
        return redirect(url, code=307)

    if MODE == "proxy":
        try:
            r = requests.get(url, timeout=20)
            if r.status_code != 200:
                abort(r.status_code, f"S3 returned {r.status_code} for tile {z}/{x}/{y}")
            return Response(r.content, content_type="image/png")
        except Exception as e:
            abort(502, f"Could not fetch tile from S3: {e}")

    abort(500, f"Invalid MODE: {MODE}")


@app.get("/health")
def health():
    return {"ok": True, "mode": MODE, "cache_entries": len(cache)}


if __name__ == "__main__":
    cache = load_cache()
    print(f"Loaded {len(cache)} cached tile URLs")
    print(f"Serving on http://{HOST}:{PORT}")
    app.run(host=HOST, port=PORT, debug=False)