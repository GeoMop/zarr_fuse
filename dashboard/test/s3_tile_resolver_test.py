import json
import os
import time
import boto3

access_key = ""
secret_key = ""
endpoint_url = ""

bucket_name = "app-databuk-test-service"
prefix = "test_tiles/"
cache_file = "tile_url_cache.json"

s3 = boto3.client(
    "s3",
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    endpoint_url=endpoint_url,
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
    return f"{prefix}{z}/{x}/{y}.png"

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