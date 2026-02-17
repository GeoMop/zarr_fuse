#!/usr/bin/env python3
import json
import math
import sys
import time
import uuid

import boto3
from botocore.config import Config

# === EDIT THESE TWO ===
S3_URL = "https://s3.cl4.du.cesnet.cz"   # e.g. "https://s3.cld.cesnet.cz"
BUCKET = "test-zarr-storage"
# ======================

GiB = 1024 ** 3
MiB = 1024 ** 2

SIZE_BYTES = 1 * GiB
PART_SIZE = 64 * MiB   # must be >= 5 MiB for multipart
DL_CHUNK = 8 * MiB


def die(msg: str, code: int = 2) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    raise SystemExit(code)


def human_speed(bytes_per_sec: float) -> str:
    mib_s = bytes_per_sec / MiB
    gbit_s = (bytes_per_sec * 8) / 1e9
    return f"{mib_s:.1f} MiB/s ({gbit_s:.2f} Gbit/s)"


def make_client(creds: dict):
    ak = creds.get("access_key")
    sk = creds.get("secret_key")
    st = creds.get("session_token")

    if not ak or not sk:
        die('Secrets JSON must include "access_key" and "secret_key".')

    # CESNET / S3-compatible compatibility knobs:
    # - region_name is used for SigV4 signing only
    # - checksum settings avoid AWS-style streaming checksums/trailers that some endpoints reject
    cfg = Config(
        region_name="us-east-1",
        s3={"addressing_style": "path"},
        retries={"max_attempts": 5, "mode": "standard"},
        request_checksum_calculation="when_required",
        response_checksum_validation="when_required",
    )

    return boto3.client(
        "s3",
        endpoint_url=S3_URL,
        aws_access_key_id=ak,
        aws_secret_access_key=sk,
        aws_session_token=st,
        config=cfg,
    )


def multipart_upload(s3, bucket: str, key: str) -> float:
    resp = s3.create_multipart_upload(Bucket=bucket, Key=key)
    upload_id = resp["UploadId"]

    buf = b"\0" * PART_SIZE
    num_parts = math.ceil(SIZE_BYTES / PART_SIZE)

    parts = []
    sent = 0
    start = time.perf_counter()

    try:
        for part_number in range(1, num_parts + 1):
            remaining = SIZE_BYTES - sent
            this_size = PART_SIZE if remaining >= PART_SIZE else remaining
            body = buf if this_size == PART_SIZE else (b"\0" * this_size)

            up = s3.upload_part(
                Bucket=bucket,
                Key=key,
                PartNumber=part_number,
                UploadId=upload_id,
                Body=body,
                ContentLength=this_size,  # important for some S3-compatible servers (avoids MissingContentLength)
            )
            parts.append({"PartNumber": part_number, "ETag": up["ETag"]})
            sent += this_size

        s3.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
    except Exception:
        try:
            s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        except Exception:
            pass
        raise

    return time.perf_counter() - start


def streaming_download(s3, bucket: str, key: str) -> float:
    start = time.perf_counter()
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"]

    while True:
        chunk = body.read(DL_CHUNK)
        if not chunk:
            break

    body.close()
    return time.perf_counter() - start


def main():
    if len(sys.argv) != 2:
        die("Usage: python3 test_s3_bandwidth.py /path/to/secrets.json")

    # Read secrets from JSON file (single positional param = file path)
    with open(sys.argv[1], "r", encoding="utf-8") as f:
        raw = f.read()

    creds = json.loads(raw)
    if not isinstance(creds, dict):
        die("Secrets JSON must be an object/dict.")

    if not S3_URL.startswith("http"):
        die("Set S3_URL in the script (must start with http/https).")
    if not BUCKET:
        die("Set BUCKET in the script.")

    s3 = make_client(creds)
    key = f"bwtest/{uuid.uuid4().hex}.bin"

    print(f"Endpoint: {S3_URL}")
    print(f"Bucket:   {BUCKET}")
    print(f"Key:      {key}")
    print(f"Size:     {SIZE_BYTES} bytes (1 GiB)")
    print()

    # Upload
    t_up = multipart_upload(s3, BUCKET, key)
    up_bps = SIZE_BYTES / t_up
    print(f"UPLOAD:   {t_up:.3f} s  -> {human_speed(up_bps)}")

    # Download
    t_dn = streaming_download(s3, BUCKET, key)
    dn_bps = SIZE_BYTES / t_dn
    print(f"DOWNLOAD: {t_dn:.3f} s  -> {human_speed(dn_bps)}")

    # Cleanup
    try:
        s3.delete_object(Bucket=BUCKET, Key=key)
    except Exception:
        pass


if __name__ == "__main__":
    main()
