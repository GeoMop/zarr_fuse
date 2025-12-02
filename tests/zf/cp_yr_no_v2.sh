#!/usr/bin/env bash
# more portable, respects the user’s PATH

# Exit immediately on errors (-e),
# treat unset variables as errors (-u),
# and make pipelines fail if any command in the chain fails (-o pipefail).
set -euo pipefail

# Simple runner: echo the command, run it, and fail on error because of `set -e`
run() {
  echo
  echo ">>> $*"
  "$@"
}

# Test producing help.
run zf --help

# Test copy from v02 zarr-fuse to v03 zarr-fuse
run zf cp \
  --src.STORE_URL=zip://source_v2.zip \
  --dst.STORE_URL=s3://bucket/dest.zarr \
  None \
  dst_schema.yaml