#!/bin/sh

# The script is used to install extra dependencies for ingress server extractirs and is used
# in the OCI (Dockerfile) to improve readability and maintainability of the Dockerfile.

set -eu
set -x

ROOT="${1:-.}"

if [ -f "${ROOT}/inputs/requirements.txt" ]; then
  pip install --no-cache-dir -r "${ROOT}/inputs/requirements.txt"
fi

if [ -f "${ROOT}/inputs/pyproject.toml" ]; then
  pip install --no-cache-dir "${ROOT}/inputs"
fi
