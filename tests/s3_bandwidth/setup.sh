#!/usr/bin/env bash
set -xeuo pipefail

module load python

VENV_DIR="venv"

# Create venv if missing
if [[ ! -d "$VENV_DIR" ]]; then
  python3 -m venv "$VENV_DIR"
fi

# Activate venv
# shellcheck disable=SC1090
#source "$VENV_DIR/bin/activate"

PY="$VENV_DIR/bin/python3"
# Minimal deps for the S3 test script
$PY -m pip install --upgrade pip >/dev/null
$PY -m pip install boto3

