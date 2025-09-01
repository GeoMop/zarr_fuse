#!/bin/bash

set -x

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Users secrets:
keys_file="${SCRIPT_DIR}/cesnet_s3_keys.json"
export AWS_ACCESS_KEY_ID=$(jq -r .key  ${keys_file})
export AWS_SECRET_ACCESS_KEY=$(jq -r .secret  ${keys_file})

cmd=$1
shift

contains() {
  local val="$1"
  shift
  for x; do
    [[ "$x" == "$val" ]] && return 0
  done
  return 1
}

high_cmds=(cp mv sync ls rb mb rm)

# Check if user wants high-level command
if contains "$cmd" "${high_cmds[@]}"; then
  cli="s3"
else
  cli="s3api"
fi

aws $cli $cmd --endpoint-url https://s3.cl4.du.cesnet.cz $@

# Examples:
# 
