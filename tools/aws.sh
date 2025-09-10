#!/bin/bash

#set -x

# aws command wraper for CESNET s3 storage
# Usage:
# ./aws.sh <profile> [s3|s3api|...] ...
#
# - it reads and setup the "service" and "personal" profiles according to the file 'cesnet_s3_keys.json' with secrets:
#    {
#      "personal": {"key":"...","secret":"..."},
#      "service": {"key":"...", "secret": "..."}
#    }
# - special env variables are set to deal with CESNET/CEPH compatibility issues
# - it automaticaly set --enpoint-url parameter
# 



SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Users secrets:
keys_file="${SCRIPT_DIR}/cesnet_s3_keys.json"

# SERVICE profile
aws configure set profile.service.aws_access_key_id $(jq -r .service.key  ${keys_file})
aws configure set profile.service.aws_secret_access_key $(jq -r .service.secret  ${keys_file})
aws configure set profile.service.aws_secret_access_key $(jq -r .service.secret  ${keys_file})

#aws configure set profile.service.region                us-east-1
#aws configure set profile.service.s3.addressing_style   path
#aws configure set profile.service.s3.request_checksum_calculation   when_required
#aws configure set profile.service.s3.response_checksum_validation   when_required
#aws configure set profile.service.s3.payload_signing false

# PERSONAL profile (bucket owner)
aws configure set profile.personal.aws_access_key_id $(jq -r .personal.key  ${keys_file})
aws configure set profile.personal.aws_secret_access_key $(jq -r .personal.secret  ${keys_file})
#aws configure set profile.personal.s3.addressing_style   path


export AWS_REQUEST_CHECKSUM_CALCULATION=when_required
export AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
export AWS_NO_CHUNKED_ENCODING=true

end_pt="--endpoint-url https://s3.cl4.du.cesnet.cz"
# cmd=$1
# shift
# 
# high_cmd() {
#   local val="$1"
#   shift
#   high_cmds=(cp mv sync ls rb mb rm)
# 
#   for item in "${high_cmds[@]}"; do
#     [[ "$item" == "$val" ]] && return 0
#   done
#   return 1
# }
# 
# 
# # Check if user wants high-level command
# if high_cmd "$cmd" ; then
#   cli="s3"
# elif [[ "$cmd" == "sts" ]] ; then
#   cli= 
# elif [[ "$cmd" == "write" ]] ; then
#   path=$1
#   shift
#   echo "$@" | \
#   aws $end_pt s3 cp - "s3://$path" \
#   --acl bucket-owner-full-control
#   exit 0
# else
#   cli="s3api"
# fi
profile=$1
shift
aws --profile $profile $end_pt $@

# Examples:
# 
