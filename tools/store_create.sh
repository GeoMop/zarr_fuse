#!/bin/bash

# Usage:
# bash ./store_create.sh [-f] <BUCKET_NAME>
#
# BUCKET_NAME without leading s3://
# !! name can not contain underscores, but may contain dashes.
#
# 1. delete the bucket if '-f' is present
# 2. try to create the bucket 
# 3. set the policy
# 4. list the bucket policy
# 5. try write, read, delete operations

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

AWS="$SCRIPT_DIR/aws.sh"

#TENANT="6505d0ea_cc40_4dea_bf99_0c5b3f7eb526"
# extract tenant from list-buckets
TENANT=`$AWS personal s3api list-buckets | jq -r '.Owner.ID | split("$")[0]'`

FORCE=0
# Parse options manually with shift
while [ "$#" -gt 0 ]; do
  case "$1" in
    -f)
      FORCE=1
      shift
      ;;
    --) # explicit end of options
      shift
      break
      ;;
    *)  # first non-option => BUCKET
      break
      ;;
  esac
done

BUCKET="$1"
shift

########################################
# Handle bucket creation
########################################
if [[ $FORCE -eq 1 ]]; then
  echo "Forcing removal of existing bucket $BUCKET ..."
  # remove all objects and bucket
  "$AWS" service s3 rb "s3://$BUCKET" --force || true
fi

echo "Creating bucket $BUCKET ..."
if ! "$AWS" service s3api create-bucket --bucket "$BUCKET"; then
  echo "Bucket $BUCKET may already exist."
  # check if it's owned by us
  if "$AWS" service s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
    echo "Bucket $BUCKET already exists and is accessible. Continuing..."
  else
    echo "Bucket $BUCKET exists but not accessible!"
    exit 1
  fi
fi

########################################
# Create temporary files
########################################
policy_file="$(mktemp /tmp/s3-policy.XXXXXX.json)"
tmp_upload="$(mktemp /tmp/s3-upload.XXXXXX.txt)"
tmp_download="$(mktemp /tmp/s3-download.XXXXXX.txt)"
trap 'rm -f "$policy_file" "$tmp_upload" "$tmp_download"' EXIT

########################################
# Write and apply policy
########################################
cat > "$policy_file" <<END
{
  "Statement": [
    {
      "Sid": "HLAVO surface bucket-tenant-rw policy",
      "Effect": "Allow",
      "Principal": { "AWS": ["$TENANT"] },
      "Action": [
        "s3:ListBucket",
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::$BUCKET",
        "arn:aws:s3:::$BUCKET/*"
      ]
    }
  ]
}
END

"$AWS" service s3api put-bucket-policy --bucket "$BUCKET" --policy "file://$policy_file"

echo "Current bucket policy:"
"$AWS" service s3api get-bucket-policy --bucket "$BUCKET" --output json

########################################
# Test with personal profile - high-level s3
########################################
echo "Testing with personal profile (s3)..."
echo "hello from $(hostname) at $(date -Iseconds)" > "$tmp_upload"
key1="tests/cli-$(date +%s)-$RANDOM.txt"

"$AWS" personal s3 cp "$tmp_upload" "s3://$BUCKET/$key1"
"$AWS" personal s3 ls "s3://$BUCKET/$key1"
"$AWS" personal s3 cp "s3://$BUCKET/$key1" "$tmp_download"
diff -u "$tmp_upload" "$tmp_download"
"$AWS" personal s3 rm "s3://$BUCKET/$key1"
