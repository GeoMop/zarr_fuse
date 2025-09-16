#!/bin/bash
set -euo pipefail
set -x

# where this script lives
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# paths
KEYS_FILE="$SCRIPT_DIR/cesnet_s3_keys.json"
CFG_FILE="$SCRIPT_DIR/rclone.cfg"

# ensure jq is installed
if ! command -v jq >/dev/null 2>&1; then
  echo "ERROR: 'jq' is required but not installed. Please install jq and try again." >&2
  exit 1
fi

# ensure credentials file exists
if [ ! -f "$KEYS_FILE" ]; then
  echo "ERROR: Credentials file not found at $KEYS_FILE" >&2
  exit 1
fi

# extract key & secret
ACCESS_KEY=$(jq -r '.key'    < "$KEYS_FILE")
SECRET_KEY=$(jq -r '.secret' < "$KEYS_FILE")

# write rclone config
cat > "$CFG_FILE" <<EOF
[s3]
type = s3
provider = Other
access_key_id = $ACCESS_KEY
secret_access_key = $SECRET_KEY
endpoint = https://s3.cl4.du.cesnet.cz
region = du
EOF

# cleanup function to stop container on Ctrl-C
cleanup() {
  echo
  echo "Stopping rclone container $CONTAINER_ID..."
  docker stop "$CONTAINER_ID" >/dev/null
  echo "Container stopped."
  rm $CFG_FILE
  exit 0
}

# bind cleanup to SIGINT
trap cleanup SIGINT

# start the dockerized rclone daemon
CONTAINER_ID=$(docker run -d \
  -p 5572:5572 \
  -v "$CFG_FILE":/config/rclone/rclone.conf \
  rclone/rclone:latest \
    rcd \
      --rc-web-gui \
      --rc-addr :5572 \
      --rc-no-auth
      )

echo "Rclone container started (ID: $CONTAINER_ID). Waiting for web GUI on port 5572..."

# wait until the web GUI responds (give it up to 30 seconds)
MAX_WAIT=30
SECONDS_PASSED=0
until curl -s http://localhost:5572/ >/dev/null 2>&1; do
  if [ "$SECONDS_PASSED" -ge "$MAX_WAIT" ]; then
    echo "ERROR: rclone web GUI did not start within $MAX_WAIT seconds." >&2
    cleanup
  fi
  sleep 1
  SECONDS_PASSED=$((SECONDS_PASSED + 1))
done

echo "Web GUI is up! Launching browser..."

# open default browser (Linux/Mac/Windows)
URL="http://localhost:5572/"
if command -v xdg-open >/dev/null 2>&1; then
  xdg-open "$URL"
elif command -v open >/dev/null 2>&1; then
  open "$URL"
elif command -v start >/dev/null 2>&1; then
  start "" "$URL"
else
  echo "Please open your browser and go to $URL"
fi

echo "Press Ctrl-C to stop the rclone container and exit."

# wait indefinitely until SIGINT
while true; do
  sleep 1
done
