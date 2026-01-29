#!/usr/bin/env bash
set -euo pipefail

REGION=${REGION:-europe-west1}
SERVICE=${SERVICE:-csv-bq-multi}
EXTRA_QS=""

# Parse arguments
if [ $# -gt 0 ] && [[ "$1" != "?"* ]]; then
    # First argument is service name (if it doesn't start with ?)
    SERVICE="$1"
    EXTRA_QS="${2:-}"
else
    # First argument is query string
    EXTRA_QS="${1:-}"
fi

echo "Service: $SERVICE" >&2
echo "Region: $REGION" >&2

SERVICE_URL=$(gcloud run services describe "$SERVICE" --region "$REGION" --format='value(status.url)')
ID_TOKEN=$(gcloud auth print-identity-token)

# Print info to stderr so jq only sees JSON
echo "Triggering: ${SERVICE_URL}/run${EXTRA_QS}" >&2

RESP=$(curl -s -H "Authorization: Bearer $ID_TOKEN" "${SERVICE_URL}/run${EXTRA_QS}")

# If response is JSON, pretty print; otherwise show raw
if echo "$RESP" | jq -e . >/dev/null 2>&1; then
  echo "$RESP" | jq
else
  echo "$RESP"
  echo "⚠️ Response was not valid JSON (see above)." >&2
fi