#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."  # repo root

# shellcheck disable=SC1091
. scripts/common.sh

load_state || true

PROJECT_ID=${PROJECT_ID:-$(active_project)}
REGION=$(prompt_default "Region" "${REGION:-europe-west1}")
SERVICE=$(prompt_default "Service name" "${SERVICE:-${SERVICE:-shoptet-bq-multi}}")

SERVICE_URL=$(gcloud run services describe "${SERVICE}" --region "${REGION}" --format='value(status.url)')
ID_TOKEN=$(gcloud auth print-identity-token)

echo "Triggering: ${SERVICE_URL}/run"
curl -s -H "Authorization: Bearer ${ID_TOKEN}" "${SERVICE_URL}/run" | jq