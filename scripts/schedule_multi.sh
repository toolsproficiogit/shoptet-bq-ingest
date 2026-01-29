#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."  # repo root

# shellcheck disable=SC1091
. scripts/common.sh

echo "== Schedule Multi-pipeline Service =="

load_state || true

PROJECT_ID=${PROJECT_ID:-$(active_project)}
PROJECT_ID=$(prompt_default "Project ID" "${PROJECT_ID}")
REGION=$(prompt_default "Region" "${REGION:-europe-west1}")

# Support service name as first argument
if [ $# -gt 0 ]; then
    SERVICE="$1"
else
    SERVICE=$(prompt_default "Service name" "${SERVICE:-csv-bq-multi}")
fi
# Use service name in job name for multi-service setup
DEFAULT_JOB_NAME="${SERVICE}-schedule"
JOB=$(prompt_default "Scheduler job name" "${JOB:-$DEFAULT_JOB_NAME}")
CRON=$(prompt_default "Cron (UTC)" "${CRON:-0 6 * * *}")
PIPELINE_ID=$(prompt_default "Optional pipeline id (leave blank for ALL)" "")

# Get service URL
echo "Fetching service URL for ${SERVICE}..."
SERVICE_URL=$(gcloud run services describe "${SERVICE}" --region "${REGION}" --format='value(status.url)' 2>/dev/null) || {
    echo "Error: Service ${SERVICE} not found in region ${REGION}"
    exit 1
}

SA_NAME="csv-bq-invoker"
SA="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

echo "Ensuring invoker service account ${SA}..."
gcloud iam service-accounts create "${SA_NAME}" --display-name "CSV BQ Invoker" 2>/dev/null || true

echo "Granting invoker role to ${SA}..."
gcloud run services add-iam-policy-binding "${SERVICE}" \
  --region "${REGION}" \
  --member "serviceAccount:${SA}" \
  --role roles/run.invoker \
  --condition=None 2>/dev/null || true

URI="${SERVICE_URL}/run"
if [[ -n "${PIPELINE_ID}" ]]; then
  URI="${URI}?pipeline=${PIPELINE_ID}"
fi

echo "Creating/updating Cloud Scheduler job ${JOB} ..."
gcloud scheduler jobs delete "${JOB}" --location "${REGION}" -q >/dev/null 2>&1 || true
gcloud scheduler jobs create http "${JOB}" \
  --location "${REGION}" \
  --schedule "${CRON}" \
  --http-method GET \
  --uri "${URI}" \
  --oidc-service-account-email "${SA}"

echo ""
echo "✅ Scheduled ${JOB} for service ${SERVICE}"
echo "Calls: ${URI}"
echo ""
echo "Multi-service note:"
echo "  To schedule multiple services, run:"
echo "  ./scripts/schedule_multi.sh csv-bq-multi-0"
echo "  ./scripts/schedule_multi.sh csv-bq-multi-1"
echo "  ./scripts/schedule_multi.sh csv-bq-multi-2"