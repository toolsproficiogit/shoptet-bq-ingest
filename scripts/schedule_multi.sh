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
SERVICE=$(prompt_default "Service name" "${SERVICE:-shoptet-bq-multi}")
JOB=$(prompt_default "Scheduler job name" "daily-shoptet-bq")
CRON=$(prompt_default "Cron (UTC)" "0 6 * * *")
PIPELINE_ID=$(prompt_default "Optional pipeline id (leave blank for ALL)" "")

SERVICE_URL=$(gcloud run services describe "${SERVICE}" --region "${REGION}" --format='value(status.url)')
SA_NAME="shoptet-bq-invoker"
SA="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

echo "Ensuring invoker service account ${SA} ..."
gcloud iam service-accounts create "${SA_NAME}" --display-name "Shoptet BQ Invoker" 2>/dev/null || true
gcloud run services add-iam-policy-binding "${SERVICE}" \
  --region "${REGION}" \
  --member "serviceAccount:${SA}" \
  --role roles/run.invoker \
  --condition=None

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

echo
echo "✅ Scheduled ${JOB}"
echo "Calls: ${URI}"