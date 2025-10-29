#!/usr/bin/env bash
set -euo pipefail

prompt() { read -rp "$1: " REPLY && echo "$REPLY"; }

PROJECT_ID=${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || true)}
if [[ -z "${PROJECT_ID}" ]]; then PROJECT_ID=$(prompt "Project ID"); fi
REGION=${REGION:-$(prompt "Region (e.g. europe-west1)")}
SERVICE=${SERVICE:-$(prompt "Service name (e.g. shoptet-bq-multi)")}
JOB=${JOB:-$(prompt "Scheduler job name (e.g. shoptet-bq-daily)")}
CRON=${CRON:-$(prompt "Cron (UTC), e.g. '0 6 * * *' for 06:00 daily")}
PIPELINE_ID=${PIPELINE_ID:-$(prompt "Optional pipeline id (leave blank for ALL)")}

SERVICE_URL=$(gcloud run services describe "${SERVICE}" --region "${REGION}" --format='value(status.url)')

SA_NAME="shoptet-bq-invoker"
SA="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

echo "Creating/ensuring invoker service account ${SA} ..."
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

echo "Done. Job '${JOB}' will call: ${URI}"
