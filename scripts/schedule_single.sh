#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/helpers.sh"
need gcloud


read -rp "Project ID: " PROJECT_ID
read -rp "Region (e.g. europe-central2): " REGION
read -rp "Service name [shoptet-bq-ingest]: " SERVICE; SERVICE=${SERVICE:-shoptet-bq-ingest}
read -rp "Job name [shoptet-bq-daily]: " JOB; JOB=${JOB:-shoptet-bq-daily}
read -rp "Cron (e.g. '5 * * * *' hourly, '0 6 * * *' daily 06:00 UTC): " CRON


URL=$(get_service_url "$SERVICE" "$REGION")
SA=shoptet-bq-invoker
SA_EMAIL="$SA@${PROJECT_ID}.iam.gserviceaccount.com"


gcloud iam service-accounts create "$SA" --display-name "Shoptet BQ Invoker" || true


gcloud run services add-iam-policy-binding "$SERVICE" \
--region "$REGION" \
--member "serviceAccount:${SA_EMAIL}" \
--role roles/run.invoker


gcloud scheduler jobs create http "$JOB" \
--location "$REGION" \
--schedule "$CRON" \
--http-method GET \
--uri "${URL}/run" \
--oidc-service-account-email "$SA_EMAIL"


echo "Created Scheduler job $JOB -> $CRON"