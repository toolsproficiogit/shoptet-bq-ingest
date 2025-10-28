#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/helpers.sh"
need gcloud


read -rp "Project ID: " PROJECT_ID
read -rp "Region (e.g. europe-central2): " REGION
read -rp "Service name [shoptet-bq-multi]: " SERVICE; SERVICE=${SERVICE:-shoptet-bq-multi}
read -rp "Job name [shoptet-bq-multi-daily]: " JOB; JOB=${JOB:-shoptet-bq-multi-daily}
read -rp "Cron (e.g. '0 5 * * *'): " CRON
read -rp "(Optional) Specific pipeline id to run (blank for all): " PID


URL=$(get_service_url "$SERVICE" "$REGION")
[[ -n "$PID" ]] && URL="${URL}/run?pipeline=${PID}" || URL="${URL}/run"
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
--uri "$URL" \
--oidc-service-account-email "$SA_EMAIL"


echo "Created Scheduler job $JOB -> $CRON (URL: $URL)"