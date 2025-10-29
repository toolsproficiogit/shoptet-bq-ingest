#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/helpers.sh"
need gcloud


read -rp "Project ID: " PROJECT_ID
read -rp "Region (e.g. europe-central2): " REGION
read -rp "Service name [shoptet-bq-ingest]: " SERVICE; SERVICE=${SERVICE:-shoptet-bq-ingest}
read -rp "CSV URL: " CSV_URL
read -rp "BigQuery table (project.dataset.table): " BQ_TABLE_ID
read -rp "Window days [30]: " WINDOW_DAYS; WINDOW_DAYS=${WINDOW_DAYS:-30}
read -rp "Load mode [auto]: " LOAD_MODE; LOAD_MODE=${LOAD_MODE:-auto}
read -rp "BQ Location [EU]: " BQ_LOCATION; BQ_LOCATION=${BQ_LOCATION:-EU}


REPO=containers
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${SERVICE}:v1"


gcloud config set project "$PROJECT_ID" >/dev/null
ensure_apis
ensure_repo "$PROJECT_ID" "$REGION" "$REPO"


gcloud builds submit --tag "$IMAGE"


gcloud run deploy "$SERVICE" \
--image "$IMAGE" \
--platform managed \
--region "$REGION" \
--no-allow-unauthenticated \
--set-env-vars CSV_URL="$CSV_URL",BQ_TABLE_ID="$BQ_TABLE_ID",WINDOW_DAYS="$WINDOW_DAYS",LOAD_MODE="$LOAD_MODE",BQ_LOCATION="$BQ_LOCATION"


URL=$(get_service_url "$SERVICE" "$REGION")
ID_TOKEN=$(gcloud auth print-identity-token)
echo "Test with: curl -H 'Authorization: Bearer $ID_TOKEN' ${URL}/run | jq"
