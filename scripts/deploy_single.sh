#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."  # repo root

# shellcheck disable=SC1091
. scripts/common.sh

echo "== Shoptet → BigQuery: Single-pipeline Deploy =="

PROJECT_ID=${PROJECT_ID:-$(active_project)}
PROJECT_ID=$(prompt_default "Project ID" "${PROJECT_ID}")
REGION=$(prompt_default "Region" "europe-west1")
SERVICE=$(prompt_default "Service name" "shoptet-bq-ingest")
BQ_LOCATION=$(prompt_default "BigQuery location" "EU")
DATASET=$(prompt_default "BigQuery dataset (must exist)" "shoptet_export")
TABLE_NAME=$(prompt_default "BigQuery table name" "orders")
CSV_URL=$(prompt_default "CSV URL" "")
WINDOW_DAYS=$(prompt_default "Window days" "30")
LOAD_MODE=$(prompt_default "Load mode (auto/full/window)" "auto")

if [[ -z "$CSV_URL" ]]; then echo "❌ CSV URL is required."; exit 1; fi
BQ_TABLE_ID="${PROJECT_ID}.${DATASET}.${TABLE_NAME}"

REPO=${REPO:-containers}
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${SERVICE}:v1"

echo "Enabling required APIs (idempotent)..."
gcloud services enable run.googleapis.com artifactregistry.googleapis.com cloudbuild.googleapis.com iam.googleapis.com

echo "Ensuring Artifact Registry repo '${REPO}' exists in ${REGION}..."
gcloud artifacts repositories describe "${REPO}" --location "${REGION}" >/dev/null 2>&1 || \
gcloud artifacts repositories create "${REPO}" --repository-format=docker --location "${REGION}" --description="Containers"

echo "Building image ${IMAGE} ..."
gcloud builds submit --tag "${IMAGE}"

echo "Deploying Cloud Run service ${SERVICE} ..."
gcloud run deploy "${SERVICE}" \
  --image "${IMAGE}" \
  --region "${REGION}" \
  --platform managed \
  --no-allow-unauthenticated \
  --set-env-vars CSV_URL="${CSV_URL}",BQ_TABLE_ID="${BQ_TABLE_ID}",WINDOW_DAYS="${WINDOW_DAYS}",LOAD_MODE="${LOAD_MODE}",BQ_LOCATION="${BQ_LOCATION}"

SERVICE_URL=$(gcloud run services describe "${SERVICE}" --region "${REGION}" --format='value(status.url)')

save_state "$PROJECT_ID" "$REGION" "$SERVICE" "$SERVICE_URL"

echo
echo "✅ Deployed"
echo "Project:       $PROJECT_ID"
echo "Region:        $REGION"
echo "Service:       $SERVICE"
echo "Service URL:   $SERVICE_URL"
echo
echo "Tip: first manual run:"
echo "  ID_TOKEN=\$(gcloud auth print-identity-token)"
echo "  curl -s -H \"Authorization: Bearer \$ID_TOKEN\" \"$SERVICE_URL/run\" | jq"