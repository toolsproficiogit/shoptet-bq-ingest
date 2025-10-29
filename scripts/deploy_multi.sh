#!/usr/bin/env bash
set -euo pipefail

# Remote-only multi-pipeline deploy:
# - Uploads YAML to GCS and sets CONFIG_URL
# - Deploys Cloud Run with MULTI_MODE=true

prompt() { read -rp "$1: " REPLY && echo "$REPLY"; }

PROJECT_ID=${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || true)}
if [[ -z "${PROJECT_ID}" ]]; then PROJECT_ID=$(prompt "Project ID"); fi
REGION=${REGION:-$(prompt "Region (e.g. europe-west1)")}
SERVICE=${SERVICE:-$(prompt "Service name (e.g. shoptet-bq-multi)")}
LOCAL_YAML=${LOCAL_YAML:-$(prompt "Local YAML path (e.g. config/config.yaml)")}
BQ_LOCATION=${BQ_LOCATION:-$(prompt "BigQuery location (e.g. EU)")}
BUCKET=${BUCKET:-$(prompt "GCS bucket for config (e.g. my-shoptet-configs)")}
OBJECT=${OBJECT:-$(prompt "GCS object name [shoptet_config.yaml]")}
OBJECT=${OBJECT:-shoptet_config.yaml}

REPO=${REPO:-containers}
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${SERVICE}:v1"

echo "Enabling required APIs (idempotent)..."
gcloud services enable run.googleapis.com artifactregistry.googleapis.com cloudbuild.googleapis.com iam.googleapis.com

echo "Ensuring Artifact Registry repo '${REPO}' exists in ${REGION}..."
gcloud artifacts repositories describe "${REPO}" --location "${REGION}" >/dev/null 2>&1 || \
gcloud artifacts repositories create "${REPO}" --repository-format=docker --location "${REGION}" --description="Containers"

echo "Uploading YAML to gs://${BUCKET}/${OBJECT} ..."
gsutil mb -p "${PROJECT_ID}" -l "${REGION}" "gs://${BUCKET}" 2>/dev/null || true
gsutil cp "${LOCAL_YAML}" "gs://${BUCKET}/${OBJECT}"

CONFIG_URL="https://storage.googleapis.com/${BUCKET}/${OBJECT}"

echo "Building image ${IMAGE} ..."
gcloud builds submit --tag "${IMAGE}"

echo "Deploying Cloud Run service ${SERVICE} ..."
gcloud run deploy "${SERVICE}" \
  --image "${IMAGE}" \
  --region "${REGION}" \
  --platform managed \
  --no-allow-unauthenticated \
  --set-env-vars MULTI_MODE=true,CONFIG_URL="${CONFIG_URL}",BQ_LOCATION="${BQ_LOCATION}"

SERVICE_URL=$(gcloud run services describe "${SERVICE}" --region "${REGION}" --format='value(status.url)')
ID_TOKEN=$(gcloud auth print-identity-token)

echo "Done."
echo "Service URL: ${SERVICE_URL}"
echo "Test all pipelines:"
echo "  curl -s -H 'Authorization: Bearer ${ID_TOKEN}' ${SERVICE_URL}/run | jq"
echo "Run a single pipeline:"
echo "  curl -s -H 'Authorization: Bearer ${ID_TOKEN}' \"${SERVICE_URL}/run?pipeline=<PIPELINE_ID>\" | jq"
