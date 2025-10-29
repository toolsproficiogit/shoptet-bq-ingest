#!/usr/bin/env bash
# Upload a YAML config to GCS and wire it to a Cloud Run service via CONFIG_URL.
set -euo pipefail

prompt() { read -rp "$1: " REPLY && echo "$REPLY"; }

PROJECT_ID=${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || true)}
if [[ -z "${PROJECT_ID}" ]]; then PROJECT_ID=$(prompt "Project ID"); fi
REGION=${REGION:-$(prompt "Region (e.g. europe-west1)")}
SERVICE=${SERVICE:-$(prompt "Service name (e.g. shoptet-bq-multi)")}
LOCAL_YAML=${LOCAL_YAML:-$(prompt "Path to local YAML (e.g. config/config.yaml)")}
BUCKET=${BUCKET:-$(prompt "GCS bucket name (e.g. my-shoptet-configs)")}
OBJECT=${OBJECT:-$(prompt "Object name [shoptet_config.yaml]")}
OBJECT=${OBJECT:-shoptet_config.yaml}

echo "Ensuring bucket gs://${BUCKET} ..."
gsutil mb -p "${PROJECT_ID}" -l "${REGION}" "gs://${BUCKET}" 2>/dev/null || true

echo "Uploading ${LOCAL_YAML} to gs://${BUCKET}/${OBJECT} ..."
gsutil cp "${LOCAL_YAML}" "gs://${BUCKET}/${OBJECT}"

CONFIG_URL="https://storage.googleapis.com/${BUCKET}/${OBJECT}"

echo "Setting CONFIG_URL on service ${SERVICE} ..."
gcloud run services update "${SERVICE}" \
  --region "${REGION}" \
  --set-env-vars CONFIG_URL="${CONFIG_URL}",MULTI_MODE=true

SERVICE_URL=$(gcloud run services describe "${SERVICE}" --region "${REGION}" --format='value(status.url)')
ID_TOKEN=$(gcloud auth print-identity-token)
echo "Done. Test with:"
echo "  curl -s -H 'Authorization: Bearer ${ID_TOKEN}' ${SERVICE_URL}/run | jq"
echo "Current CONFIG_URL => ${CONFIG_URL}"
