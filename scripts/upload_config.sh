#!/usr/bin/env bash
# Upload a YAML config to GCS and wire it to a Cloud Run service via CONFIG_URL.
set -euo pipefail
source "$(dirname "$0")/helpers.sh"
need gcloud
need gsutil


read -rp "Project ID: " PROJECT_ID
read -rp "Region (e.g. europe-central2): " REGION
read -rp "Service name (e.g. shoptet-bq-multi): " SERVICE
read -rp "Path to local YAML (e.g. config/config.yaml): " LOCAL_YAML
read -rp "GCS bucket name (e.g. my-shoptet-configs): " BUCKET
read -rp "Optional object name [shoptet_config.yaml]: " OBJ; OBJ=${OBJ:-shoptet_config.yaml}


# Ensure bucket exists
if ! gsutil ls -p "$PROJECT_ID" gs://$BUCKET >/dev/null 2>&1; then
echo "Creating bucket gs://$BUCKET ..."
gsutil mb -p "$PROJECT_ID" -l EU gs://$BUCKET
fi


echo "Uploading $LOCAL_YAML to gs://$BUCKET/$OBJ ..."
gsutil cp "$LOCAL_YAML" gs://$BUCKET/$OBJ


# Grant objectViewer to the Cloud Run service account on the bucket
SA=$(get_service_account "$SERVICE" "$REGION")
if [[ -z "${SA}" ]]; then
echo "Could not determine service account for $SERVICE in $REGION" >&2
exit 1
fi


echo "Granting objectViewer on bucket to $SA ..."
gsutil iam ch serviceAccount:${SA}:objectViewer gs://$BUCKET


CONFIG_URL="https://storage.googleapis.com/${BUCKET}/${OBJ}"


echo "Setting CONFIG_URL env on service ..."
gcloud run services update "$SERVICE" \
--region "$REGION" \
--set-env-vars CONFIG_URL="$CONFIG_URL",MULTI_MODE=true


URL=$(get_service_url "$SERVICE" "$REGION")
ID_TOKEN=$(gcloud auth print-identity-token)
echo "Done. Test with: curl -H 'Authorization: Bearer $ID_TOKEN' ${URL}/run | jq"
echo "Current CONFIG_URL => $CONFIG_URL"