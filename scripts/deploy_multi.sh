#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/helpers.sh"
need gcloud


read -rp "Project ID: " PROJECT_ID
read -rp "Region (e.g. europe-central2): " REGION
read -rp "Service name [shoptet-bq-multi]: " SERVICE; SERVICE=${SERVICE:-shoptet-bq-multi}
read -rp "Use baked config (config/config.yaml) or remote URL? [baked/url]: " MODE; MODE=${MODE:-baked}


REPO=containers
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${SERVICE}:v1"


gcloud config set project "$PROJECT_ID" >/dev/null
ensure_apis
ensure_repo "$PROJECT_ID" "$REGION" "$REPO"


gcloud builds submit --tag "$IMAGE"


if [[ $MODE == url ]]; then
read -rp "Config URL (HTTPS or signed GCS): " CONFIG_URL
ENVFLAGS="--set-env-vars MULTI_MODE=true,CONFIG_URL=$CONFIG_URL,BQ_LOCATION=EU"
else
ENVFLAGS="--set-env-vars MULTI_MODE=true,BQ_LOCATION=EU"
fi


gcloud run deploy "$SERVICE" \
--image "$IMAGE" \
--platform managed \
--region "$REGION" \
--no-allow-unauthenticated \
$ENVFLAGS


URL=$(get_service_url "$SERVICE" "$REGION")
ID_TOKEN=$(gcloud auth print-identity-token)
echo "Run all pipelines: curl -H 'Authorization: Bearer $ID_TOKEN' ${URL}/run | jq"
echo "Run one pipeline: curl -H 'Authorization: Bearer $ID_TOKEN' '${URL}/run?pipeline=orders_core' | jq"
