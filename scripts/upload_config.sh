#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."  # repo root

# shellcheck disable=SC1091
. scripts/common.sh

echo "== Upload/Update Remote YAML and Wire CONFIG_URL =="

load_state || true

PROJECT_ID=${PROJECT_ID:-$(active_project)}
PROJECT_ID=$(prompt_default "Project ID" "${PROJECT_ID}")
REGION=$(prompt_default "Region" "${REGION:-europe-west1}")
SERVICE=$(prompt_default "Service name" "${SERVICE:-csv-bq-multi}")
LOCAL_YAML=$(prompt_default "Path to local YAML" "config/config.yaml")
DEFAULT_BUCKET="csv-config-${PROJECT_ID}"
BUCKET=$(prompt_default "GCS bucket name" "${BUCKET:-$DEFAULT_BUCKET}")
OBJECT=$(prompt_default "Object name" "${OBJECT:-csv_config.yaml}")

if [[ ! -r "$LOCAL_YAML" ]]; then echo "❌ Cannot read YAML: $LOCAL_YAML"; exit 1; fi
sed -i 's/\r$//' "$LOCAL_YAML" || true

# Validate YAML
python3 - <<'PY'
import yaml,sys,os
p=os.environ.get("LOCAL_YAML")
with open(p,"rb") as f:
    d=yaml.safe_load(f)
assert isinstance(d,dict) and isinstance(d.get("pipelines"),list)
print("✅ YAML valid; pipelines:", len(d["pipelines"]))
PY

gsutil mb -p "${PROJECT_ID}" -l "${REGION}" "gs://${BUCKET}" 2>/dev/null || true
gsutil cp "${LOCAL_YAML}" "gs://${BUCKET}/${OBJECT}"

CONFIG_URL="https://storage.googleapis.com/${BUCKET}/${OBJECT}"

gcloud run services update "${SERVICE}" \
  --region "${REGION}" \
  --set-env-vars CONFIG_URL="${CONFIG_URL}",MULTI_MODE=true

# Runtime SA -> objectViewer
RUNTIME_SA=$(gcloud run services describe "${SERVICE}" --region "${REGION}" --format="value(spec.template.spec.serviceAccountName)")
if [[ -z "${RUNTIME_SA}" ]]; then
  PROJECT_NUMBER=$(gcloud projects describe "${PROJECT_ID}" --format="value(projectNumber)")
  RUNTIME_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
fi
gsutil iam ch serviceAccount:${RUNTIME_SA}:objectViewer gs://${BUCKET}

SERVICE_URL=$(gcloud run services describe "${SERVICE}" --region "${REGION}" --format='value(status.url)')
save_state "$PROJECT_ID" "$REGION" "$SERVICE" "$SERVICE_URL"

echo
echo "✅ CONFIG_URL wired to $SERVICE"
echo "Service URL:   $SERVICE_URL"
echo "CONFIG_URL:    $CONFIG_URL"
echo
echo "Test now:"
echo "  ID_TOKEN=\$(gcloud auth print-identity-token)"
echo "  curl -s -H \"Authorization: Bearer \$ID_TOKEN\" \"$SERVICE_URL/run\" | jq"