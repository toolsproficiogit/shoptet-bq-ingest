#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."  # ensure repo root

# shellcheck disable=SC1091
. scripts/common.sh

echo "== Shoptet → BigQuery: Multi-pipeline (Remote YAML) Deploy =="

PROJECT_ID=${PROJECT_ID:-$(active_project)}
PROJECT_ID=$(prompt_default "Project ID" "${PROJECT_ID}")
REGION=$(prompt_default "Region" "europe-west1")
SERVICE=$(prompt_default "Service name" "shoptet-bq-multi")
LOCAL_YAML=$(prompt_default "Local YAML path" "config/config.yaml")
LOCAL_SCHEMAS=$(prompt_default "Schema library path" "config/schemas.yaml")
BQ_LOCATION=$(prompt_default "BigQuery location" "EU")

# Config bucket defaults to unique per project
DEFAULT_BUCKET="shoptet-config-${PROJECT_ID}"
BUCKET=$(prompt_default "GCS bucket for config" "$DEFAULT_BUCKET")
OBJECT_CFG=$(prompt_default "Pipelines object name" "shoptet_config.yaml")
OBJECT_SCH=$(prompt_default "Schemas object name" "schemas.yaml")

REPO=${REPO:-containers}
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${SERVICE}:v1"

echo "Enabling required APIs (idempotent)..."
gcloud services enable run.googleapis.com artifactregistry.googleapis.com cloudbuild.googleapis.com iam.googleapis.com >/dev/null

echo "Ensuring Artifact Registry repo '${REPO}' exists in ${REGION}..."
if ! gcloud artifacts repositories describe "${REPO}" --location "${REGION}" >/dev/null 2>&1; then
  gcloud artifacts repositories create "${REPO}" --repository-format=docker --location "${REGION}" --description="Containers"
fi

# Preflight YAML readability + normalize newlines
for f in "$LOCAL_YAML" "$LOCAL_SCHEMAS"; do
  if [[ ! -r "$f" ]]; then echo "❌ Cannot read file: $f"; exit 1; fi
  sed -i 's/\r$//' "$f" || true
done

# Make sure inline Python sees these vars
export LOCAL_YAML
export LOCAL_SCHEMAS

# Validate pipelines YAML
python3 - <<'PY'
import yaml,os,sys
p=os.environ.get("LOCAL_YAML")
with open(p,"rb") as f:
    d=yaml.safe_load(f)
assert isinstance(d,dict), "Top-level must be a mapping"
pls=d.get("pipelines")
assert isinstance(pls,list) and len(pls)>0, "'pipelines' must be a non-empty list"
print("✅ Pipelines YAML OK:", len(pls), "pipeline(s)")
PY

# Validate schema library YAML
python3 - <<'PY'
import yaml,os,sys
p=os.environ.get("LOCAL_SCHEMAS")
with open(p,"rb") as f:
    d=yaml.safe_load(f)
ets=d.get("export_types",{})
assert isinstance(ets,dict) and len(ets)>=1, "'export_types' must be a mapping with at least one schema"
for name,fields in ets.items():
    assert isinstance(fields,list) and all(isinstance(f,dict) for f in fields), f"schema '{name}' must be list of field maps"
print("✅ Schema library OK:", ", ".join(ets.keys()))
PY

# Duplicate table detection with confirmation
echo "Checking for duplicate BigQuery table targets..."
DUPES=$(python3 - <<'PY'
import yaml,os,collections
p=os.environ.get("LOCAL_YAML")
with open(p,"rb") as f:
    d=yaml.safe_load(f)
cts=collections.Counter(p.get("bq_table_id") for p in d.get("pipelines",[]) if p.get("bq_table_id"))
dupes=[t for t,c in cts.items() if c>1]
print(",".join(dupes))
PY
)
if [[ -n "$DUPES" ]]; then
  echo "⚠️  WARNING: Multiple pipelines point to the same BigQuery table(s):"
  echo "    $DUPES"
  read -rp "Do you wish to proceed anyway? (Y/N): " CONFIRM
  if [[ ! "$CONFIRM" =~ ^[Yy]$ ]]; then
    echo "❌ Deployment cancelled. Edit your YAML to fix the duplicates."
    exit 1
  fi
fi

echo "Ensuring bucket gs://${BUCKET} ..."
gsutil mb -p "${PROJECT_ID}" -l "${REGION}" "gs://${BUCKET}" 2>/dev/null || true

echo "Uploading configs to gs://${BUCKET}/ ..."
gsutil cp "${LOCAL_YAML}" "gs://${BUCKET}/${OBJECT_CFG}"
gsutil cp "${LOCAL_SCHEMAS}" "gs://${BUCKET}/${OBJECT_SCH}"

CONFIG_URL="https://storage.googleapis.com/${BUCKET}/${OBJECT_CFG}"
SCHEMA_URL="https://storage.googleapis.com/${BUCKET}/${OBJECT_SCH}"

echo "Building image ${IMAGE} ..."
gcloud builds submit --tag "${IMAGE}"

echo "Deploying Cloud Run service ${SERVICE} ..."
gcloud run deploy "${SERVICE}" \
  --image "${IMAGE}" \
  --region "${REGION}" \
  --platform managed \
  --no-allow-unauthenticated \
  --set-env-vars MULTI_MODE=true,CONFIG_URL="${CONFIG_URL}",SCHEMA_URL="${SCHEMA_URL}",BQ_LOCATION="${BQ_LOCATION}"

SERVICE_URL=$(gcloud run services describe "${SERVICE}" --region "${REGION}" --format='value(status.url)')

# Determine runtime service account (explicit or default GCE SA)
RUNTIME_SA=$(gcloud run services describe "${SERVICE}" --region "${REGION}" --format="value(spec.template.spec.serviceAccountName)")
if [[ -z "${RUNTIME_SA}" ]]; then
  PROJECT_NUMBER=$(gcloud projects describe "${PROJECT_ID}" --format="value(projectNumber)")
  RUNTIME_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
fi

# Grant objectViewer on the config bucket to the runtime SA
gsutil iam ch "serviceAccount:${RUNTIME_SA}:objectViewer" "gs://${BUCKET}"

# Persist basic state for helper scripts
save_state "$PROJECT_ID" "$REGION" "$SERVICE" "$SERVICE_URL"

echo
echo "✅ Deployed"
echo "Service URL:   $SERVICE_URL"
echo "CONFIG_URL:    $CONFIG_URL"
echo "SCHEMA_URL:    $SCHEMA_URL"
echo
echo "First run:"
echo "  ./scripts/trigger.sh"
