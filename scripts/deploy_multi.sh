#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."  # ensure repo root

# shellcheck disable=SC1091
. scripts/common.sh

echo "== CSV → BigQuery: Multi-pipeline Deploy (YAML or Google Sheets) =="
echo ""

PROJECT_ID=${PROJECT_ID:-$(active_project)}
PROJECT_ID=$(prompt_default "Project ID" "${PROJECT_ID}")
REGION=$(prompt_default "Region" "europe-west1")
SERVICE=$(prompt_default "Service name" "csv-bq-multi")

# Configuration source selection
echo ""
echo "Configuration source:"
echo "  1) YAML files in GCS (default)"
echo "  2) Google Sheets"
echo ""
CONFIG_SOURCE=$(prompt_default "Select source (1 or 2)" "1")

BQ_LOCATION=$(prompt_default "BigQuery location" "EU")

# Allow unknown columns setting
echo ""
echo "Unknown columns handling:"
echo "  If your CSV exports include unexpected or renamed fields,"
echo "  enable this to skip unrecognized columns instead of failing."
echo ""
ALLOW_UNKNOWN=$(prompt_default "Allow unknown columns (true/false)" "false")

REPO=${REPO:-containers}
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${SERVICE}:v1"

# Enable required APIs
echo ""
echo "📡 Enabling required APIs..."
APIS_TO_ENABLE=(
  "run.googleapis.com"
  "artifactregistry.googleapis.com"
  "cloudbuild.googleapis.com"
  "iam.googleapis.com"
  "bigquery.googleapis.com"
  "storage-api.googleapis.com"
  "logging.googleapis.com"
  "monitoring.googleapis.com"
  "cloudscheduler.googleapis.com"
)

# Add Google Sheets APIs if using Sheets config
if [[ "$CONFIG_SOURCE" == "2" ]]; then
  APIS_TO_ENABLE+=(
    "sheets.googleapis.com"
    "drive.googleapis.com"
  )
fi

for api in "${APIS_TO_ENABLE[@]}"; do
  gcloud services enable "$api" --project="$PROJECT_ID" >/dev/null 2>&1 || true
done
echo "✓ APIs enabled"

echo ""
echo "Ensuring Artifact Registry repo '${REPO}' exists in ${REGION}..."
if ! gcloud artifacts repositories describe "${REPO}" --location "${REGION}" >/dev/null 2>&1; then
  gcloud artifacts repositories create "${REPO}" --repository-format=docker --location "${REGION}" --description="Containers"
fi

# Configuration source handling
if [[ "$CONFIG_SOURCE" == "2" ]]; then
  # Google Sheets configuration
  echo ""
  echo "🔧 Google Sheets Configuration"
  echo ""
  SHEET_ID=$(prompt_default "Google Sheets ID" "")
  
  if [[ -z "$SHEET_ID" ]]; then
    echo "❌ Google Sheets ID is required"
    exit 1
  fi
  
  # Set environment variables for Google Sheets
  ENV_VARS="USE_SOURCE_SHEETS=TRUE,CONFIG_SHEET_ID=${SHEET_ID},BQ_LOCATION=${BQ_LOCATION},ALLOW_UNKNOWN_COLUMNS=${ALLOW_UNKNOWN},BATCH_SIZE=5000,CHUNK_SIZE=1048576,DEDUPE_MODE=auto_dedupe,WINDOW_DAYS=30,LOG_LEVEL=INFO"
  
  CONFIG_URL="sheets://${SHEET_ID}"
  SCHEMA_URL="sheets://${SHEET_ID}"
  
else
  # YAML configuration (default)
  echo ""
  echo "📄 YAML Configuration"
  echo ""
  LOCAL_YAML=$(prompt_default "Local YAML path" "config/config.yaml")
  LOCAL_SCHEMAS=$(prompt_default "Schema library path" "config/schemas.yaml")
  
  # Config bucket defaults to unique per project
  DEFAULT_BUCKET="csv-config-${PROJECT_ID}"
  BUCKET=$(prompt_default "GCS bucket for config" "$DEFAULT_BUCKET")
  OBJECT_CFG=$(prompt_default "Pipelines object name" "csv_config.yaml")
  OBJECT_SCH=$(prompt_default "Schemas object name" "schemas.yaml")
  
  # Preflight YAML readability + normalize newlines
  for f in "$LOCAL_YAML" "$LOCAL_SCHEMAS"; do
    if [[ ! -r "$f" ]]; then echo "❌ Cannot read file: $f"; exit 1; fi
    sed -i 's/\r$//' "$f" || true
  done
  
  # Make sure inline Python sees these vars
  export LOCAL_YAML
  export LOCAL_SCHEMAS
  
  # Validate pipelines YAML
  echo "Validating pipelines YAML..."
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
  echo "Validating schema library YAML..."
  python3 - <<'PY'
import yaml,os,sys
p=os.environ.get("LOCAL_SCHEMAS")
with open(p,"rb") as f:
    d=yaml.safe_load(f)
ets=d.get("schemas",{})
assert isinstance(ets,dict) and len(ets)>=1, "'schemas' must be a mapping with at least one schema"
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
  
  # Set environment variables for YAML
  ENV_VARS="USE_SOURCE_SHEETS=FALSE,CONFIG_URL=${CONFIG_URL},SCHEMA_URL=${SCHEMA_URL},BQ_LOCATION=${BQ_LOCATION},ALLOW_UNKNOWN_COLUMNS=${ALLOW_UNKNOWN},BATCH_SIZE=5000,CHUNK_SIZE=1048576,DEDUPE_MODE=auto_dedupe,WINDOW_DAYS=30,LOG_LEVEL=INFO"
fi

echo ""
echo "Building image ${IMAGE} ..."
gcloud builds submit --tag "${IMAGE}"

echo ""
echo "Deploying Cloud Run service ${SERVICE} ..."
gcloud run deploy "${SERVICE}" \
  --image "${IMAGE}" \
  --region "${REGION}" \
  --platform managed \
  --no-allow-unauthenticated \
  --memory 1Gi \
  --timeout 900 \
  --set-env-vars "$ENV_VARS"

SERVICE_URL=$(gcloud run services describe "${SERVICE}" --region "${REGION}" --format='value(status.url)')

# Determine runtime service account (explicit or default GCE SA)
RUNTIME_SA=$(gcloud run services describe "${SERVICE}" --region "${REGION}" --format="value(spec.template.spec.serviceAccountName)")
if [[ -z "${RUNTIME_SA}" ]]; then
  PROJECT_NUMBER=$(gcloud projects describe "${PROJECT_ID}" --format="value(projectNumber)")
  RUNTIME_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
fi

# Grant permissions to runtime service account
if [[ "$CONFIG_SOURCE" == "2" ]]; then
  # For Google Sheets: grant Sheets API access
  echo ""
  echo "Granting Google Sheets API permissions to service account ${RUNTIME_SA}..."
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${RUNTIME_SA}" \
    --role="roles/editor" \
    --condition=None >/dev/null 2>&1 || true
else
  # For YAML: grant GCS bucket access
  echo ""
  echo "Granting GCS bucket access to service account ${RUNTIME_SA}..."
  gsutil iam ch "serviceAccount:${RUNTIME_SA}:objectViewer" "gs://${BUCKET}"
fi

# Grant BigQuery permissions to runtime service account
echo "Granting BigQuery permissions to service account ${RUNTIME_SA}..."
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${RUNTIME_SA}" \
  --role="roles/bigquery.dataEditor" \
  --condition=None >/dev/null 2>&1 || true

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${RUNTIME_SA}" \
  --role="roles/bigquery.jobUser" \
  --condition=None >/dev/null 2>&1 || true

# Persist basic state for helper scripts
save_state "$PROJECT_ID" "$REGION" "$SERVICE" "$SERVICE_URL"

echo ""
echo "✅ Deployed"
echo "Service URL:   $SERVICE_URL"
if [[ "$CONFIG_SOURCE" == "2" ]]; then
  echo "Config Source: Google Sheets (ID: $SHEET_ID)"
else
  echo "CONFIG_URL:    $CONFIG_URL"
  echo "SCHEMA_URL:    $SCHEMA_URL"
fi
echo ""
echo "First run:"
echo "  ./scripts/trigger.sh"