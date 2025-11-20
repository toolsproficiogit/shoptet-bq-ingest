#!/usr/bin/env bash
set -euo pipefail

REGION=${REGION:-europe-west1}
SERVICE=${SERVICE:-csv-bq-multi}

echo "🔄 Updating service [$SERVICE] in region [$REGION] to allow unknown columns..."
gcloud run services update "$SERVICE" \
  --region "$REGION" \
  --set-env-vars ALLOW_UNKNOWN_COLUMNS=true

echo "✅ Updated. Verifying:"
gcloud run services describe "$SERVICE" --region "$REGION" \
  --format="table(spec.template.spec.containers[].env[].name,spec.template.spec.containers[].env[].value)"