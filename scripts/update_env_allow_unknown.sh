#!/usr/bin/env bash
set -euo pipefail

# Fixed version of update_env_allow_unknown.sh
# This script updates ALLOW_UNKNOWN_COLUMNS while preserving all other environment variables
#
# Usage:
#   ./update_env_allow_unknown.sh
#   REGION=europe-west1 SERVICE=csv-bq-multi ./update_env_allow_unknown.sh

REGION=${REGION:-europe-west1}
SERVICE=${SERVICE:-csv-bq-multi}

echo "🔄 Updating service [$SERVICE] in region [$REGION] to allow unknown columns..."

# Get all current environment variables
echo "📋 Fetching current environment variables..."
CURRENT_ENV=$(gcloud run services describe "$SERVICE" --region "$REGION" \
  --format="value(spec.template.spec.containers[0].env[].name,spec.template.spec.containers[0].env[].value)" 2>/dev/null || true)

# Build the new environment variables string, preserving existing ones
ENV_VARS=""
while IFS=$'\t' read -r name value; do
  if [[ -n "$name" ]]; then
    if [[ -z "$ENV_VARS" ]]; then
      ENV_VARS="${name}=${value}"
    else
      ENV_VARS="${ENV_VARS},${name}=${value}"
    fi
  fi
done <<< "$CURRENT_ENV"

# Add or update ALLOW_UNKNOWN_COLUMNS
if [[ "$ENV_VARS" == *"ALLOW_UNKNOWN_COLUMNS"* ]]; then
  # Replace existing ALLOW_UNKNOWN_COLUMNS
  ENV_VARS=$(echo "$ENV_VARS" | sed 's/ALLOW_UNKNOWN_COLUMNS=[^,]*/ALLOW_UNKNOWN_COLUMNS=true/')
else
  # Add new ALLOW_UNKNOWN_COLUMNS
  if [[ -z "$ENV_VARS" ]]; then
    ENV_VARS="ALLOW_UNKNOWN_COLUMNS=true"
  else
    ENV_VARS="${ENV_VARS},ALLOW_UNKNOWN_COLUMNS=true"
  fi
fi

echo "✅ Updating service with preserved environment variables..."
gcloud run services update "$SERVICE" \
  --region "$REGION" \
  --set-env-vars "$ENV_VARS"

echo "✅ Updated. Verifying:"
gcloud run services describe "$SERVICE" --region "$REGION" \
  --format="table(spec.template.spec.containers[].env[].name,spec.template.spec.containers[].env[].value)"
