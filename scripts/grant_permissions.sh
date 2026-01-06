#!/usr/bin/env bash
set -euo pipefail

# Grant all required roles to a deploying user.
# 
# This includes permissions for:
#   - Cloud Run deployment and management
#   - BigQuery operations
#   - Cloud Storage (YAML configs)
#   - Google Sheets (configuration source)
#   - Cloud Scheduler (scheduling)
#   - IAM and service account management
#   - Cloud Build and Artifact Registry
#
# Usage:
#   PROJECT_ID="my-project" USER_EMAIL="user@example.com" ./scripts/grant_permissions.sh

if [[ -z "${PROJECT_ID:-}" || -z "${USER_EMAIL:-}" ]]; then
  echo "Please set PROJECT_ID and USER_EMAIL env vars, e.g.:"
  echo '  PROJECT_ID="my-project" USER_EMAIL="user@example.com" ./scripts/grant_permissions.sh'
  exit 1
fi

ROLES=(
  roles/run.admin                      # Deploy and manage Cloud Run services
  roles/cloudscheduler.admin           # Create and manage scheduled jobs
  roles/iam.serviceAccountAdmin        # Create and manage service accounts
  roles/iam.serviceAccountUser         # Allow using service accounts in deploys
  roles/cloudbuild.builds.editor       # Build and push Docker images
  roles/artifactregistry.admin         # Create and manage Artifact Registry repositories
  roles/serviceusage.serviceUsageAdmin # Enable required APIs
  roles/storage.admin                  # Create buckets & upload YAML (remote config)
  roles/bigquery.admin                 # BigQuery operations (for testing/validation)
  roles/editor                         # For Google Sheets API access (alternative: roles/sheets.editor if available)
)

echo "Granting roles to user ${USER_EMAIL} on project ${PROJECT_ID}..."
for ROLE in "${ROLES[@]}"; do
  echo "  -> ${ROLE}"
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="user:${USER_EMAIL}" \
    --role="${ROLE}" \
    --condition=None >/dev/null
done

# Ensure Cloud Build SA can push to Artifact Registry
PROJECT_NUMBER=$(gcloud projects describe "${PROJECT_ID}" --format='value(projectNumber)')
CB_SA="${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com"
echo "Granting Artifact Registry writer to Cloud Build SA: ${CB_SA}"
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${CB_SA}" \
  --role="roles/artifactregistry.writer" \
  --condition=None >/dev/null

echo "✅ All roles granted."