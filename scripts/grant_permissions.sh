#!/usr/bin/env bash
set -euo pipefail

# Grant all required roles (including Cloud Storage) to a deploying user.
# Usage:
#   PROJECT_ID="my-project" USER_EMAIL="user@example.com" ./scripts/grant_permissions.sh

if [[ -z "${PROJECT_ID:-}" || -z "${USER_EMAIL:-}" ]]; then
  echo "Please set PROJECT_ID and USER_EMAIL env vars, e.g.:"
  echo '  PROJECT_ID="my-project" USER_EMAIL="user@example.com" ./scripts/grant_permissions.sh'
  exit 1
fi

ROLES=(
  roles/run.admin
  roles/cloudscheduler.admin
  roles/iam.serviceAccountAdmin
  roles/iam.serviceAccountUser
  roles/cloudbuild.builds.editor
  roles/artifactregistry.admin
  roles/serviceusage.serviceUsageAdmin
  roles/storage.admin
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

echo "All roles granted."
