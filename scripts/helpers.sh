#!/usr/bin/env bash
set -euo pipefail


need() { command -v "$1" >/dev/null 2>&1 || { echo "Missing dependency: $1"; exit 1; }; }
confirm() { read -rp "$1 [y/N]: " yn; [[ ${yn,,} == y ]]; }
ensure_apis() {
gcloud services enable run.googleapis.com cloudscheduler.googleapis.com \
bigquery.googleapis.com artifactregistry.googleapis.com iam.googleapis.com
}
ensure_repo() {
local PROJECT_ID=$1 REGION=$2 REPO=${3:-containers}
gcloud artifacts repositories describe "$REPO" --location="$REGION" 2>/dev/null || \
gcloud artifacts repositories create "$REPO" --repository-format=docker --location="$REGION" --description="Containers"
}
get_service_url() {
local SERVICE=$1 REGION=$2
gcloud run services describe "$SERVICE" --region "$REGION" --format='value(status.url)'
}
get_service_account() {
local SERVICE=$1 REGION=$2
gcloud run services describe "$SERVICE" --region "$REGION" --format='value(spec.template.spec.serviceAccountName)'
}