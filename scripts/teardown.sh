#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/helpers.sh"
need gcloud


read -rp "Project ID: " PROJECT_ID
read -rp "Region: " REGION
read -rp "Service name to delete: " SERVICE
read -rp "Scheduler job to delete (blank to skip): " JOB


[[ -n "$JOB" ]] && gcloud scheduler jobs delete "$JOB" --location "$REGION" -q || true


gcloud run services delete "$SERVICE" --region "$REGION" -q || true