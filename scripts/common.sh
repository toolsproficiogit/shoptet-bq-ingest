#!/usr/bin/env bash
set -euo pipefail

STATE_FILE=".deploy_state"

prompt_default() {
  local prompt="$1"
  local default="${2:-}"
  if [[ -n "$default" ]]; then
    read -rp "$prompt [default: $default]: " REPLY || true
    echo "${REPLY:-$default}"
  else
    read -rp "$prompt: " REPLY || true
    echo "$REPLY"
  fi
}

active_project() {
  gcloud config get-value project 2>/dev/null || true
}

save_state() {
  local project="$1" region="$2" service="$3" service_url="$4"
  cat > "$STATE_FILE" <<EOF
PROJECT_ID="$project"
REGION="$region"
SERVICE="$service"
SERVICE_URL="$service_url"
EOF
}

load_state() {
  if [[ -f "$STATE_FILE" ]]; then
    # shellcheck source=/dev/null
    . "$STATE_FILE"
  fi
}
