#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."  # ensure repo root

# shellcheck disable=SC1091
. scripts/common.sh

# Update Environment Variables on Deployed Service
#
# This script allows you to update environment variables on an already-deployed
# Cloud Run service without redeploying the entire service.
#
# Supported variables:
#   - ALLOW_UNKNOWN_COLUMNS: true/false (skip unknown CSV columns)
#   - USE_SOURCE_SHEETS: TRUE/FALSE (switch between Google Sheets and YAML config)
#   - CONFIG_SHEET_ID: Google Sheets ID (when using Google Sheets)
#   - LOG_LEVEL: DEBUG/INFO/WARNING/ERROR
#   - BATCH_SIZE: Rows per batch for BigQuery load
#   - CHUNK_SIZE: Bytes per chunk for HTTP streaming
#   - DEDUPE_MODE: no_dedupe/auto_dedupe/full_dedupe
#   - WINDOW_DAYS: Days to retain for non-empty tables
#
# Usage:
#   ./scripts/update_env.sh
#   ./scripts/update_env.sh --allow-unknown true
#   ./scripts/update_env.sh --use-sheets true --sheet-id YOUR_SHEET_ID
#   ./scripts/update_env.sh --log-level DEBUG

load_state

PROJECT_ID=${PROJECT_ID:-$(active_project)}
REGION=${REGION:-europe-west1}
SERVICE=${SERVICE:-csv-bq-multi}

echo "== Update Environment Variables =="
echo ""
echo "Service: $SERVICE"
echo "Region: $REGION"
echo ""

# Parse command-line arguments
ALLOW_UNKNOWN=""
USE_SHEETS=""
SHEET_ID=""
LOG_LEVEL=""
BATCH_SIZE=""
CHUNK_SIZE=""
DEDUPE_MODE=""
WINDOW_DAYS=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --allow-unknown)
      ALLOW_UNKNOWN="$2"
      shift 2
      ;;
    --use-sheets)
      USE_SHEETS="$2"
      shift 2
      ;;
    --sheet-id)
      SHEET_ID="$2"
      shift 2
      ;;
    --log-level)
      LOG_LEVEL="$2"
      shift 2
      ;;
    --batch-size)
      BATCH_SIZE="$2"
      shift 2
      ;;
    --chunk-size)
      CHUNK_SIZE="$2"
      shift 2
      ;;
    --dedupe-mode)
      DEDUPE_MODE="$2"
      shift 2
      ;;
    --window-days)
      WINDOW_DAYS="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# If no arguments provided, use interactive mode
if [[ -z "$ALLOW_UNKNOWN" && -z "$USE_SHEETS" && -z "$LOG_LEVEL" && -z "$BATCH_SIZE" && -z "$CHUNK_SIZE" && -z "$DEDUPE_MODE" && -z "$WINDOW_DAYS" ]]; then
  echo "Select which variables to update:"
  echo ""
  echo "1) ALLOW_UNKNOWN_COLUMNS (skip unknown CSV columns)"
  echo "2) USE_SOURCE_SHEETS (switch between Google Sheets and YAML)"
  echo "3) LOG_LEVEL (change logging level)"
  echo "4) BATCH_SIZE (rows per batch for BigQuery)"
  echo "5) CHUNK_SIZE (bytes per chunk for HTTP streaming)"
  echo "6) DEDUPE_MODE (deduplication mode)"
  echo "7) WINDOW_DAYS (days to retain for non-empty tables)"
  echo "8) View current environment variables"
  echo ""
  
  CHOICE=$(prompt_default "Select option (1-8)" "1")
  
  case $CHOICE in
    1)
      ALLOW_UNKNOWN=$(prompt_default "ALLOW_UNKNOWN_COLUMNS (true/false)" "false")
      ;;
    2)
      USE_SHEETS=$(prompt_default "USE_SOURCE_SHEETS (TRUE/FALSE)" "FALSE")
      if [[ "$USE_SHEETS" == "TRUE" ]]; then
        SHEET_ID=$(prompt_default "CONFIG_SHEET_ID" "")
        if [[ -z "$SHEET_ID" ]]; then
          echo "❌ Sheet ID is required when enabling Google Sheets"
          exit 1
        fi
      fi
      ;;
    3)
      LOG_LEVEL=$(prompt_default "LOG_LEVEL (DEBUG/INFO/WARNING/ERROR)" "INFO")
      ;;
    4)
      BATCH_SIZE=$(prompt_default "BATCH_SIZE (rows per batch)" "5000")
      ;;
    5)
      CHUNK_SIZE=$(prompt_default "CHUNK_SIZE (bytes per chunk)" "1048576")
      ;;
    6)
      DEDUPE_MODE=$(prompt_default "DEDUPE_MODE (no_dedupe/auto_dedupe/full_dedupe)" "auto_dedupe")
      ;;
    7)
      WINDOW_DAYS=$(prompt_default "WINDOW_DAYS (days to retain)" "30")
      ;;
    8)
      echo "Current environment variables:"
      gcloud run services describe "$SERVICE" --region "$REGION" --project "$PROJECT_ID" \
        --format="table(spec.template.spec.containers[0].env[].name,spec.template.spec.containers[0].env[].value)"
      exit 0
      ;;
    *)
      echo "❌ Invalid option"
      exit 1
      ;;
  esac
fi

# Fetch current environment variables
echo "Fetching current environment variables..."
CURRENT_ENV=$(gcloud run services describe "$SERVICE" --region "$REGION" --project "$PROJECT_ID" \
  --format=json | jq -r '.spec.template.spec.containers[0].env | map("\(.name)=\(.value)") | join(",")')

# Parse current environment into an associative array
declare -A ENV_MAP
IFS=',' read -ra ENV_PAIRS <<< "$CURRENT_ENV"
for pair in "${ENV_PAIRS[@]}"; do
  if [[ -n "$pair" ]]; then
    key="${pair%%=*}"
    value="${pair#*=}"
    ENV_MAP["$key"]="$value"
  fi
done

# Update environment variables
if [[ -n "$ALLOW_UNKNOWN" ]]; then
  ENV_MAP["ALLOW_UNKNOWN_COLUMNS"]="$ALLOW_UNKNOWN"
fi

if [[ -n "$USE_SHEETS" ]]; then
  ENV_MAP["USE_SOURCE_SHEETS"]="$USE_SHEETS"
  if [[ "$USE_SHEETS" == "TRUE" && -n "$SHEET_ID" ]]; then
    ENV_MAP["CONFIG_SHEET_ID"]="$SHEET_ID"
    # Remove YAML-specific variables if switching to Sheets
    unset 'ENV_MAP[CONFIG_URL]'
    unset 'ENV_MAP[SCHEMA_URL]'
  elif [[ "$USE_SHEETS" == "FALSE" ]]; then
    # Remove Sheets-specific variables if switching to YAML
    unset 'ENV_MAP[CONFIG_SHEET_ID]'
  fi
fi

if [[ -n "$LOG_LEVEL" ]]; then
  ENV_MAP["LOG_LEVEL"]="$LOG_LEVEL"
fi

if [[ -n "$BATCH_SIZE" ]]; then
  ENV_MAP["BATCH_SIZE"]="$BATCH_SIZE"
fi

if [[ -n "$CHUNK_SIZE" ]]; then
  ENV_MAP["CHUNK_SIZE"]="$CHUNK_SIZE"
fi

if [[ -n "$DEDUPE_MODE" ]]; then
  ENV_MAP["DEDUPE_MODE"]="$DEDUPE_MODE"
fi

if [[ -n "$WINDOW_DAYS" ]]; then
  ENV_MAP["WINDOW_DAYS"]="$WINDOW_DAYS"
fi

# Build the environment variables string
ENV_VARS=""
for key in "${!ENV_MAP[@]}"; do
  if [[ -z "$ENV_VARS" ]]; then
    ENV_VARS="${key}=${ENV_MAP[$key]}"
  else
    ENV_VARS="${ENV_VARS},${key}=${ENV_MAP[$key]}"
  fi
done

# Show what will be updated
echo ""
echo "📋 Changes to be applied:"
echo ""
if [[ -n "$ALLOW_UNKNOWN" ]]; then
  echo "  ALLOW_UNKNOWN_COLUMNS: ${ENV_MAP[ALLOW_UNKNOWN_COLUMNS]}"
fi
if [[ -n "$USE_SHEETS" ]]; then
  echo "  USE_SOURCE_SHEETS: ${ENV_MAP[USE_SOURCE_SHEETS]}"
  if [[ -n "$SHEET_ID" ]]; then
    echo "  CONFIG_SHEET_ID: ${ENV_MAP[CONFIG_SHEET_ID]}"
  fi
fi
if [[ -n "$LOG_LEVEL" ]]; then
  echo "  LOG_LEVEL: ${ENV_MAP[LOG_LEVEL]}"
fi
if [[ -n "$BATCH_SIZE" ]]; then
  echo "  BATCH_SIZE: ${ENV_MAP[BATCH_SIZE]}"
fi
if [[ -n "$CHUNK_SIZE" ]]; then
  echo "  CHUNK_SIZE: ${ENV_MAP[CHUNK_SIZE]}"
fi
if [[ -n "$DEDUPE_MODE" ]]; then
  echo "  DEDUPE_MODE: ${ENV_MAP[DEDUPE_MODE]}"
fi
if [[ -n "$WINDOW_DAYS" ]]; then
  echo "  WINDOW_DAYS: ${ENV_MAP[WINDOW_DAYS]}"
fi
echo ""

read -rp "Apply changes? (Y/N): " CONFIRM
if [[ ! "$CONFIRM" =~ ^[Yy]$ ]]; then
  echo "❌ Cancelled"
  exit 1
fi

# Update the service
echo ""
echo "Updating Cloud Run service..."
gcloud run services update "$SERVICE" \
  --region "$REGION" \
  --project "$PROJECT_ID" \
  --set-env-vars "$ENV_VARS" \
  --quiet

echo ""
echo "✅ Environment variables updated"
echo ""
echo "Updated variables:"
gcloud run services describe "$SERVICE" --region "$REGION" --project "$PROJECT_ID" \
  --format="table(spec.template.spec.containers[0].env[].name,spec.template.spec.containers[0].env[].value)"