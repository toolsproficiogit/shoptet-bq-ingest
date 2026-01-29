#!/bin/bash
# Deploy multi-pipeline CSV-to-BigQuery service with schema validation support

set -e

# Source common functions
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh" || {
    echo "Error: common.sh not found"
    exit 1
}

# ======================================================================
# CONFIGURATION
# ======================================================================

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
SERVICE_NAME="${SERVICE_NAME:-csv-bq-multi}"
REGION="${REGION:-europe-west1}"
MEMORY="${MEMORY:-2Gi}"
TIMEOUT="${TIMEOUT:-3600}"
ALLOW_UNKNOWN_COLUMNS="${ALLOW_UNKNOWN_COLUMNS:-false}"
SCHEMA_MIGRATION_MODE="${SCHEMA_MIGRATION_MODE:-strict}"
USE_SOURCE_SHEETS="${USE_SOURCE_SHEETS:-false}"
CONFIG_SOURCE=""
CONFIG_URL=""
SCHEMA_URL=""
CONFIG_SHEET_ID=""
PIPELINE_HASH_MOD="${PIPELINE_HASH_MOD:-1}"
PIPELINE_HASH_REMAINDER="${PIPELINE_HASH_REMAINDER:-0}"

# ======================================================================
# FUNCTIONS
# ======================================================================

print_header() {
    echo ""
    echo "╔════════════════════════════════════════════════════════════╗"
    echo "║  CSV-to-BigQuery Cloud Run Service Deployment              ║"
    echo "╚════════════════════════════════════════════════════════════╝"
    echo ""
}

prompt_configuration_source() {
    echo "Select configuration source:"
    echo "  1) YAML files (default)"
    echo "  2) Google Sheets"
    echo ""
    read -p "Enter choice (1 or 2) [1]: " choice
    choice=${choice:-1}
    
    if [ "$choice" = "2" ]; then
        USE_SOURCE_SHEETS="true"
        read -p "Enter Google Sheets ID: " CONFIG_SHEET_ID
        if [ -z "$CONFIG_SHEET_ID" ]; then
            echo "Error: Google Sheets ID is required"
            exit 1
        fi
    else
        USE_SOURCE_SHEETS="false"
        read -p "Enter CONFIG_URL (GCS path to csv_config.yaml): " CONFIG_URL
        read -p "Enter SCHEMA_URL (GCS path to schemas.yaml): " SCHEMA_URL
        if [ -z "$CONFIG_URL" ] || [ -z "$SCHEMA_URL" ]; then
            echo "Error: CONFIG_URL and SCHEMA_URL are required"
            exit 1
        fi
    fi
}

prompt_allow_unknown_columns() {
    echo ""
    echo "Allow unknown columns in CSV files?"
    echo "  - false (default): Reject CSVs with unknown columns"
    echo "  - true: Accept and load CSVs with unknown columns"
    echo ""
    read -p "Enter choice (true or false) [false]: " choice
    choice=${choice:-false}
    ALLOW_UNKNOWN_COLUMNS="$choice"
}

prompt_schema_migration_mode() {
    echo ""
    echo "Schema migration strategy:"
    echo "  1) strict (default): Fail if schema doesn't match"
    echo "  2) auto_migrate: Add new fields, fail on removals/changes"
    echo "  3) recreate: Drop and recreate table (DATA LOSS!)"
    echo ""
    read -p "Enter choice (1, 2, or 3) [1]: " choice
    choice=${choice:-1}
    
    case $choice in
        1) SCHEMA_MIGRATION_MODE="strict" ;;
        2) SCHEMA_MIGRATION_MODE="auto_migrate" ;;
        3) 
            echo ""
            echo "WARNING: 'recreate' mode will DELETE all data in the table!"
            read -p "Are you sure? (yes/no): " confirm
            if [ "$confirm" = "yes" ]; then
                SCHEMA_MIGRATION_MODE="recreate"
            else
                echo "Cancelled. Using 'strict' mode instead."
                SCHEMA_MIGRATION_MODE="strict"
            fi
            ;;
        *) SCHEMA_MIGRATION_MODE="strict" ;;
    esac
}

validate_inputs() {
    if [ -z "$PROJECT_ID" ]; then
        echo "Error: PROJECT_ID not set"
        exit 1
    fi
    
    if [ "$USE_SOURCE_SHEETS" = "true" ]; then
        if [ -z "$CONFIG_SHEET_ID" ]; then
            echo "Error: CONFIG_SHEET_ID is required when using Google Sheets"
            exit 1
        fi
    else
        if [ -z "$CONFIG_URL" ] || [ -z "$SCHEMA_URL" ]; then
            echo "Error: CONFIG_URL and SCHEMA_URL are required when using YAML"
            exit 1
        fi
    fi
}

enable_apis() {
    echo ""
    echo "Enabling required Google Cloud APIs..."
    gcloud services enable \
        cloudbuild.googleapis.com \
        run.googleapis.com \
        bigquery.googleapis.com \
        logging.googleapis.com \
        --project="$PROJECT_ID"
    
    if [ "$USE_SOURCE_SHEETS" = "true" ]; then
        echo "Enabling Google Sheets API for configuration..."
        gcloud services enable \
            sheets.googleapis.com \
            drive.googleapis.com \
            --project="$PROJECT_ID"
    fi
    
    echo "✓ APIs enabled"
}

build_and_deploy() {
    echo ""
    echo "Building and deploying service..."
    
    # Build Docker image
    gcloud builds submit \
        --project="$PROJECT_ID" \
        --tag="gcr.io/$PROJECT_ID/$SERVICE_NAME:latest" \
        --timeout="3600s" \
        .
    
    # Prepare environment variables
    ENV_VARS="MULTI_MODE=true"
    ENV_VARS="$ENV_VARS,BQ_LOCATION=EU"
    ENV_VARS="$ENV_VARS,ALLOW_UNKNOWN_COLUMNS=$ALLOW_UNKNOWN_COLUMNS"
    ENV_VARS="$ENV_VARS,SCHEMA_MIGRATION_MODE=$SCHEMA_MIGRATION_MODE"
    ENV_VARS="$ENV_VARS,PIPELINE_HASH_MOD=$PIPELINE_HASH_MOD"
    ENV_VARS="$ENV_VARS,PIPELINE_HASH_REMAINDER=$PIPELINE_HASH_REMAINDER"
    
    if [ "$USE_SOURCE_SHEETS" = "true" ]; then
        ENV_VARS="$ENV_VARS,USE_SOURCE_SHEETS=true"
        ENV_VARS="$ENV_VARS,CONFIG_SHEET_ID=$CONFIG_SHEET_ID"
    else
        ENV_VARS="$ENV_VARS,USE_SOURCE_SHEETS=false"
        ENV_VARS="$ENV_VARS,CONFIG_URL=$CONFIG_URL"
        ENV_VARS="$ENV_VARS,SCHEMA_URL=$SCHEMA_URL"
    fi
    
    # Deploy to Cloud Run
    gcloud run deploy "$SERVICE_NAME" \
        --project="$PROJECT_ID" \
        --region="$REGION" \
        --image="gcr.io/$PROJECT_ID/$SERVICE_NAME:latest" \
        --memory="$MEMORY" \
        --timeout="$TIMEOUT" \
        --set-env-vars="$ENV_VARS" \
        --no-allow-unauthenticated \
        --platform=managed
    
    echo "✓ Service deployed"
}

print_summary() {
    echo ""
    echo "╔════════════════════════════════════════════════════════════╗"
    echo "║  Deployment Summary                                        ║"
    echo "╚════════════════════════════════════════════════════════════╝"
    echo ""
    echo "Service Name:              $SERVICE_NAME"
    echo "Region:                    $REGION"
    echo "Project:                   $PROJECT_ID"
    echo ""
    echo "Configuration:"
    if [ "$USE_SOURCE_SHEETS" = "true" ]; then
        echo "  Source:                  Google Sheets"
        echo "  Sheet ID:                $CONFIG_SHEET_ID"
    else
        echo "  Source:                  YAML"
        echo "  Config URL:              $CONFIG_URL"
        echo "  Schema URL:              $SCHEMA_URL"
    fi
    echo ""
    echo "Settings:"
    echo "  Allow Unknown Columns:   $ALLOW_UNKNOWN_COLUMNS"
    echo "  Schema Migration Mode:   $SCHEMA_MIGRATION_MODE"
    if [ "$PIPELINE_HASH_MOD" -gt 1 ]; then
        echo "  Multi-Service Mode:      Enabled"
        echo "    Total Services:        $PIPELINE_HASH_MOD"
        echo "    This Service:          $PIPELINE_HASH_REMAINDER"
    else
        echo "  Multi-Service Mode:      Disabled (single service)"
    fi
    echo ""
    echo "Next steps:"
    echo "  1. Test the service: ./scripts/trigger.sh"
    echo "  2. View logs: gcloud run logs read $SERVICE_NAME --limit 50"
    echo "  3. Update env vars: ./scripts/update_env.sh"
    echo ""
}

prompt_multi_service_config() {
    echo ""
    echo "Multi-service deployment (hash-based distribution):"
    echo "  - Single service (default): PIPELINE_HASH_MOD=1"
    echo "  - Multiple services: Set PIPELINE_HASH_MOD to total number of services"
    echo ""
    read -p "Enter total number of services [1]: " hash_mod
    hash_mod=${hash_mod:-1}
    PIPELINE_HASH_MOD="$hash_mod"
    
    if [ "$PIPELINE_HASH_MOD" -gt 1 ]; then
        echo ""
        echo "This is service number (0 to $((PIPELINE_HASH_MOD - 1))):"
        read -p "Enter service number [0]: " hash_remainder
        hash_remainder=${hash_remainder:-0}
        PIPELINE_HASH_REMAINDER="$hash_remainder"
        
        echo ""
        echo "Multi-service configuration:"
        echo "  Total services:        $PIPELINE_HASH_MOD"
        echo "  This service number:   $PIPELINE_HASH_REMAINDER"
        echo "  Service name suffix:   -$PIPELINE_HASH_REMAINDER"
        
        SERVICE_NAME="${SERVICE_NAME}-${PIPELINE_HASH_REMAINDER}"
    fi
}

# ======================================================================
# MAIN
# ======================================================================

main() {
    print_header
    
    echo "Configuration Source Selection:"
    prompt_configuration_source
    
    echo ""
    echo "Additional Settings:"
    prompt_allow_unknown_columns
    prompt_schema_migration_mode
    prompt_multi_service_config
    
    echo ""
    echo "Validating inputs..."
    validate_inputs
    
    echo ""
    echo "Summary of deployment:"
    echo "  Project:                 $PROJECT_ID"
    echo "  Service:                 $SERVICE_NAME"
    echo "  Region:                  $REGION"
    if [ "$USE_SOURCE_SHEETS" = "true" ]; then
        echo "  Config Source:           Google Sheets ($CONFIG_SHEET_ID)"
    else
        echo "  Config Source:           YAML"
    fi
    echo "  Allow Unknown Columns:   $ALLOW_UNKNOWN_COLUMNS"
    echo "  Schema Migration Mode:   $SCHEMA_MIGRATION_MODE"
    echo ""
    read -p "Proceed with deployment? (yes/no): " confirm
    if [ "$confirm" != "yes" ]; then
        echo "Deployment cancelled."
        exit 0
    fi
    
    enable_apis
    build_and_deploy
    print_summary
}

main "$@"