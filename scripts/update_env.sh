#!/bin/bash
# Update environment variables on a deployed Cloud Run service
# Supports updating: ALLOW_UNKNOWN_COLUMNS, USE_SOURCE_SHEETS, SCHEMA_MIGRATION_MODE, and others

set -e

# ======================================================================
# CONFIGURATION
# ======================================================================

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
SERVICE_NAME="${SERVICE_NAME:-csv-bq-multi}"
REGION="${REGION:-europe-west1}"

# ======================================================================
# FUNCTIONS
# ======================================================================

print_header() {
    echo ""
    echo "╔════════════════════════════════════════════════════════════╗"
    echo "║  Update Cloud Run Service Environment Variables            ║"
    echo "╚════════════════════════════════════════════════════════════╝"
    echo ""
}

show_current_env() {
    echo "Current environment variables:"
    echo ""
    gcloud run services describe "$SERVICE_NAME" \
        --region="$REGION" \
        --project="$PROJECT_ID" \
        --format="table(spec.template.spec.containers[].env[].name,spec.template.spec.containers[].env[].value)" \
        || echo "Error: Could not retrieve current environment variables"
    echo ""
}

get_current_env_vars() {
    gcloud run services describe "$SERVICE_NAME" \
        --region="$REGION" \
        --project="$PROJECT_ID" \
        --format="json" | \
        jq -r '.spec.template.spec.containers[0].env[] | "\(.name)=\(.value)"' 2>/dev/null || echo ""
}

update_single_var() {
    local var_name="$1"
    local var_value="$2"
    
    echo "Updating $var_name to: $var_value"
    
    # Get current env vars
    local current_vars=$(get_current_env_vars)
    
    # Build new env vars string
    local new_vars=""
    local found=false
    
    while IFS= read -r line; do
        if [ -z "$line" ]; then
            continue
        fi
        
        local key="${line%%=*}"
        if [ "$key" = "$var_name" ]; then
            new_vars="$new_vars$var_name=$var_value"
            found=true
        else
            new_vars="$new_vars$line"
        fi
        new_vars="$new_vars,"
    done <<< "$current_vars"
    
    # If variable not found, add it
    if [ "$found" = false ]; then
        new_vars="$new_vars$var_name=$var_value,"
    fi
    
    # Remove trailing comma
    new_vars="${new_vars%,}"
    
    # Update service
    gcloud run services update "$SERVICE_NAME" \
        --region="$REGION" \
        --project="$PROJECT_ID" \
        --set-env-vars="$new_vars" \
        --quiet
    
    echo "✓ $var_name updated successfully"
}

interactive_mode() {
    echo "Select variable to update:"
    echo ""
    echo "1) ALLOW_UNKNOWN_COLUMNS (accept CSVs with unknown columns)"
    echo "2) USE_SOURCE_SHEETS (switch between YAML and Google Sheets)"
    echo "3) SCHEMA_MIGRATION_MODE (schema change handling strategy)"
    echo "4) LOG_LEVEL (logging verbosity)"
    echo "5) BATCH_SIZE (rows per batch)"
    echo "6) DEDUPE_MODE (deduplication strategy)"
    echo "7) WINDOW_DAYS (days to keep in window mode)"
    echo "8) Custom variable"
    echo ""
    read -p "Enter choice (1-8): " choice
    
    case $choice in
        1)
            echo ""
            echo "Current ALLOW_UNKNOWN_COLUMNS:"
            gcloud run services describe "$SERVICE_NAME" \
                --region="$REGION" \
                --project="$PROJECT_ID" \
                --format="value(spec.template.spec.containers[0].env[?name=='ALLOW_UNKNOWN_COLUMNS'].value)" || echo "Not set"
            echo ""
            echo "Options: true, false"
            read -p "Enter value: " value
            update_single_var "ALLOW_UNKNOWN_COLUMNS" "$value"
            ;;
        2)
            echo ""
            echo "Current USE_SOURCE_SHEETS:"
            gcloud run services describe "$SERVICE_NAME" \
                --region="$REGION" \
                --project="$PROJECT_ID" \
                --format="value(spec.template.spec.containers[0].env[?name=='USE_SOURCE_SHEETS'].value)" || echo "Not set"
            echo ""
            echo "Options: true (Google Sheets), false (YAML)"
            read -p "Enter value: " value
            update_single_var "USE_SOURCE_SHEETS" "$value"
            
            if [ "$value" = "true" ]; then
                read -p "Enter CONFIG_SHEET_ID: " sheet_id
                update_single_var "CONFIG_SHEET_ID" "$sheet_id"
            fi
            ;;
        3)
            echo ""
            echo "Current SCHEMA_MIGRATION_MODE:"
            gcloud run services describe "$SERVICE_NAME" \
                --region="$REGION" \
                --project="$PROJECT_ID" \
                --format="value(spec.template.spec.containers[0].env[?name=='SCHEMA_MIGRATION_MODE'].value)" || echo "Not set"
            echo ""
            echo "Options:"
            echo "  strict: Fail if schema doesn't match (default)"
            echo "  auto_migrate: Add new fields, fail on removals/changes"
            echo "  recreate: Drop and recreate table (DATA LOSS!)"
            read -p "Enter value: " value
            
            if [ "$value" = "recreate" ]; then
                echo ""
                echo "WARNING: 'recreate' mode will DELETE all data in the table!"
                read -p "Are you sure? (yes/no): " confirm
                if [ "$confirm" != "yes" ]; then
                    echo "Cancelled."
                    return
                fi
            fi
            
            update_single_var "SCHEMA_MIGRATION_MODE" "$value"
            ;;
        4)
            echo ""
            echo "Current LOG_LEVEL:"
            gcloud run services describe "$SERVICE_NAME" \
                --region="$REGION" \
                --project="$PROJECT_ID" \
                --format="value(spec.template.spec.containers[0].env[?name=='LOG_LEVEL'].value)" || echo "Not set"
            echo ""
            echo "Options: DEBUG, INFO, WARNING, ERROR"
            read -p "Enter value: " value
            update_single_var "LOG_LEVEL" "$value"
            ;;
        5)
            echo ""
            echo "Current BATCH_SIZE:"
            gcloud run services describe "$SERVICE_NAME" \
                --region="$REGION" \
                --project="$PROJECT_ID" \
                --format="value(spec.template.spec.containers[0].env[?name=='BATCH_SIZE'].value)" || echo "Not set"
            echo ""
            read -p "Enter value (default 5000): " value
            value=${value:-5000}
            update_single_var "BATCH_SIZE" "$value"
            ;;
        6)
            echo ""
            echo "Current DEDUPE_MODE:"
            gcloud run services describe "$SERVICE_NAME" \
                --region="$REGION" \
                --project="$PROJECT_ID" \
                --format="value(spec.template.spec.containers[0].env[?name=='DEDUPE_MODE'].value)" || echo "Not set"
            echo ""
            echo "Options: no_dedupe, auto_dedupe, full_dedupe"
            read -p "Enter value: " value
            update_single_var "DEDUPE_MODE" "$value"
            ;;
        7)
            echo ""
            echo "Current WINDOW_DAYS:"
            gcloud run services describe "$SERVICE_NAME" \
                --region="$REGION" \
                --project="$PROJECT_ID" \
                --format="value(spec.template.spec.containers[0].env[?name=='WINDOW_DAYS'].value)" || echo "Not set"
            echo ""
            read -p "Enter value (default 30): " value
            value=${value:-30}
            update_single_var "WINDOW_DAYS" "$value"
            ;;
        8)
            read -p "Enter variable name: " var_name
            read -p "Enter variable value: " var_value
            update_single_var "$var_name" "$var_value"
            ;;
        *)
            echo "Invalid choice"
            return 1
            ;;
    esac
}

# ======================================================================
# MAIN
# ======================================================================

main() {
    print_header
    
    # Check if arguments provided
    if [ $# -eq 0 ]; then
        # Interactive mode
        show_current_env
        interactive_mode
    else
        # Command-line mode
        while [ $# -gt 0 ]; do
            case "$1" in
                --list)
                    show_current_env
                    shift
                    ;;
                --set)
                    shift
                    if [ $# -lt 1 ]; then
                        echo "Error: --set requires a value (VAR=VALUE)"
                        exit 1
                    fi
                    var_assignment="$1"
                    var_name="${var_assignment%%=*}"
                    var_value="${var_assignment#*=}"
                    update_single_var "$var_name" "$var_value"
                    shift
                    ;;
                *)
                    echo "Unknown option: $1"
                    echo "Usage: $0 [--list] [--set VAR=VALUE]"
                    exit 1
                    ;;
            esac
        done
    fi
    
    echo ""
    echo "✓ Done"
}

main "$@"