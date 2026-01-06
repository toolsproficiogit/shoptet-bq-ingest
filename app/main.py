#!/usr/bin/env python3
"""
CSV → BigQuery Cloud Run Service (Multi-pipeline with Google Sheets support)

Optimized version with:
- Streaming CSV processing (low memory)
- Configurable deduplication modes
- Proper upsert logic (MERGE statement)
- Error isolation per pipeline
- Data type consistency (DATETIME → TIMESTAMP)
- Google Sheets and YAML configuration support
"""

import csv
import io
import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import yaml
from flask import Flask
from google.cloud import bigquery
from google.cloud.exceptions import BadRequest

# ======================================================================
# CONFIGURATION
# ======================================================================

app = Flask(__name__)

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL)
log = logging.getLogger(__name__)

# BigQuery
BQ_LOCATION = os.getenv("BQ_LOCATION", "EU")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "1048576"))

# Deduplication modes
DEDUPE_MODES = {"no_dedupe", "auto_dedupe", "full_dedupe"}
DEFAULT_DEDUPE_MODE = "auto_dedupe"

# ======================================================================
# UTILITY FUNCTIONS
# ======================================================================

def _choose_keys(field_names: List[str]) -> Tuple[str, ...]:
    """Determine key fields for MERGE based on available columns."""
    candidates = ["id", "product_id", "order_id", "customer_id", "sku"]
    for cand in candidates:
        if cand in field_names:
            return (cand,)
    # Fallback: use first field if no standard key found
    return (field_names[0],) if field_names else ("id",)


def _coerce_for_json(
    rows: List[Dict[str, Any]], 
    schema_def: Optional[List[Dict[str, Any]]] = None
) -> List[Dict[str, Any]]:
    """
    Coerce row data to JSON-serializable format, enforcing types based on schema.
    This ensures that STRING fields remain strings even if they contain numeric values.
    """
    out: List[Dict[str, Any]] = []
    
    # Build a map of field names to their expected types
    string_fields = set()
    if schema_def:
        for field in schema_def:
            if field.get("type", "").upper() == "STRING":
                string_fields.add(field.get("name"))
    
    for r in rows:
        rr = dict(r)
        
        # Enforce STRING type for fields that should be strings
        # This prevents JSON serialization from converting "123" to 123
        for field_name in string_fields:
            if field_name in rr and rr[field_name] is not None:
                # Ensure the value is a string
                rr[field_name] = str(rr[field_name])
        
        out.append(rr)
    return out


def is_table_empty(bq: bigquery.Client, table_id: str) -> bool:
    """Check if a BigQuery table is empty."""
    try:
        table = bq.get_table(table_id)
        return table.num_rows == 0
    except Exception:
        return True


def ensure_table_with_schema(
    bq: bigquery.Client,
    table_id: str,
    schema: List[bigquery.SchemaField],
) -> bigquery.Table:
    """Ensure table exists with the given schema."""
    try:
        return bq.get_table(table_id)
    except Exception:
        table = bigquery.Table(table_id, schema=schema)
        return bq.create_table(table)


def bq_fields_from_schema(schema_def: List[Dict[str, Any]]) -> List[bigquery.SchemaField]:
    """Convert schema definition to BigQuery SchemaField objects."""
    fields = []
    for field_def in schema_def:
        name = field_def.get("name", "")
        bq_type = field_def.get("type", "STRING").upper()
        mode = field_def.get("mode", "NULLABLE").upper()
        
        # Normalize DATETIME to TIMESTAMP for BigQuery compatibility
        if bq_type == "DATETIME":
            bq_type = "TIMESTAMP"
        
        fields.append(bigquery.SchemaField(name, bq_type, mode=mode))
    
    return fields


def filter_by_last_days(
    rows: List[Dict[str, Any]],
    days: int,
    date_field: str = "date"
) -> List[Dict[str, Any]]:
    """Keep only rows from last N days."""
    if not rows or days <= 0:
        return rows
    
    cutoff = datetime.now() - timedelta(days=days)
    result = []
    
    for row in rows:
        dt = row.get(date_field)
        if dt is None:
            continue
        if isinstance(dt, datetime) and dt >= cutoff:
            result.append(row)
        elif isinstance(dt, str):
            try:
                dt_obj = datetime.fromisoformat(dt.replace("Z", "+00:00"))
                if dt_obj >= cutoff:
                    result.append(row)
            except Exception:
                result.append(row)
        else:
            result.append(row)
    
    return result


def apply_dedupe(
    rows: List[Dict[str, Any]],
    dedupe_mode: str,
    key_fields: Optional[Tuple[str, ...]] = None
) -> List[Dict[str, Any]]:
    """Apply deduplication based on mode."""
    if dedupe_mode == "no_dedupe":
        return rows
    
    if dedupe_mode == "full_dedupe":
        # Remove completely identical rows
        seen = set()
        result = []
        for row in rows:
            row_tuple = tuple(sorted(row.items()))
            if row_tuple not in seen:
                seen.add(row_tuple)
                result.append(row)
        return result
    
    if dedupe_mode == "auto_dedupe":
        # Remove duplicates based on key fields
        if not key_fields:
            key_fields = _choose_keys(list(rows[0].keys()) if rows else [])
        
        seen = {}
        for row in rows:
            key_val = tuple(row.get(k) for k in key_fields)
            seen[key_val] = row
        
        return list(seen.values())
    
    return rows


def load_to_staging_batched(
    bq: bigquery.Client,
    rows: List[Dict[str, Any]],
    target_table: str,
    schema_fields: List[bigquery.SchemaField],
    location: str,
    schema_def: Optional[List[Dict[str, Any]]] = None,
    batch_size: int = BATCH_SIZE,
) -> Optional[str]:
    """Load rows to a staging table in batches."""
    if not rows:
        return None
    
    dataset = ".".join(target_table.split(".")[:2])
    staging = f"{dataset}._stg_{int(time.time())}"
    
    bq.create_table(bigquery.Table(staging, schema=schema_fields))
    
    log.info("Loading %d rows to staging table %s with schema enforcement", len(rows), staging)
    
    # Create job config with explicit schema
    job_config = bigquery.LoadJobConfig()
    job_config.schema = schema_fields
    job_config.schema_update_options = [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
    
    # Load in batches
    total_loaded = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        json_rows = _coerce_for_json(batch, schema_def=schema_def)
        job = bq.load_table_from_json(
            json_rows, 
            staging, 
            location=location,
            job_config=job_config
        )
        job.result()
        total_loaded += len(json_rows)
        log.info("Loaded batch %d rows to staging %s (total: %d)", len(json_rows), staging, total_loaded)
    
    log.info("Completed loading %d rows to staging %s", total_loaded, staging)
    return staging


def merge_staging(
    bq: bigquery.Client,
    staging_table: str,
    target_table: str,
    dedupe_mode: str,
    location: str,
) -> int:
    """
    Merge staging table to target using MERGE statement.
    This implements upsert logic: UPDATE existing rows (by key), INSERT new rows.
    Preserves full history in target table while updating recent data from staging.
    """
    try:
        # Get schema information from both tables
        tgt = bq.get_table(target_table)
        stg = bq.get_table(staging_table)
        
        target_fields = [f.name for f in tgt.schema]
        staging_fields = [f.name for f in stg.schema]
        
        # Determine key fields for MERGE (id, product_id, order_id, etc.)
        key_fields = _choose_keys(target_fields)
        key_set = set(key_fields)
        
        # Common fields (excluding keys) to update
        common_fields = [f for f in target_fields if f in staging_fields and f not in key_set]
        
        # Build MERGE statement
        select_cols = list(key_fields) + common_fields
        select_sql = ", ".join(select_cols)
        
        # UPDATE clause: set all common fields
        update_sets = ", ".join([f"T.{f} = S.{f}" for f in common_fields]) if common_fields else ""
        update_clause = f"WHEN MATCHED THEN UPDATE SET {update_sets}" if update_sets else ""
        
        # INSERT clause: insert keys and common fields
        insert_cols = list(key_fields) + common_fields
        insert_fields = ", ".join(insert_cols)
        insert_values = ", ".join([f"S.{f}" for f in insert_cols])
        
        # ON clause: match by all key fields
        on_clause = " AND ".join([f"T.{k} = S.{k}" for k in key_fields])
        
        query = f"""
        MERGE `{target_table}` T
        USING (SELECT {select_sql} FROM `{staging_table}`) S
        ON {on_clause}
        {update_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_fields}) VALUES ({insert_values});
        """
        
        log.info("Executing MERGE for %s with key fields: %s", target_table, key_fields)
        bq.query(query, location=location).result()
        log.info("MERGE completed for %s", target_table)
        
    except Exception as e:
        log.exception("Error during merge for %s: %s", target_table, e)
        raise
    finally:
        # Clean up staging table
        try:
            bq.delete_table(staging_table, not_found_ok=True)
            log.info("Deleted staging table %s", staging_table)
        except Exception as e:
            log.warning("Failed to delete staging table %s: %s", staging_table, e)
    
    return 0


def schema_from_config(
    pipeline_cfg: Dict[str, Any],
    schema_lib: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    """Get schema for a pipeline from config or library."""
    if pipeline_cfg.get("schema"):
        base = list(pipeline_cfg["schema"])
    else:
        export_type = pipeline_cfg.get("export_type")
        if export_type and export_type in schema_lib:
            base = list(schema_lib[export_type])
        else:
            base = []
    
    # Add auto-generated columns
    base.append({"name": "identifier", "type": "STRING", "mode": "NULLABLE"})
    base.append({"name": "date_only", "type": "DATE", "mode": "NULLABLE"})
    
    return base


# ======================================================================
# CONFIGURATION PROVIDERS
# ======================================================================

class ConfigProvider:
    """Base class for configuration providers."""
    
    def get_pipelines(self) -> List[Dict[str, Any]]:
        raise NotImplementedError
    
    def get_schema_library(self) -> Dict[str, List[Dict[str, Any]]]:
        raise NotImplementedError


class YAMLConfigProvider(ConfigProvider):
    """Load configuration from YAML files in GCS."""
    
    def __init__(self, config_url: str, schema_url: str):
        self.config_url = config_url
        self.schema_url = schema_url
    
    def get_pipelines(self) -> List[Dict[str, Any]]:
        try:
            import requests
            resp = requests.get(self.config_url, timeout=10)
            resp.raise_for_status()
            data = yaml.safe_load(resp.text)
            return data.get("pipelines", [])
        except Exception as e:
            log.exception("Failed to load YAML config: %s", e)
            return []
    
    def get_schema_library(self) -> Dict[str, List[Dict[str, Any]]]:
        try:
            import requests
            resp = requests.get(self.schema_url, timeout=10)
            resp.raise_for_status()
            data = yaml.safe_load(resp.text)
            return data.get("schemas", {})
        except Exception as e:
            log.exception("Failed to load schema library: %s", e)
            return {}


class GoogleSheetsConfigProvider(ConfigProvider):
    """Load configuration from Google Sheets."""
    
    def __init__(self, sheet_id: str):
        self.sheet_id = sheet_id
    
    def get_pipelines(self) -> List[Dict[str, Any]]:
        try:
            import gspread
            from google.auth import default
            
            creds, _ = default()
            gc = gspread.authorize(creds)
            sheet = gc.open_by_key(self.sheet_id)
            
            # Get Pipeline_Config worksheet
            ws = sheet.worksheet("Pipeline_Config")
            records = ws.get_all_records()
            
            pipelines = []
            for record in records:
                if record.get("pipeline_id"):
                    pipelines.append(record)
            
            return pipelines
        except Exception as e:
            log.exception("Failed to load Google Sheets config: %s", e)
            return []
    
    def get_schema_library(self) -> Dict[str, List[Dict[str, Any]]]:
        try:
            import gspread
            from google.auth import default
            
            creds, _ = default()
            gc = gspread.authorize(creds)
            sheet = gc.open_by_key(self.sheet_id)
            
            # Get Schema_Config worksheet
            ws = sheet.worksheet("Schema_Config")
            records = ws.get_all_records()
            
            schemas = {}
            for record in records:
                export_type = record.get("export_type")
                if export_type:
                    if export_type not in schemas:
                        schemas[export_type] = []
                    schemas[export_type].append(record)
            
            return schemas
        except Exception as e:
            log.exception("Failed to load schema library from Sheets: %s", e)
            return {}


def get_config_provider() -> ConfigProvider:
    """Get the appropriate configuration provider."""
    use_sheets = os.getenv("USE_SOURCE_SHEETS", "FALSE").upper() == "TRUE"
    
    if use_sheets:
        sheet_id = os.getenv("CONFIG_SHEET_ID")
        if not sheet_id:
            raise ValueError("CONFIG_SHEET_ID env var required when USE_SOURCE_SHEETS=TRUE")
        return GoogleSheetsConfigProvider(sheet_id)
    else:
        config_url = os.getenv("CONFIG_URL")
        schema_url = os.getenv("SCHEMA_URL")
        if not config_url or not schema_url:
            raise ValueError("CONFIG_URL and SCHEMA_URL env vars required when USE_SOURCE_SHEETS=FALSE")
        return YAMLConfigProvider(config_url, schema_url)


# ======================================================================
# PIPELINE PROCESSING
# ======================================================================

def process_pipeline(
    p: Dict[str, Any],
    schema_lib: Dict[str, List[Dict[str, Any]]],
    allow_unknown: bool = False,
) -> Dict[str, Any]:
    """Process a single pipeline with error isolation."""
    pipeline_id = p.get("id") or "unknown"
    csv_url = p.get("csv_url")
    table_id = p.get("bq_table_id")
    
    if not csv_url or not table_id:
        return {
            "pipeline": pipeline_id,
            "status": "error",
            "message": "Missing csv_url or bq_table_id",
        }
    
    load_mode = (p.get("load_mode") or os.getenv("LOAD_MODE", "auto")).lower()
    window_days = int(p.get("window_days", os.getenv("WINDOW_DAYS", 30)))
    dedupe_mode = (p.get("dedupe_mode") or os.getenv("DEDUPE_MODE", DEFAULT_DEDUPE_MODE)).lower()
    
    # Validate dedupe mode
    if dedupe_mode not in DEDUPE_MODES:
        log.warning("Invalid dedupe_mode '%s' for pipeline %s, using default", dedupe_mode, pipeline_id)
        dedupe_mode = DEFAULT_DEDUPE_MODE
    
    schema_def = schema_from_config(p, schema_lib)
    bq_fields = bq_fields_from_schema(schema_def)
    
    try:
        bq_cli = bigquery.Client()
        table = ensure_table_with_schema(bq_cli, table_id, bq_fields)
        
        # Determine effective load mode
        if load_mode not in {"auto", "full", "window"}:
            load_mode = "auto"
        if load_mode == "auto":
            effective_mode = "full" if is_table_empty(bq_cli, table_id) else "window"
        else:
            effective_mode = load_mode
        
        log.info("Processing pipeline %s with mode=%s, window_days=%d", pipeline_id, effective_mode, window_days)
        
        # Fetch CSV
        try:
            import requests
            resp = requests.get(csv_url, timeout=300)
            resp.raise_for_status()
            csv_text = resp.text
        except Exception as e:
            log.exception("Failed to fetch CSV for pipeline %s: %s", pipeline_id, e)
            return {
                "pipeline": pipeline_id,
                "table": table_id,
                "status": "error_fetch",
                "message": str(e),
            }
        
        # Parse CSV
        rows = []
        try:
            reader = csv.DictReader(io.StringIO(csv_text), delimiter=";")
            for row_dict in reader:
                if not row_dict or all(v is None or v == "" for v in row_dict.values()):
                    continue
                
                # Add identifier and date_only
                row_dict["identifier"] = pipeline_id
                if "date" in row_dict:
                    try:
                        dt = datetime.fromisoformat(row_dict["date"].replace("Z", "+00:00"))
                        row_dict["date_only"] = dt.date()
                    except Exception:
                        pass
                
                # Filter by window if needed
                if effective_mode == "window":
                    dt = row_dict.get("date")
                    if dt:
                        try:
                            dt_obj = datetime.fromisoformat(str(dt).replace("Z", "+00:00"))
                            cutoff = datetime.now() - timedelta(days=window_days)
                            if dt_obj < cutoff:
                                continue
                        except Exception:
                            pass
                
                rows.append(row_dict)
        
        except Exception as e:
            log.exception("Failed to parse CSV for pipeline %s: %s", pipeline_id, e)
            return {
                "pipeline": pipeline_id,
                "table": table_id,
                "status": "error_parse",
                "message": str(e),
            }
        
        if not rows:
            return {
                "pipeline": pipeline_id,
                "table": table_id,
                "status": "success",
                "rows_loaded": 0,
                "message": "No rows to load",
            }
        
        # Apply deduplication
        if dedupe_mode != "no_dedupe":
            key_fields = _choose_keys(list(rows[0].keys()) if rows else [])
            rows = apply_dedupe(rows, dedupe_mode, key_fields)
        
        # Load to staging
        staging = load_to_staging_batched(bq_cli, rows, table_id, bq_fields, BQ_LOCATION, schema_def=schema_def)
        
        if not staging:
            return {
                "pipeline": pipeline_id,
                "table": table_id,
                "status": "success",
                "rows_loaded": 0,
                "message": "No rows to load",
            }
        
        # Merge staging to target
        merge_staging(bq_cli, staging, table_id, dedupe_mode, BQ_LOCATION)
        
        return {
            "pipeline": pipeline_id,
            "table": table_id,
            "status": "success",
            "rows_loaded": len(rows),
        }
    
    except Exception as e:
        log.exception("Pipeline %s failed: %s", pipeline_id, e)
        return {
            "pipeline": pipeline_id,
            "table": table_id,
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__,
        }


# ======================================================================
# FLASK HTTP ENDPOINT
# ======================================================================

@app.route("/", methods=["GET", "POST"])
def trigger():
    """HTTP endpoint for Cloud Run trigger."""
    try:
        # Get configuration
        provider = get_config_provider()
        pipelines = provider.get_pipelines()
        schema_lib = provider.get_schema_library()
        
        allow_unknown = os.getenv("ALLOW_UNKNOWN_COLUMNS", "false").lower() == "true"
        
        # Process each pipeline
        results = []
        for pipeline in pipelines:
            if pipeline.get("active", True) is False:
                continue
            
            result = process_pipeline(pipeline, schema_lib, allow_unknown)
            results.append(result)
        
        return {
            "status": "ok",
            "pipelines_processed": len(results),
            "results": results,
        }, 200
    
    except Exception as e:
        log.exception("Service error: %s", e)
        return {
            "status": "error",
            "message": str(e),
        }, 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)