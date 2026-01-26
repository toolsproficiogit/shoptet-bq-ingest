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
- Schema validation and migration strategy
- Full parser support (decimal_comma, datetime, date_only, etc.)
"""

import csv
import io
import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

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

# Schema migration strategy
SCHEMA_MIGRATION_MODE = os.getenv("SCHEMA_MIGRATION_MODE", "strict").lower()
VALID_SCHEMA_MODES = {"strict", "auto_migrate", "recreate"}

# Deduplication modes
DEDUPE_MODES = {"no_dedupe", "auto_dedupe", "full_dedupe"}
DEFAULT_DEDUPE_MODE = "auto_dedupe"

# Data types
DATA_TYPES = {"time_series", "snapshot"}
DEFAULT_DATA_TYPE = "time_series"

# Snapshot retention modes (only for data_type="snapshot")
SNAPSHOT_RETENTION_MODES = {"latest_only", "all_snapshots", "daily_latest"}
DEFAULT_SNAPSHOT_RETENTION = "daily_latest"

# ======================================================================
# PARSER FUNCTIONS
# ======================================================================

def decimal_comma_to_float(s: Optional[str]) -> Optional[float]:
    """Parse decimal number with comma as decimal separator (Czech Shoptet format).
    
    Shoptet CSV uses comma as decimal separator with NO thousands separator.
    Examples: 3021,29 -> 3021.29, 8,54 -> 8.54, 1585 -> 1585.0
    """
    if s is None:
        return None
    
    s = str(s).strip().strip('"').strip("'")
    if s == "" or s.lower() in {"na", "nan", "null"}:
        return None
    
    # Simple approach: just replace comma with dot
    # Shoptet format NEVER has thousands separators
    normalized = s.replace(",", ".")
    
    try:
        return float(normalized)
    except ValueError:
        return None


def parse_datetime(s: Optional[Union[str, datetime]]) -> Optional[datetime]:
    """Parse datetime string in common formats."""
    if s is None:
        return None
    if isinstance(s, datetime):
        return s
    s = str(s).strip().strip('"').strip("'")
    if not s:
        return None
    # Try common datetime formats
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%d.%m.%Y %H:%M:%S", "%d.%m.%Y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    # Try ISO format with timezone
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        pass
    return None


def parse_date_only(s: Optional[Union[str, datetime]]) -> Optional[str]:
    """Parse date string and return as ISO date string (YYYY-MM-DD)."""
    dt = s if isinstance(s, datetime) else parse_datetime(s)
    return dt.date().isoformat() if dt else None


# Parser registry - maps parse type names to parser functions
PARSERS = {
    "string": lambda v: (str(v).strip().strip('"').strip("'") if v not in (None, "") else None),
    "float": lambda v: float(v) if v not in (None, "", "null", "NaN") else None,
    "int": lambda v: int(float(v)) if v not in (None, "", "null", "NaN") else None,
    "bool": lambda v: (str(v).strip().lower() in {"1", "true", "t", "yes", "y"}) if v not in (None, "") else None,
    "datetime": parse_datetime,
    "date_only": parse_date_only,
    "decimal_comma": decimal_comma_to_float,
}

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


def build_row_from_record(
    rec: Dict[str, Any], 
    schema_def: List[Dict[str, Any]], 
    pipeline_id: str
) -> Dict[str, Any]:
    """
    Build a row from a CSV record using the schema definition.
    Applies appropriate parsers to each field based on the 'parse' type.
    Only processes fields that have a 'source' defined (CSV fields).
    Skips computed fields like ingestion_time and date_only.
    """
    row: Dict[str, Any] = {}
    
    for field_def in schema_def:
        name = field_def.get("name")
        source = field_def.get("source")
        parser_key = field_def.get("parse", "string")
        
        # Skip fields without source - these are computed/injected fields
        if not source:
            continue
        
        # Get the parser function for this field type
        parser = PARSERS.get(parser_key, PARSERS["string"])
        
        # Apply parser to the source field value
        raw_value = rec.get(source)
        parsed_value = parser(raw_value)
        row[name] = parsed_value
        
        # Debug logging for all fields (not just decimal_comma)
        if raw_value is not None and raw_value != "":
            log.debug(f"Pipeline {pipeline_id}: Field {name} (source={source}, parse={parser_key}) - raw={repr(raw_value)} -> parsed={repr(parsed_value)}")
        elif parsed_value is None:
            log.debug(f"Pipeline {pipeline_id}: Field {name} (source={source}, parse={parser_key}) - raw={repr(raw_value)} -> PARSED AS NULL")
    
    # Add identifier
    row["identifier"] = pipeline_id
    
    return row


def _coerce_for_json(
    rows: List[Dict[str, Any]], 
    schema_def: Optional[List[Dict[str, Any]]] = None,
    pipeline_id: str = ""
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
        
        # Convert datetime objects to ISO format strings for JSON serialization
        for key, value in rr.items():
            if isinstance(value, datetime):
                rr[key] = value.isoformat()
        
        out.append(rr)
    return out


def is_table_empty(bq: bigquery.Client, table_id: str) -> bool:
    """Check if a BigQuery table is empty."""
    try:
        table = bq.get_table(table_id)
        return table.num_rows == 0
    except Exception:
        return True


def _normalize_schema_field(field: bigquery.SchemaField) -> Dict[str, Any]:
    """Convert BigQuery SchemaField to dict for comparison."""
    return {
        "name": field.name,
        "type": field.field_type,
        "mode": field.mode,
    }


def _schemas_match(
    existing_schema: List[bigquery.SchemaField],
    expected_schema: List[bigquery.SchemaField],
) -> bool:
    """Check if two schemas match (ignoring descriptions)."""
    if len(existing_schema) != len(expected_schema):
        return False
    
    existing_dict = {f.name: _normalize_schema_field(f) for f in existing_schema}
    expected_dict = {f.name: _normalize_schema_field(f) for f in expected_schema}
    
    return existing_dict == expected_dict


def _get_schema_diff(
    existing_schema: List[bigquery.SchemaField],
    expected_schema: List[bigquery.SchemaField],
) -> Dict[str, Any]:
    """Get the differences between two schemas."""
    existing_names = {f.name for f in existing_schema}
    expected_names = {f.name for f in expected_schema}
    
    added_fields = expected_names - existing_names
    removed_fields = existing_names - expected_names
    
    # Check for type changes
    changed_fields = {}
    for field in expected_schema:
        if field.name in existing_names:
            existing_field = next(f for f in existing_schema if f.name == field.name)
            if (existing_field.field_type != field.field_type or 
                existing_field.mode != field.mode):
                changed_fields[field.name] = {
                    "existing": f"{existing_field.field_type} ({existing_field.mode})",
                    "expected": f"{field.field_type} ({field.mode})",
                }
    
    return {
        "added": sorted(added_fields),
        "removed": sorted(removed_fields),
        "changed": changed_fields,
    }


def validate_or_migrate_schema(
    bq: bigquery.Client,
    table_id: str,
    expected_schema: List[bigquery.SchemaField],
    migration_mode: str,
) -> None:
    """
    Validate or migrate table schema based on migration mode.
    
    Modes:
    - strict: Fail if schema doesn't match (default)
    - auto_migrate: Add new fields, fail on removals/changes
    - recreate: Drop and recreate table (data loss!)
    """
    try:
        existing_table = bq.get_table(table_id)
        existing_schema = existing_table.schema
        
        # Check if schemas match
        if _schemas_match(existing_schema, expected_schema):
            log.info("Schema for %s matches expected schema", table_id)
            return
        
        # Schemas don't match - handle based on mode
        diff = _get_schema_diff(existing_schema, expected_schema)
        
        if migration_mode == "strict":
            error_msg = f"Schema mismatch for {table_id}:\n"
            if diff["added"]:
                error_msg += f"  Added fields: {', '.join(diff['added'])}\n"
            if diff["removed"]:
                error_msg += f"  Removed fields: {', '.join(diff['removed'])}\n"
            if diff["changed"]:
                error_msg += f"  Changed fields: {', '.join(diff['changed'].keys())}\n"
            error_msg += "Set SCHEMA_MIGRATION_MODE=auto_migrate or recreate to handle schema changes."
            raise ValueError(error_msg)
        
        elif migration_mode == "auto_migrate":
            # Only allow adding new fields
            if diff["removed"] or diff["changed"]:
                error_msg = f"Auto-migrate cannot handle removals or changes for {table_id}:\n"
                if diff["removed"]:
                    error_msg += f"  Removed fields: {', '.join(diff['removed'])}\n"
                if diff["changed"]:
                    error_msg += f"  Changed fields: {', '.join(diff['changed'].keys())}\n"
                error_msg += "Manually handle these changes or use SCHEMA_MIGRATION_MODE=recreate."
                raise ValueError(error_msg)
            
            # Add new fields
            if diff["added"]:
                log.info("Adding new fields to %s: %s", table_id, diff["added"])
                new_schema = list(existing_schema)
                for field in expected_schema:
                    if field.name in diff["added"]:
                        new_schema.append(field)
                
                existing_table.schema = new_schema
                bq.update_table(existing_table, ["schema"])
                log.info("Successfully added fields to %s", table_id)
        
        elif migration_mode == "recreate":
            log.warning("RECREATING table %s - DATA WILL BE LOST!", table_id)
            bq.delete_table(table_id, not_found_ok=True)
            new_table = bigquery.Table(table_id, schema=expected_schema)
            bq.create_table(new_table)
            log.info("Successfully recreated table %s", table_id)
        
        else:
            log.warning("Unknown schema migration mode: %s, using strict", migration_mode)
            raise ValueError(f"Unknown schema migration mode: {migration_mode}")
    
    except Exception as e:
        if "Not found" in str(e) or "404" in str(e):
            # Table doesn't exist, will be created
            log.info("Table %s does not exist, will be created", table_id)
        else:
            raise


def ensure_table_with_schema(
    bq: bigquery.Client,
    table_id: str,
    schema: List[bigquery.SchemaField],
    migration_mode: str = "strict",
) -> bigquery.Table:
    """Ensure table exists with the given schema, handling migrations."""
    try:
        table = bq.get_table(table_id)
        # Table exists, validate/migrate schema
        validate_or_migrate_schema(bq, table_id, schema, migration_mode)
        return bq.get_table(table_id)
    except Exception as e:
        if "Not found" in str(e) or "404" in str(e):
            # Table doesn't exist, create it
            log.info("Creating new table %s", table_id)
            table = bigquery.Table(table_id, schema=schema)
            return bq.create_table(table)
        else:
            raise


def bq_fields_from_schema(schema_def: List[Dict[str, Any]]) -> List[bigquery.SchemaField]:
    """Convert schema definition to BigQuery SchemaField objects."""
    fields = []
    for field_def in schema_def:
        name = field_def.get("name", "")
        bq_type = field_def.get("type", "STRING").upper()
        mode = field_def.get("mode", "NULLABLE").upper()
        description = field_def.get("description", "")
        
        # Normalize DATETIME to TIMESTAMP for BigQuery compatibility
        if bq_type == "DATETIME":
            bq_type = "TIMESTAMP"
        
        fields.append(bigquery.SchemaField(
            name, 
            bq_type, 
            mode=mode,
            description=description
        ))
    
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


def inject_ingestion_timestamp(
    rows: List[Dict[str, Any]],
    timestamp_column: str = "ingestion_timestamp"
) -> List[Dict[str, Any]]:
    """Inject ingestion timestamp into all rows for snapshot data."""
    if not rows:
        return rows
    
    now = datetime.now()
    for row in rows:
        row[timestamp_column] = now
    
    log.debug(f"Injected timestamp column '{timestamp_column}' with value {repr(now)} to {len(rows)} rows")
    
    return rows


def apply_snapshot_retention(
    rows: List[Dict[str, Any]],
    retention_mode: str,
    key_fields: Optional[Tuple[str, ...]] = None,
    timestamp_column: str = "ingestion_timestamp"
) -> List[Dict[str, Any]]:
    """
    Apply snapshot-specific retention logic.
    
    Modes:
    - latest_only: Keep only the latest snapshot per key (no history)
    - all_snapshots: Keep all snapshots (for troubleshooting/sub-day comparisons)
    - daily_latest: Keep only latest snapshot per day per key (default, balances history with storage)
    """
    if not rows or retention_mode == "all_snapshots":
        return rows
    
    if not key_fields:
        key_fields = _choose_keys(list(rows[0].keys()) if rows else [])
    
    if retention_mode == "latest_only":
        # Keep only the latest snapshot per key (overall)
        seen = {}
        for row in rows:
            key_val = tuple(row.get(k) for k in key_fields)
            # Keep if we haven't seen this key, or if this row is newer
            if key_val not in seen:
                seen[key_val] = row
            else:
                existing_ts = seen[key_val].get(timestamp_column)
                new_ts = row.get(timestamp_column)
                if new_ts and existing_ts and new_ts > existing_ts:
                    seen[key_val] = row
        return list(seen.values())
    
    elif retention_mode == "daily_latest":
        # Keep only the latest snapshot per day per key
        seen = {}
        for row in rows:
            key_val = tuple(row.get(k) for k in key_fields)
            ts = row.get(timestamp_column)
            
            # Extract date from timestamp for grouping
            if isinstance(ts, datetime):
                date_key = ts.date()
            else:
                date_key = datetime.now().date()
            
            composite_key = (key_val, date_key)
            
            # Keep if we haven't seen this key+date, or if this row is newer
            if composite_key not in seen:
                seen[composite_key] = row
            else:
                existing_ts = seen[composite_key].get(timestamp_column)
                if ts and existing_ts and ts > existing_ts:
                    seen[composite_key] = row
        
        return list(seen.values())
    
    return rows


def apply_dedupe(
    rows: List[Dict[str, Any]],
    dedupe_mode: str,
    key_fields: Optional[Tuple[str, ...]] = None
) -> List[Dict[str, Any]]:
    """Apply deduplication based on mode."""
    if dedupe_mode == "no_dedupe":
        return rows
    
    if dedupe_mode == "full_dedupe":
        # For full_dedupe: keep only the LAST occurrence of each key
        # This is important for MERGE to work (one source row per target row)
        if not key_fields:
            key_fields = _choose_keys(list(rows[0].keys()) if rows else [])
        
        seen = {}
        for row in rows:
            key_val = tuple(row.get(k) for k in key_fields)
            seen[key_val] = row  # Keep last occurrence
        
        return list(seen.values())
    
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
        
        # Debug logging for first row
        if i == 0 and json_rows:
            first_row = json_rows[0]
            for key, value in first_row.items():
                if isinstance(value, float):
                    log.debug(f"Staging load - Field {key}: {value} (type: {type(value).__name__})")
        
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
    key_fields: Optional[Tuple[str, ...]] = None,
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
        # Use configured key_fields if provided, otherwise auto-detect
        if not key_fields:
            key_fields = _choose_keys(target_fields)
        key_set = set(key_fields)
        
        log.info("Using key fields for MERGE: %s", key_fields)
        
        # Common fields (excluding keys) to update
        common_fields = [f for f in target_fields if f in staging_fields and f not in key_set]
        
        # Build MERGE statement with defensive deduplication
        # Use ROW_NUMBER to keep only the LAST occurrence of each key in staging
        select_cols = list(key_fields) + common_fields
        select_sql = ", ".join(select_cols)
        
        # Add ROW_NUMBER to deduplicate by key, keeping the last row
        row_num_partition = ", ".join(key_fields) if key_fields else "1"
        dedup_sql = f"""SELECT {select_sql}
        FROM (
            SELECT {select_sql},
                   ROW_NUMBER() OVER (PARTITION BY {row_num_partition} ORDER BY 1 DESC) as rn
            FROM `{staging_table}`
        )
        WHERE rn = 1"""
        
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
        USING ({dedup_sql}) S
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


def extract_key_fields(
    pipeline_cfg: Dict[str, Any],
    schema_lib: Dict[str, List[Dict[str, Any]]],
) -> Optional[Tuple[str, ...]]:
    """
    Extract key fields from pipeline config or schema library.
    Key fields are used for deduplication and MERGE operations.
    Format: comma-separated field names (e.g., "id,product_id" or "order_id,line_item_id")
    """
    # First check if key_fields is specified in pipeline config
    if pipeline_cfg.get("key_fields"):
        key_str = pipeline_cfg.get("key_fields")
        if isinstance(key_str, str):
            fields = tuple(f.strip() for f in key_str.split(",") if f.strip())
            if fields:
                log.info("Using configured key fields: %s", fields)
                return fields
    
    # Then check schema library for key_fields attribute
    export_type = pipeline_cfg.get("export_type")
    if export_type and export_type in schema_lib:
        schema_list = schema_lib[export_type]
        if schema_list and isinstance(schema_list, list) and len(schema_list) > 0:
            first_schema = schema_list[0]
            if isinstance(first_schema, dict) and first_schema.get("key_fields"):
                key_str = first_schema.get("key_fields")
                if isinstance(key_str, str):
                    fields = tuple(f.strip() for f in key_str.split(",") if f.strip())
                    if fields:
                        log.info("Using key fields from schema library: %s", fields)
                        return fields
    
    # No explicit key fields configured
    log.debug("No explicit key_fields configured for pipeline %s", pipeline_cfg.get("id"))
    return None


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
    base.append({"name": "identifier", "type": "STRING", "mode": "NULLABLE", "parse": "string"})
    base.append({"name": "date_only", "type": "DATE", "mode": "NULLABLE", "parse": "date_only"})
    
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
                    
                    # Map parse_logic to parse for compatibility
                    if "parse_logic" in record and "parse" not in record:
                        record["parse"] = record.pop("parse_logic")
                    
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
    migration_mode: str = "strict",
) -> Dict[str, Any]:
    """Process a single pipeline with error isolation."""
    # Try multiple field names for pipeline ID
    pipeline_id = p.get("id") or p.get("pipeline_id") or p.get("name") or p.get("export_type") or "unknown"
    csv_url = p.get("csv_url")
    table_id = p.get("bq_table_id")
    
    if not csv_url or not table_id:
        log.warning(f"Pipeline {pipeline_id}: Missing csv_url or bq_table_id. Pipeline dict keys: {list(p.keys())}")
        return {
            "pipeline": pipeline_id,
            "status": "error",
            "message": "Missing csv_url or bq_table_id",
        }
    
    load_mode = (p.get("load_mode") or os.getenv("LOAD_MODE", "auto")).lower()
    window_days = int(p.get("window_days", os.getenv("WINDOW_DAYS", 30)))
    dedupe_mode = (p.get("dedupe_mode") or os.getenv("DEDUPE_MODE", DEFAULT_DEDUPE_MODE)).lower()
    data_type = (p.get("data_type") or os.getenv("DATA_TYPE", DEFAULT_DATA_TYPE)).lower()
    add_ingestion_timestamp = str(p.get("add_ingestion_timestamp", "false")).lower() == "true"
    ingestion_timestamp_column = p.get("ingestion_timestamp_column", "ingestion_timestamp")
    snapshot_retention_mode = (p.get("snapshot_retention_mode") or DEFAULT_SNAPSHOT_RETENTION).lower()
    
    # Validate dedupe mode
    if dedupe_mode not in DEDUPE_MODES:
        log.warning("Invalid dedupe_mode '%s' for pipeline %s, using default", dedupe_mode, pipeline_id)
        dedupe_mode = DEFAULT_DEDUPE_MODE
    
    # Validate data type
    if data_type not in DATA_TYPES:
        log.warning("Invalid data_type '%s' for pipeline %s, using default", data_type, pipeline_id)
        data_type = DEFAULT_DATA_TYPE
    
    # Validate snapshot retention mode
    if data_type == "snapshot" and snapshot_retention_mode not in SNAPSHOT_RETENTION_MODES:
        log.warning("Invalid snapshot_retention_mode '%s' for pipeline %s, using default", snapshot_retention_mode, pipeline_id)
        snapshot_retention_mode = DEFAULT_SNAPSHOT_RETENTION
    
    schema_def = schema_from_config(p, schema_lib)
    bq_fields = bq_fields_from_schema(schema_def)
    
    try:
        bq_cli = bigquery.Client()
        table = ensure_table_with_schema(bq_cli, table_id, bq_fields, migration_mode)
        
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
        
        # Parse CSV and build rows using schema parsers
        rows = []
        filtered_count = 0
        try:
            reader = csv.DictReader(io.StringIO(csv_text), delimiter=";")
            for csv_idx, csv_record in enumerate(reader):
                if not csv_record or all(v is None or v == "" for v in csv_record.values()):
                    continue
                
                # Debug: Log the raw CSV record structure for first row
                if csv_idx == 0:
                    log.debug(f"Pipeline {pipeline_id}: Raw CSV record keys: {list(csv_record.keys())}")
                    log.debug(f"Pipeline {pipeline_id}: Raw CSV record sample: {dict(list(csv_record.items())[:5])}")
                    if 'code' in csv_record:
                        log.debug(f"Pipeline {pipeline_id}: CSV 'code' field value: {repr(csv_record['code'])}")
                
                # Build row using schema parsers
                row = build_row_from_record(csv_record, schema_def, pipeline_id)
                
                # Filter by window if needed
                if effective_mode == "window":
                    # Find the date field (could be named 'date' or other timestamp field)
                    dt = None
                    for field_def in schema_def:
                        if field_def.get("type") in {"TIMESTAMP", "DATE", "DATETIME"}:
                            field_name = field_def.get("name")
                            if field_name in row:
                                dt = row.get(field_name)
                                break
                    
                    if dt:
                        if isinstance(dt, datetime):
                            cutoff = datetime.now() - timedelta(days=window_days)
                            if dt < cutoff:
                                filtered_count += 1
                                continue
                        elif isinstance(dt, str):
                            # Try to parse if it's still a string
                            try:
                                dt_obj = parse_datetime(dt)
                                if dt_obj:
                                    cutoff = datetime.now() - timedelta(days=window_days)
                                    if dt_obj < cutoff:
                                        filtered_count += 1
                                        continue
                            except:
                                pass
                
                rows.append(row)
            
            if effective_mode == "window" and filtered_count > 0:
                log.info(f"Pipeline {pipeline_id}: Filtered out {filtered_count} rows older than {window_days} days")
        
        except Exception as e:
            log.exception("Failed to parse CSV for pipeline %s: %s", pipeline_id, e)
            return {
                "pipeline": pipeline_id,
                "table": table_id,
                "status": "error_parse",
                "message": str(e),
            }
        
        if not rows:
            log.info(f"Pipeline {pipeline_id}: No rows to load after filtering (mode={effective_mode}, window_days={window_days})")
            return {
                "pipeline": pipeline_id,
                "table": table_id,
                "status": "success",
                "rows_loaded": 0,
                "message": f"No rows to load (filtered {filtered_count} old rows)" if effective_mode == "window" else "No rows to load",
            }
        
        # Debug: Log first row structure
        if rows:
            first_row = rows[0]
            log.debug(f"Pipeline {pipeline_id}: First row structure (keys={list(first_row.keys())})")
            for key, val in first_row.items():
                log.debug(f"  {key} = {repr(val)}")
        
        # Extract configured key fields for this pipeline
        configured_keys = extract_key_fields(p, schema_lib)
        
        # Handle snapshot data type
        if data_type == "snapshot":
            # Inject ingestion timestamp for snapshot data
            if add_ingestion_timestamp:
                rows = inject_ingestion_timestamp(rows, ingestion_timestamp_column)
                log.info("Pipeline %s: Injected ingestion timestamp column '%s'", pipeline_id, ingestion_timestamp_column)
            
            # Apply snapshot-specific retention logic
            key_fields = configured_keys or _choose_keys(list(rows[0].keys()) if rows else [])
            rows = apply_snapshot_retention(rows, snapshot_retention_mode, key_fields, ingestion_timestamp_column)
            log.info("Pipeline %s: Applied snapshot retention mode '%s' with key fields: %s", pipeline_id, snapshot_retention_mode, key_fields)
        else:
            # Apply regular deduplication for time-series data
            if dedupe_mode != "no_dedupe":
                # Use configured key fields if available, otherwise auto-detect
                key_fields = configured_keys or _choose_keys(list(rows[0].keys()) if rows else [])
                rows = apply_dedupe(rows, dedupe_mode, key_fields)
                log.info("Pipeline %s: Applied %s deduplication with key fields: %s", pipeline_id, dedupe_mode, key_fields)
        
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
        
        # Merge staging to target with configured key fields
        merge_staging(bq_cli, staging, table_id, dedupe_mode, BQ_LOCATION, key_fields=configured_keys)
        
        rows_count = len(rows)
        log.info(f"Pipeline {pipeline_id}: Successfully loaded {rows_count} rows (mode={effective_mode}, filtered {filtered_count} old rows)")
        
        return {
            "pipeline": pipeline_id,
            "table": table_id,
            "status": "success",
            "rows_loaded": rows_count,
            "rows_filtered": filtered_count if effective_mode == "window" else 0,
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
@app.route("/run", methods=["GET", "POST"])
def trigger():
    """HTTP endpoint for Cloud Run trigger."""
    try:
        # Get configuration
        provider = get_config_provider()
        pipelines = provider.get_pipelines()
        schema_lib = provider.get_schema_library()
        
        allow_unknown = os.getenv("ALLOW_UNKNOWN_COLUMNS", "false").lower() == "true"
        migration_mode = SCHEMA_MIGRATION_MODE if SCHEMA_MIGRATION_MODE in VALID_SCHEMA_MODES else "strict"
        
        # Process each pipeline
        results = []
        for pipeline in pipelines:
            if pipeline.get("active", True) is False:
                continue
            
            result = process_pipeline(pipeline, schema_lib, allow_unknown, migration_mode)
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