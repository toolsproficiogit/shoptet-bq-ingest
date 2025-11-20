import csv
import io
import os
import re
import sys
import json
import time
import logging
from datetime import datetime, timedelta, date
from typing import List, Dict, Any, Tuple, Optional, Union

import requests
import yaml
from flask import Flask, jsonify, request
from google.cloud import bigquery, storage
from google.api_core.exceptions import NotFound, BadRequest

# ----------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("shoptet-bq")

app = Flask(__name__)

# ----------------------------------------------------------------------
# Helpers: GCS, encoding
# ----------------------------------------------------------------------
def parse_gcs_from_https(url: str) -> Optional[Tuple[str, str]]:
    m = re.match(r"^https?://storage\.googleapis\.com/([^/]+)/(.*)$", url)
    return (m.group(1), m.group(2)) if m else None


def _decode_bytes(data: bytes, preferred: Optional[str] = None) -> str:
    """
    Decode CSV bytes robustly. Try:
    - preferred (if provided and != auto/default)
    - utf-8-sig, utf-8, cp1250, windows-1250, iso-8859-2, latin-1
    - finally utf-8 with replacement
    """
    tried = set()
    if preferred and preferred.lower() not in {"auto", "default"}:
        enc = preferred
        tried.add(enc.lower())
        try:
            return data.decode(enc).lstrip("\ufeff")
        except Exception:
            pass

    for enc in ("utf-8-sig", "utf-8", "cp1250", "windows-1250", "iso-8859-2", "latin-1"):
        if enc.lower() in tried:
            continue
        try:
            return data.decode(enc).lstrip("\ufeff")
        except Exception:
            continue

    return data.decode("utf-8", errors="replace").lstrip("\ufeff")


def fetch_text_from_url(url: str, timeout: int = 120, preferred_encoding: Optional[str] = None) -> str:
    """
    Fetch bytes from HTTPS or GCS and decode using robust fallback logic.
    Supports:
      - gs://bucket/path
      - https://storage.googleapis.com/bucket/path
      - generic https://...
    """
    if url.startswith("gs://"):
        bucket_name, blob_name = url[5:].split("/", 1)
        data = storage.Client().bucket(bucket_name).blob(blob_name).download_as_bytes()
        return _decode_bytes(data, preferred=preferred_encoding)

    gcs_pair = parse_gcs_from_https(url)
    if gcs_pair:
        bucket_name, blob_name = gcs_pair
        data = storage.Client().bucket(bucket_name).blob(blob_name).download_as_bytes()
        return _decode_bytes(data, preferred=preferred_encoding)

    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return _decode_bytes(r.content, preferred=preferred_encoding)

# ----------------------------------------------------------------------
# Parsers
# ----------------------------------------------------------------------
def decimal_comma_to_float(s: Optional[str]) -> Optional[float]:
    if s is None:
        return None
    s = s.strip().strip('"').strip("'")
    if s == "" or s.lower() in {"na", "nan", "null"}:
        return None
    normalized = s.replace(".", "").replace(",", ".")
    try:
        return float(normalized)
    except ValueError:
        return None


def parse_datetime(s: Optional[Union[str, datetime]]) -> Optional[datetime]:
    if s is None:
        return None
    if isinstance(s, datetime):
        return s
    s = str(s).strip().strip('"').strip("'")
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def parse_date_only(s: Optional[Union[str, datetime]]) -> Optional[str]:
    dt = s if isinstance(s, datetime) else parse_datetime(s)
    return dt.date().isoformat() if dt else None


PARSERS = {
    "string": lambda v: (str(v).strip().strip('"').strip("'") if v not in (None, "") else None),
    "float": lambda v: float(v) if v not in (None, "", "null", "NaN") else None,
    "int": lambda v: int(float(v)) if v not in (None, "", "null", "NaN") else None,
    "bool": lambda v: (str(v).strip().lower() in {"1", "true", "t", "yes", "y"}) if v not in (None, "") else None,
    "datetime": parse_datetime,
    "date_only": parse_date_only,
    "decimal_comma": decimal_comma_to_float,
}

# ----------------------------------------------------------------------
# CSV reading (Fix #1: skip blank headers)
# ----------------------------------------------------------------------
def read_shoptet_csv(csv_text: str) -> Tuple[List[Dict[str, Any]], List[str], List[str]]:
    """
    Returns (rows, headers, parse_errors).
    - Semicolon delimiter.
    - Skips empty header names created by trailing delimiters.
    """
    errors: List[str] = []
    rows: List[Dict[str, Any]] = []

    f = io.StringIO(csv_text)
    reader = csv.reader(f, delimiter=";", quotechar='"')

    headers: Optional[List[str]] = None
    keep_idx: Optional[List[int]] = None

    for i, rec in enumerate(reader):
        # skip empty/blank lines
        if not rec or all((c or "").strip() == "" for c in rec):
            continue

        if headers is None:
            raw_headers = [(h or "").strip().lstrip("\ufeff") for h in rec]
            keep_idx = [idx for idx, h in enumerate(raw_headers) if h != ""]
            headers = [raw_headers[idx] for idx in keep_idx]
            continue

        if keep_idx is None:
            keep_idx = list(range(len(rec)))

        filtered = [rec[idx] if idx < len(rec) else "" for idx in keep_idx]

        if len(filtered) != len(headers):
            errors.append(f"Line {i+1}: filtered column count {len(filtered)} != header {len(headers)}")
            continue

        rows.append(dict(zip(headers, filtered)))

    return rows, (headers or []), errors


def filter_by_window(rows: List[Dict[str, Any]], date_key: str, days: int) -> List[Dict[str, Any]]:
    if days <= 0:
        return rows
    cutoff = datetime.now() - timedelta(days=days)
    out: List[Dict[str, Any]] = []
    for r in rows:
        dt = r.get(date_key)
        dt = dt if isinstance(dt, datetime) else parse_datetime(dt)
        if dt and dt >= cutoff:
            out.append(r)
    return out

# ----------------------------------------------------------------------
# BigQuery helpers
# ----------------------------------------------------------------------
def bq_client() -> bigquery.Client:
    return bigquery.Client()


def ensure_table_with_schema(bq: bigquery.Client, table_id: str, required_fields: List[bigquery.SchemaField]) -> bigquery.Table:
    """Ensure table exists and add any missing columns."""
    try:
        table = bq.get_table(table_id)
        existing = {f.name: f for f in table.schema}
        to_add = [f for f in required_fields if f.name not in existing]
        if to_add:
            table.schema = list(table.schema) + to_add
            table = bq.update_table(table, ["schema"])
            log.info("Added columns to %s: %s", table_id, [f.name for f in to_add])
        return table
    except NotFound:
        table = bigquery.Table(table_id, schema=required_fields)
        table = bq.create_table(table)
        log.info("Created table %s", table_id)
        return table


def is_table_empty(bq: bigquery.Client, table_id: str) -> bool:
    query = f"SELECT COUNT(1) AS c FROM `{table_id}`"
    res = list(bq.query(query).result())
    return res[0]["c"] == 0


def _coerce_for_json(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        rr = dict(r)
        v = rr.get("date")
        if isinstance(v, datetime):
            rr["date"] = v.strftime("%Y-%m-%d %H:%M:%S")
        v2 = rr.get("date_only")
        if isinstance(v2, (datetime, date)):
            rr["date_only"] = v2.isoformat() if isinstance(v2, date) else v2.date().isoformat()
        out.append(rr)
    return out


def _choose_keys(existing_fields: List[str]) -> Tuple[str, ...]:
    """Choose merge keys based on available columns."""
    candidates = [
        ("identifier", "date", "orderItemType"),
        ("identifier", "date", "code"),
        ("identifier", "date", "email"),
        ("identifier", "date"),
    ]
    for ks in candidates:
        if all(k in existing_fields for k in ks):
            return ks
    return ("identifier", "date")


def _dedupe(rows: List[Dict[str, Any]], key_fields: Tuple[str, ...]) -> List[Dict[str, Any]]:
    seen: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    for r in rows:
        k = tuple(r.get(kf) for kf in key_fields)
        prev = seen.get(k)
        if prev is None:
            seen[k] = r
        else:
            # simple precedence: latest wins
            seen[k] = r
    return list(seen.values())


def load_to_staging(
    bq: bigquery.Client,
    rows: List[Dict[str, Any]],
    target_table: str,
    schema_fields: List[bigquery.SchemaField],
    location: str,
    key_fields: Tuple[str, ...],
) -> Optional[str]:
    if not rows:
        return None

    dataset = ".".join(target_table.split(".")[:2])
    staging = f"{dataset}._stg_{int(time.time())}"

    bq.create_table(bigquery.Table(staging, schema=schema_fields))
    deduped = _dedupe(rows, key_fields)
    json_rows = _coerce_for_json(deduped)
    job = bq.load_table_from_json(json_rows, staging, location=location)
    job.result()
    log.info("Loaded %d rows to staging %s", len(json_rows), staging)
    return staging


def merge_staging(
    bq: bigquery.Client,
    staging: str,
    target: str,
    location: str,
    key_fields: Tuple[str, ...],
):
    """
    Fix #2: only reference columns that exist in BOTH staging and target.
    This allows columns to be dropped from schema or renamed without breaking MERGE.
    """
    tgt = bq.get_table(target)
    stg = bq.get_table(staging)

    target_fields = [f.name for f in tgt.schema]
    staging_fields = [f.name for f in stg.schema]

    key_set = set(key_fields)
    common_fields = [f for f in target_fields if f in staging_fields and f not in key_set]

    sel_cols = list(key_fields) + common_fields
    select_sql = ", ".join(sel_cols)

    update_sets = ", ".join([f"T.{f} = S.{f}" for f in common_fields]) if common_fields else ""
    update_clause = f"WHEN MATCHED THEN UPDATE SET {update_sets}" if update_sets else ""

    insert_cols = list(key_fields) + common_fields
    insert_fields = ", ".join(insert_cols)
    insert_values = ", ".join([f"S.{f}" for f in insert_cols])

    query = f"""
    MERGE `{target}` T
    USING (
      SELECT {select_sql}
      FROM `{staging}`
    ) S
    ON {" AND ".join([f"T.{k} = S.{k}" for k in key_fields])}
    {update_clause}
    WHEN NOT MATCHED THEN
      INSERT ({insert_fields}) VALUES ({insert_values});
    """
    bq.query(query, location=location).result()
    bq.delete_table(staging, not_found_ok=True)

# ----------------------------------------------------------------------
# Schema library
# ----------------------------------------------------------------------
DEFAULT_SCHEMA_DEF = [
    {"name": "date", "source": "date", "type": "DATETIME", "parse": "datetime"},
    {"name": "orderItemType", "source": "orderItemType", "type": "STRING", "parse": "string"},
    {"name": "orderItemTotalPriceWithoutVat", "source": "orderItemTotalPriceWithoutVat", "type": "FLOAT", "parse": "decimal_comma"},
    {"name": "identifier", "source": None, "type": "STRING", "parse": "string"},
    {"name": "date_only", "source": None, "type": "DATE", "parse": "date_only"},
]


def load_schema_library() -> Dict[str, List[Dict[str, Any]]]:
    lib_url = os.getenv("SCHEMA_URL")
    if not lib_url:
        return {}
    try:
        text = fetch_text_from_url(lib_url)
        data = yaml.safe_load(text) or {}
        export_types = data.get("export_types", {})
        if not isinstance(export_types, dict):
            log.warning("SCHEMA_URL export_types not a dict, ignoring.")
            return {}
        return export_types
    except Exception as e:
        log.exception("Failed to load SCHEMA_URL: %s", e)
        return {}


def schema_from_config(pipeline_cfg: Dict[str, Any], schema_lib: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    if pipeline_cfg.get("schema"):
        base = list(pipeline_cfg["schema"])
    else:
        export_type = pipeline_cfg.get("export_type")
        if export_type and export_type in schema_lib:
            base = list(schema_lib[export_type])
        else:
            base = list(DEFAULT_SCHEMA_DEF)

    names = {f["name"] for f in base}
    if "identifier" not in names:
        base.append({"name": "identifier", "source": None, "type": "STRING", "parse": "string"})
    if "date_only" not in names:
        base.append({"name": "date_only", "source": None, "type": "DATE", "parse": "date_only"})
    return base


def bq_fields_from_schema(schema_def: List[Dict[str, Any]]) -> List[bigquery.SchemaField]:
    return [bigquery.SchemaField(f["name"], f["type"]) for f in schema_def]


def build_row_from_record(rec: Dict[str, Any], schema_def: List[Dict[str, Any]], pipeline_id: str) -> Dict[str, Any]:
    row: Dict[str, Any] = {}
    for f in schema_def:
        name = f["name"]
        src = f.get("source")
        parser_key = f.get("parse", "string")
        parser = PARSERS.get(parser_key, PARSERS["string"])
        if src:
            val = rec.get(src)
            row[name] = parser(val)
        else:
            row[name] = None

    if "identifier" in row:
        row["identifier"] = pipeline_id

    if "date_only" in row:
        if "date" in row and isinstance(row["date"], datetime):
            row["date_only"] = row["date"].date().isoformat()
        elif "date" in row and isinstance(row["date"], str):
            row["date_only"] = parse_date_only(row["date"])

    return row

# ----------------------------------------------------------------------
# Health checks
# ----------------------------------------------------------------------
def health_checks(rows: List[Dict[str, Any]], key_fields: Tuple[str, ...]) -> List[str]:
    issues: List[str] = []
    if not rows:
        issues.append("No rows parsed.")
        return issues

    seen = set()
    dups = 0
    for r in rows:
        k = tuple(r.get(kf) for kf in key_fields)
        if k in seen:
            dups += 1
        else:
            seen.add(k)
    if dups:
        issues.append(f"{dups} duplicate keys {key_fields} (dedup applied).")

    null_date = sum(1 for r in rows if r.get("date") is None)
    if null_date:
        issues.append(f"{null_date} rows with NULL date.")

    return issues

# ----------------------------------------------------------------------
# Pipeline runner (multi-pipeline only)
# ----------------------------------------------------------------------
def process_pipeline(
    p: Dict[str, Any],
    bq_loc: str,
    allow_unknown: bool,
    schema_lib: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, Any]:
    pipeline_id = p.get("id") or "unknown"
    csv_url = p["csv_url"]
    table_id = p["bq_table_id"]

    load_mode = (p.get("load_mode") or os.getenv("LOAD_MODE", "auto")).lower()
    window_days = int(p.get("window_days", os.getenv("WINDOW_DAYS", 30)))

    log.info("Pipeline %s -> %s (mode=%s, window_days=%d)", pipeline_id, table_id, load_mode, window_days)

    schema_def = schema_from_config(p, schema_lib)
    bq_fields = bq_fields_from_schema(schema_def)

    pipeline_encoding = p.get("encoding") or os.getenv("DEFAULT_CSV_ENCODING", "auto")
    csv_text = fetch_text_from_url(csv_url, preferred_encoding=pipeline_encoding)
    raw_rows, headers, parse_errors = read_shoptet_csv(csv_text)

    # Header reconciliation
    schema_sources = [f["source"] for f in schema_def if f.get("source")]
    header_set = set(headers)
    source_set = set(schema_sources)

    # ignore empty header name ""
    unknown_csv_cols = sorted(c for c in (header_set - source_set) if c != "")
    missing_schema_cols = sorted(source_set - header_set)

    if unknown_csv_cols and not allow_unknown:
        return {
            "pipeline": pipeline_id,
            "table": table_id,
            "status": "blocked_unknown_columns",
            "unknown_columns": unknown_csv_cols,
            "missing_columns": missing_schema_cols,
            "message": "Unknown CSV columns detected. Re-run with ?allow_unknown=1 or set ALLOW_UNKNOWN_COLUMNS=true.",
        }

    parsed_rows: List[Dict[str, Any]] = [build_row_from_record(src, schema_def, pipeline_id) for src in raw_rows]

    bq_cli = bq_client()
    table = ensure_table_with_schema(bq_cli, table_id, bq_fields)

    if load_mode not in {"auto", "full", "window"}:
        load_mode = "auto"

    if load_mode == "auto":
        effective_mode = "full" if is_table_empty(bq_cli, table_id) else "window"
    else:
        effective_mode = load_mode

    if effective_mode == "full":
        kept = parsed_rows
    else:
        kept = filter_by_window(parsed_rows, "date", window_days)

    kept = [r for r in kept if r.get("date") is not None]

    key_fields = _choose_keys([f.name for f in table.schema])
    issues = health_checks(kept, key_fields)

    staging = load_to_staging(bq_cli, kept, table_id, bq_fields, bq_loc, key_fields)
    if staging:
        merge_staging(bq_cli, staging, table_id, bq_loc, key_fields)

    return {
        "pipeline": pipeline_id,
        "table": table_id,
        "status": "ok",
        "headers": headers,
        "parsed_rows": len(parsed_rows),
        "kept_rows": len(kept),
        "parse_errors": len(parse_errors),
        "health_issues": issues,
        "unknown_columns": unknown_csv_cols,
        "missing_columns": missing_schema_cols,
        "mode": effective_mode,
        "window_days": window_days if effective_mode == "window" else None,
        "key_fields": key_fields,
        "encoding": pipeline_encoding,
    }

# ----------------------------------------------------------------------
# Flask endpoints
# ----------------------------------------------------------------------
@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok", "message": "Shoptet → BigQuery multi-pipeline service is running."})


@app.route("/run", methods=["GET", "POST"])
def run_ingest():
    start = time.time()
    bq_location = os.getenv("BQ_LOCATION", "EU")

    allow_unknown = (
        request.args.get("allow_unknown") in {"1", "true", "yes"}
        or os.getenv("ALLOW_UNKNOWN_COLUMNS", "false").lower() in {"1", "true", "yes"}
    )

    schema_lib = load_schema_library()

    config_url = os.getenv("CONFIG_URL")
    if not config_url:
        return jsonify({
            "status": "error",
            "message": "Multi-pipeline mode requires CONFIG_URL env var pointing to YAML in GCS/HTTPS."
        }), 400

    try:
        yaml_text = fetch_text_from_url(config_url)
        conf = yaml.safe_load(yaml_text) or {}
        all_pipes = conf.get("pipelines", [])
    except Exception as e:
        log.exception("Failed to fetch/parse CONFIG_URL")
        return jsonify({"status": "error", "message": f"Failed to fetch/parse CONFIG_URL: {e}"}), 500

    pipeline_filter = request.args.get("pipeline")
    if pipeline_filter:
        pipelines = [p for p in all_pipes if p.get("id") == pipeline_filter]
        if not pipelines:
            return jsonify({"status": "error", "message": f"Pipeline id '{pipeline_filter}' not found"}), 404
    else:
        pipelines = all_pipes

    results: List[Dict[str, Any]] = []
    for p in pipelines:
        try:
            results.append(process_pipeline(p, bq_location, allow_unknown, schema_lib))
        except BadRequest as e:
            log.exception("BigQuery error on pipeline %s", p.get("id"))
            return jsonify({"status": "error", "message": str(e)}), 500
        except Exception as e:
            log.exception("Unhandled error on pipeline %s", p.get("id"))
            return jsonify({"status": "error", "message": str(e)}), 500

    elapsed = round(time.time() - start, 2)
    return jsonify({"status": "ok", "elapsed_sec": elapsed, "results": results})