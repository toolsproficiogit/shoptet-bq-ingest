import csv
import io
import os
import re
import sys
import json
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple, Optional

import requests
import yaml
from flask import Flask, jsonify, request
from google.cloud import bigquery, storage
from google.api_core.exceptions import NotFound, BadRequest

# -------- Logging --------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("shoptet-bq")

app = Flask(__name__)

# -------- Helpers --------
def parse_gcs_from_https(url: str) -> Optional[Tuple[str, str]]:
    """
    Map https://storage.googleapis.com/<bucket>/<object> to (bucket, object)
    """
    m = re.match(r"^https?://storage\.googleapis\.com/([^/]+)/(.*)$", url)
    if m:
        return m.group(1), m.group(2)
    return None

def fetch_text_from_url(url: str, timeout=120) -> str:
    """
    Fetch YAML/CSV text from HTTPS or GCS using ADC.
    - HTTPS GCS (storage.googleapis.com): prefer Storage API (private objects OK)
    - gs:// URI: use Storage API
    - Other HTTPS: use requests (public/signed URL)
    Always decode with UTF-8-SIG to drop BOM if present.
    """
    if url.startswith("gs://"):
        bucket_name, blob_name = url[5:].split("/", 1)
        client = storage.Client()
        return client.bucket(bucket_name).blob(blob_name).download_as_text(encoding="utf-8")
    gcs_pair = parse_gcs_from_https(url)
    if gcs_pair:
        bucket_name, blob_name = gcs_pair
        client = storage.Client()
        return client.bucket(bucket_name).blob(blob_name).download_as_text(encoding="utf-8")
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.content.decode("utf-8-sig")

def decimal_comma_to_float(s: str) -> Optional[float]:
    """
    Convert '1.234,56' / '3380,17' -> float. Return None if blank/unparsable.
    """
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

def parse_datetime(s: str) -> Optional[datetime]:
    """
    Parse 'YYYY-MM-DD HH:MM:SS' -> datetime (naive).
    """
    if not s:
        return None
    s = s.strip().strip('"').strip("'")
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None

def read_shoptet_csv(csv_text: str) -> Tuple[List[Dict[str, Any]], List[str], List[str]]:
    """
    Parse a Shoptet CSV with ';' delimiter and quoted fields.
    Returns (rows, headers, errors)
    """
    errors = []
    rows: List[Dict[str, Any]] = []
    f = io.StringIO(csv_text)
    reader = csv.reader(f, delimiter=';', quotechar='"')
    headers = None
    for i, rec in enumerate(reader):
        if not rec or all((c or "").strip() == "" for c in rec):
            continue
        if headers is None:
            headers = [h.strip().lstrip("\ufeff") for h in rec]  # strip BOM
            continue
        if len(rec) != len(headers):
            errors.append(f"Line {i+1}: column count {len(rec)} != header {len(headers)}")
            continue
        row = dict(zip(headers, rec))
        rows.append(row)
    return rows, (headers or []), errors

def filter_by_window(rows: List[Dict[str, Any]], date_col: str, days: int) -> List[Dict[str, Any]]:
    if days <= 0:
        return rows
    cutoff = datetime.now() - timedelta(days=days)
    out = []
    for r in rows:
        dt = parse_datetime(r.get(date_col))
        if dt and dt >= cutoff:
            out.append(r)
    return out

def bq_client() -> bigquery.Client:
    return bigquery.Client()

def ensure_table(bq: bigquery.Client, table_id: str, location: str) -> bigquery.Table:
    """
    Ensure target table exists (schema kept simple & fixed):
      - date: DATETIME
      - orderItemType: STRING
      - orderItemTotalPriceWithoutVat: FLOAT
    """
    try:
        return bq.get_table(table_id)
    except NotFound:
        schema = [
            bigquery.SchemaField("date", "DATETIME"),
            bigquery.SchemaField("orderItemType", "STRING"),
            bigquery.SchemaField("orderItemTotalPriceWithoutVat", "FLOAT"),
        ]
        table = bigquery.Table(table_id, schema=schema)
        table = bq.create_table(table)
        log.info("Created table %s in %s", table_id, location)
        return table

def is_table_empty(bq: bigquery.Client, table_id: str) -> bool:
    query = f"SELECT COUNT(1) AS c FROM `{table_id}`"
    res = list(bq.query(query).result())
    return res[0]["c"] == 0

def _coerce_rows_for_bq(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Convert Python types to JSON-serializable values BigQuery accepts.
    - datetime -> 'YYYY-MM-DD HH:MM:SS'
    """
    out: List[Dict[str, Any]] = []
    for r in rows:
        rr = dict(r)
        dt = rr.get("date")
        if isinstance(dt, datetime):
            rr["date"] = dt.strftime("%Y-%m-%d %H:%M:%S")
        out.append(rr)
    return out

def load_to_staging(bq: bigquery.Client, rows: List[Dict[str, Any]], target_table: str, location: str) -> Optional[str]:
    """
    Load JSON rows into a temporary staging table with same schema as target.
    Returns staging table id or None if nothing to load.
    """
    if not rows:
        return None
    dataset = ".".join(target_table.split(".")[:2])
    staging = f"{dataset}._stg_{int(time.time())}"
    schema = [
        bigquery.SchemaField("date", "DATETIME"),
        bigquery.SchemaField("orderItemType", "STRING"),
        bigquery.SchemaField("orderItemTotalPriceWithoutVat", "FLOAT"),
    ]
    bq.create_table(bigquery.Table(staging, schema=schema))

    # Coerce datetimes to strings before JSON load
    json_rows = _coerce_rows_for_bq(rows)

    job = bq.load_table_from_json(json_rows, staging, location=location)
    job.result()
    log.info("Loaded %d rows to staging %s", len(json_rows), staging)
    return staging

def merge_staging(bq: bigquery.Client, staging: str, target: str, location: str) -> Tuple[int, int]:
    """
    MERGE staging into target on (date, orderItemType). Update price if changed; insert new.
    Returns (updated_count, inserted_count) — we return (0,0) as placeholders.
    """
    query = f"""
    MERGE `{target}` T
    USING `{staging}` S
    ON T.date = S.date AND T.orderItemType = S.orderItemType
    WHEN MATCHED AND T.orderItemTotalPriceWithoutVat IS DISTINCT FROM S.orderItemTotalPriceWithoutVat THEN
      UPDATE SET orderItemTotalPriceWithoutVat = S.orderItemTotalPriceWithoutVat
    WHEN NOT MATCHED THEN
      INSERT (date, orderItemType, orderItemTotalPriceWithoutVat)
      VALUES (S.date, S.orderItemType, S.orderItemTotalPriceWithoutVat);
    """
    bq.query(query, location=location).result()
    # Drop staging regardless of MERGE success to avoid clutter (MERGE already completed if we got here)
    bq.delete_table(staging, not_found_ok=True)
    return 0, 0

def health_checks(parsed: List[Dict[str, Any]], kept: List[Dict[str, Any]]) -> List[str]:
    issues = []
    if not parsed:
        issues.append("No rows parsed.")
        return issues
    # Duplicate composite keys in kept rows (date, orderItemType)
    seen = set()
    dups = 0
    for r in kept:
        k = (r.get("date"), r.get("orderItemType"))
        if k in seen:
            dups += 1
        else:
            seen.add(k)
    if dups:
        issues.append(f"{dups} duplicate (date, orderItemType) keys after windowing.")
    # Nulls
    null_price = sum(1 for r in kept if r.get("orderItemTotalPriceWithoutVat") is None)
    if null_price:
        issues.append(f"{null_price} rows with NULL price.")
    null_date = sum(1 for r in kept if r.get("date") is None)
    if null_date:
        issues.append(f"{null_date} rows with NULL date.")
    return issues

def process_pipeline(p: Dict[str, Any], bq_loc: str) -> Dict[str, Any]:
    """
    Process one pipeline: fetch CSV, parse, window/full, load, merge.
    Required keys in p: id, csv_url, bq_table_id
    Optional: load_mode (auto|full|window), window_days (int)
    """
    csv_url = p["csv_url"]
    table_id = p["bq_table_id"]
    load_mode = (p.get("load_mode") or os.getenv("LOAD_MODE", "auto")).lower()
    window_days = int(p.get("window_days", os.getenv("WINDOW_DAYS", 30)))

    log.info("Pipeline %s -> %s (mode=%s, window_days=%d)", p.get("id"), table_id, load_mode, window_days)

    csv_text = fetch_text_from_url(csv_url)
    raw_rows, headers, parse_errors = read_shoptet_csv(csv_text)

    # Coerce & standardize to expected schema
    parsed_rows: List[Dict[str, Any]] = []
    for r in raw_rows:
        dt = parse_datetime(r.get("date"))
        typ = (r.get("orderItemType") or "").strip().strip('"')
        price = decimal_comma_to_float(r.get("orderItemTotalPriceWithoutVat"))
        parsed_rows.append({
            "date": dt,
            "orderItemType": typ,
            "orderItemTotalPriceWithoutVat": price,
        })

    bq_cli = bq_client()
    ensure_table(bq_cli, table_id, bq_loc)

    # Resolve mode
    if load_mode not in {"auto", "full", "window"}:
        load_mode = "auto"
    if load_mode == "auto":
        empty = is_table_empty(bq_cli, table_id)
        effective_mode = "full" if empty else "window"
    else:
        effective_mode = load_mode

    kept_rows = parsed_rows if effective_mode == "full" else filter_by_window(parsed_rows, "date", window_days)
    # Remove rows missing required fields after coercion
    kept_rows = [r for r in kept_rows if r["date"] is not None and r["orderItemType"] not in ("", None)]

    issues = health_checks(parsed_rows, kept_rows)

    staging = load_to_staging(bq_cli, kept_rows, table_id, bq_loc)
    if staging:
        merge_staging(bq_cli, staging, table_id, bq_loc)

    return {
        "pipeline": p.get("id"),
        "table": table_id,
        "headers": headers,
        "parsed_rows": len(parsed_rows),
        "kept_rows": len(kept_rows),
        "parse_errors": len(parse_errors),
        "health_issues": issues,
        "mode": effective_mode,
        "window_days": window_days if effective_mode == "window" else None,
    }

# -------- Routes --------
@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok", "message": "Shoptet → BigQuery service is running."})

@app.route("/run", methods=["GET", "POST"])
def run_ingest():
    start = time.time()
    bq_location = os.getenv("BQ_LOCATION", "EU")

    # Single pipeline via env vars
    csv_url = os.getenv("CSV_URL")
    bq_table_id = os.getenv("BQ_TABLE_ID")

    pipelines: List[Dict[str, Any]] = []
    pipeline_filter = request.args.get("pipeline")

    if csv_url and bq_table_id:
        pipelines = [{
            "id": pipeline_filter or "single",
            "csv_url": csv_url,
            "bq_table_id": bq_table_id,
            "load_mode": os.getenv("LOAD_MODE", "auto"),
            "window_days": int(os.getenv("WINDOW_DAYS", "30")),
        }]
    else:
        # Multi-pipeline requires CONFIG_URL (remote YAML)
        config_url = os.getenv("CONFIG_URL")
        if not config_url:
            return jsonify({
                "status": "error",
                "message": "Multi-pipeline requires CONFIG_URL env var pointing to YAML in GCS/HTTPS."
            }), 400

        try:
            yaml_text = fetch_text_from_url(config_url)
            conf = yaml.safe_load(yaml_text) or {}
            all_pipes = conf.get("pipelines", [])
        except Exception as e:
            log.exception("Failed to fetch/parse CONFIG_URL")
            return jsonify({"status": "error", "message": f"Failed to fetch/parse CONFIG_URL: {e}"}), 500

        if pipeline_filter:
            pipelines = [p for p in all_pipes if p.get("id") == pipeline_filter]
            if not pipelines:
                return jsonify({"status": "error", "message": f"Pipeline id '{pipeline_filter}' not found"}), 404
        else:
            pipelines = all_pipes

    results = []
    for p in pipelines:
        try:
            results.append(process_pipeline(p, bq_location))
        except BadRequest as e:
            log.exception("BigQuery error on pipeline %s", p.get("id"))
            return jsonify({"status": "error", "message": str(e)}), 500
        except Exception as e:
            log.exception("Unhandled error on pipeline %s", p.get("id"))
            return jsonify({"status": "error", "message": str(e)}), 500

    elapsed = round(time.time() - start, 2)
    return jsonify({"status": "ok", "elapsed_sec": elapsed, "results": results})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
