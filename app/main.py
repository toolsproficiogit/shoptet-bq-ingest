import csv
import io
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import requests
import yaml
from flask import Flask, jsonify, request
from google.api_core.exceptions import NotFound, Conflict, BadRequest
from google.cloud import bigquery

# --- Logging setup ---
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# ========= Global defaults =========
DEFAULT_WINDOW_DAYS = int(os.environ.get("WINDOW_DAYS", os.environ.get("CHECK_DAYS", "30")))
DEFAULT_LOAD_MODE = os.environ.get("LOAD_MODE", "auto").lower()  # auto|full|window
BQ_LOCATION = os.environ.get("BQ_LOCATION")  # e.g. EU

# Single-pipeline envs (optional; ignored if MULTI_MODE=true or CONFIG_URL provided)
CSV_URL = os.environ.get("CSV_URL")
BQ_TABLE_ID = os.environ.get("BQ_TABLE_ID")

# Multi-pipeline toggles
MULTI_MODE = os.environ.get("MULTI_MODE", "false").lower() in ("1", "true", "yes")
CONFIG_URL = os.environ.get("CONFIG_URL")  # if set, fetch YAML config from this URL (GCS signed/HTTPS)
CONFIG_PATH = os.environ.get("CONFIG_PATH", "/app/config/config.yaml")  # baked-in path in image

EXPECTED_HEADERS = ["date", "orderItemType", "orderItemTotalPriceWithoutVat"]

# ========= Helpers =========

def normalize_decimal(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    s = value.strip().replace(" ", "").replace(" ", "")
    s = s.replace(".", "").replace(",", ".")
    if s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def parse_datetime(dt_str: Optional[str]) -> Optional[datetime]:
    if dt_str is None:
        return None
    dt_str = dt_str.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(dt_str, fmt)
        except ValueError:
            continue
    return None


def ensure_table(client: bigquery.Client, table_id: str):
    schema = [
        bigquery.SchemaField("date", "DATETIME", mode="REQUIRED"),
        bigquery.SchemaField("orderItemType", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("orderItemTotalPriceWithoutVat", "FLOAT", mode="REQUIRED"),
    ]
    try:
        return client.get_table(table_id)
    except NotFound:
        logger.info("Target table not found. Creating %s", table_id)
        table = bigquery.Table(table_id, schema=schema)
        created = client.create_table(table)
        logger.info("Created table %s", created.full_table_id)
        return created


def load_to_staging(client: bigquery.Client, dataset_id: str, rows: List[Dict]):
    staging_table_id = f"{dataset_id}.stg_shoptet_{uuid.uuid4().hex[:8]}"
    schema = [
        bigquery.SchemaField("date", "DATETIME", mode="REQUIRED"),
        bigquery.SchemaField("orderItemType", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("orderItemTotalPriceWithoutVat", "FLOAT", mode="REQUIRED"),
    ]
    table = bigquery.Table(staging_table_id, schema=schema)
    client.create_table(table)

    job = client.load_table_from_json(
        rows,
        staging_table_id,
        job_config=bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE"),
        location=BQ_LOCATION,
    )
    job.result()
    logger.info("Loaded %s rows into staging %s", job.output_rows, staging_table_id)
    return staging_table_id


def merge_staging_into_target(client: bigquery.Client, staging_table: str, target_table: str):
    query = f"""
    MERGE `{target_table}` T
    USING (SELECT date, orderItemType, orderItemTotalPriceWithoutVat FROM `{staging_table}`) S
    ON T.date = S.date AND T.orderItemType = S.orderItemType
    WHEN MATCHED AND T.orderItemTotalPriceWithoutVat IS DISTINCT FROM S.orderItemTotalPriceWithoutVat THEN
      UPDATE SET orderItemTotalPriceWithoutVat = S.orderItemTotalPriceWithoutVat
    WHEN NOT MATCHED THEN
      INSERT (date, orderItemType, orderItemTotalPriceWithoutVat)
      VALUES (S.date, S.orderItemType, S.orderItemTotalPriceWithoutVat)
    """
    job = client.query(query, location=BQ_LOCATION)
    job.result()
    logger.info("MERGE completed: %s", job.state)


def drop_table_safe(client: bigquery.Client, table_id: str):
    try:
        client.delete_table(table_id)
        logger.info("Dropped staging table %s", table_id)
    except NotFound:
        pass


def health_checks(rows: List[Dict]):
    issues = []
    if not rows:
        issues.append("No rows to process in the selected window.")
        return issues
    null_dates = sum(1 for r in rows if r["date"] is None)
    null_type = sum(1 for r in rows if not r.get("orderItemType"))
    null_price = sum(1 for r in rows if r["orderItemTotalPriceWithoutVat"] is None)
    neg_price = sum(1 for r in rows if (r["orderItemTotalPriceWithoutVat"] is not None and r["orderItemTotalPriceWithoutVat"] < 0))
    if null_dates: issues.append(f"Rows with null date: {null_dates}")
    if null_type: issues.append(f"Rows with null orderItemType: {null_type}")
    if null_price: issues.append(f"Rows with null price: {null_price}")
    if neg_price: issues.append(f"Rows with negative price: {neg_price}")
    seen = set(); dups = 0
    for r in rows:
        key = (r.get("date"), r.get("orderItemType"))
        dups += 1 if key in seen else 0
        seen.add(key)
    if dups:
        issues.append(f"Duplicate keys in staging (date, orderItemType): {dups}")
    dates = [r["date"] for r in rows if r["date"] is not None]
    if dates:
        logger.info("Staging window date range: %s -> %s", min(dates), max(dates))
    return issues


# ========= Pipeline runner =========

def fetch_text(url: str) -> str:
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    return resp.content.decode("utf-8-sig", errors="replace")


def run_pipeline(client: bigquery.Client, csv_url: str, bq_table_id: str, load_mode: str, window_days: int):
    logger.info("Starting pipeline: table=%s mode=%s window_days=%s url=%s", bq_table_id, load_mode, window_days, csv_url)

    ensure_table(client, bq_table_id)
    # Decide effective mode (auto -> check emptiness)
    mode_effective = load_mode
    if load_mode == "auto":
        try:
            tbl = client.get_table(bq_table_id)
            is_empty = (tbl.num_rows or 0) == 0
            mode_effective = "full" if is_empty else "window"
        except NotFound:
            mode_effective = "full"
    logger.info("Effective load mode: %s", mode_effective)

    # Fetch + parse CSV
    text = fetch_text(csv_url)
    reader = csv.DictReader(io.StringIO(text), delimiter=';', quotechar='"')
    raw_headers = reader.fieldnames or []
    logger.info("Parsed headers: %s", raw_headers)

    def norm(h):
        return (h or "").replace("﻿", "").strip()
    header_map = {norm(h): h for h in raw_headers}
    missing = [h for h in EXPECTED_HEADERS if h not in header_map]
    if missing:
        logger.warning("Missing expected headers: %s. Proceeding with best-effort mapping.", missing)

    cutoff = None
    if mode_effective == "window":
        cutoff = datetime.utcnow() - timedelta(days=window_days)
        logger.info("Applying cutoff >= %s", cutoff)

    staged_rows = []
    total_rows = 0
    parse_errors = 0

    for row in reader:
        total_rows += 1
        date_raw = row.get(header_map.get("date"))
        type_raw = row.get(header_map.get("orderItemType"))
        price_raw = row.get(header_map.get("orderItemTotalPriceWithoutVat"))

        dt = parse_datetime(date_raw)
        price = normalize_decimal(price_raw)
        otype = (type_raw or "").strip() if type_raw is not None else None

        if dt is None:
            parse_errors += 1
            continue
        if cutoff and dt < cutoff:
            continue
        staged_rows.append({
            "date": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "orderItemType": otype,
            "orderItemTotalPriceWithoutVat": price,
        })

    logger.info("Parsed %s rows, kept %s (mode=%s), parse_errors=%s", total_rows, len(staged_rows), mode_effective, parse_errors)

    hc_issues = health_checks([
        {
            "date": parse_datetime(r["date"]),
            "orderItemType": r["orderItemType"],
            "orderItemTotalPriceWithoutVat": r["orderItemTotalPriceWithoutVat"],
        }
        for r in staged_rows
    ])
    for issue in hc_issues:
        logger.warning("Health check: %s", issue)

    if not staged_rows:
        return {
            "status": "ok",
            "message": "No rows to process in selected mode/window.",
            "parsed_rows": total_rows,
            "kept_rows": 0,
            "parse_errors": parse_errors,
            "health_issues": hc_issues,
            "headers": raw_headers,
            "mode": mode_effective,
            "window_days": window_days if mode_effective == "window" else None,
        }

    dataset_id = bq_table_id.rsplit(".", 1)[0]
    staging = None
    try:
        staging = load_to_staging(client, dataset_id, staged_rows)
        merge_staging_into_target(client, staging, bq_table_id)
    finally:
        if staging:
            drop_table_safe(client, staging)

    return {
        "status": "ok",
        "message": "Ingest complete",
        "parsed_rows": total_rows,
        "kept_rows": len(staged_rows),
        "parse_errors": parse_errors,
        "health_issues": hc_issues,
        "headers": raw_headers,
        "mode": mode_effective,
        "window_days": window_days if mode_effective == "window" else None,
    }


# ========= HTTP endpoints =========

@app.route("/")
def ping():
    return jsonify({"status": "ok", "message": "Shoptet → BigQuery ingest"})


@app.route("/run", methods=["GET", "POST"])  # main entry
def run_ingest():
    start_ts = time.time()
    client = bigquery.Client(location=BQ_LOCATION) if BQ_LOCATION else bigquery.Client()

    # Multi mode load if CONFIG_URL/PATH present
    if MULTI_MODE or CONFIG_URL:
        try:
            if CONFIG_URL:
                logger.info("Loading config from URL: %s", CONFIG_URL)
                cfg_text = fetch_text(CONFIG_URL)
            else:
                logger.info("Loading config from path: %s", CONFIG_PATH)
                with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                    cfg_text = f.read()
            cfg = yaml.safe_load(cfg_text) or {}
            pipelines = cfg.get("pipelines", [])
        except Exception as e:
            logger.exception("Failed to load config")
            return jsonify({"status": "error", "message": f"Failed to load config: {e}"}), 500

        # Optional filter by ?pipeline=ID
        only = request.args.get("pipeline")
        results = {}
        count = 0
        for p in pipelines:
            pid = p.get("id") or f"pipeline_{count}"
            if only and pid != only:
                continue
            csv_url = p["csv_url"]
            bq_table = p["bq_table_id"]
            load_mode = (p.get("load_mode") or DEFAULT_LOAD_MODE).lower()
            window_days = int(p.get("window_days") or DEFAULT_WINDOW_DAYS)
            logger.info("Running pipeline id=%s", pid)
            res = run_pipeline(client, csv_url, bq_table, load_mode, window_days)
            results[pid] = res
            count += 1
        duration = round(time.time() - start_ts, 2)
        return jsonify({"status": "ok", "pipelines": results, "duration_sec": duration})

    # Single mode
    if not CSV_URL or not BQ_TABLE_ID:
        msg = "CSV_URL and BQ_TABLE_ID must be set (or enable MULTI_MODE/CONFIG_URL)."
        logger.error(msg)
        return jsonify({"status": "error", "message": msg}), 500

    res = run_pipeline(client, CSV_URL, BQ_TABLE_ID, DEFAULT_LOAD_MODE, DEFAULT_WINDOW_DAYS)
    duration = round(time.time() - start_ts, 2)
    res["duration_sec"] = duration
    return jsonify(res)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)