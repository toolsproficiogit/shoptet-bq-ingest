import os
import logging
import requests
import tempfile
from typing import Dict, Any, List
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPICallError

# --- IMPORT CONFIG PROVIDER STRATEGY ---
# Ensure config_loader.py is in the same directory
from config_loader import get_config_provider

# --- SETUP LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('ShoptetIngest')

def get_bq_schema(schema_config: List[Dict[str, Any]]) -> List[bigquery.SchemaField]:
    """
    Converts the plain dictionary schema from config into BigQuery SchemaField objects.
    """
    bq_schema = []
    for field in schema_config:
        bq_schema.append(
            bigquery.SchemaField(
                name=field['name'],
                field_type=field['type'],
                mode=field.get('mode', 'NULLABLE'),
                description=field.get('description')
            )
        )
    return bq_schema

def download_file(url: str, dest_path: str):
    """
    Downloads a file from a URL to a local path with streaming to handle large files.
    """
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(dest_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return dest_path

def process_pipeline(pipeline_config: Dict[str, Any], schema_config: List[Dict[str, Any]]) -> int:
    """
    Executes the full extraction and load process:
    1. Downloads CSV from Source URL.
    2. Configures BigQuery Job (Schema, Options).
    3. Uploads to BigQuery.
    4. Returns number of rows loaded.
    """
    
    # 1. Extract Configs
    pipeline_id = pipeline_config.get('id')
    source_url = pipeline_config.get('source_url')
    table_id = pipeline_config.get('destination_table')
    write_disposition = pipeline_config.get('write_disposition', 'WRITE_APPEND')
    delimiter = pipeline_config.get('delimiter', ',')
    encoding = pipeline_config.get('encoding', 'utf-8')
    skip_leading_rows = int(pipeline_config.get('skip_leading_rows', 1))

    logger.info(f"[{pipeline_id}] Downloading data from {source_url}...")

    # 2. Initialize BigQuery Client
    client = bigquery.Client()

    # 3. Create Temp File for Download
    # We use a temp file to avoid holding massive CSVs in RAM
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv') as tmp_file:
        temp_path = tmp_file.name
    
    try:
        # 4. Download Data
        download_file(source_url, temp_path)
        logger.info(f"[{pipeline_id}] Download complete. File size: {os.path.getsize(temp_path)} bytes.")

        # 5. Configure Load Job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=skip_leading_rows,
            field_delimiter=delimiter,
            encoding=encoding,
            write_disposition=write_disposition,
            schema=get_bq_schema(schema_config),
            # Robustness: Allow some jagged rows if necessary, or set to False for strict mode
            allow_quoted_newlines=True
        )

        # 6. Upload to BigQuery
        logger.info(f"[{pipeline_id}] Starting upload to {table_id}...")
        
        with open(temp_path, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_id, job_config=job_config)

        # 7. Wait for Result
        job.result()  # Waits for the job to complete.

        # 8. Get Job Stats
        table = client.get_table(table_id)
        logger.info(f"[{pipeline_id}] Job finished. Loaded {job.output_rows} rows.")
        return job.output_rows

    except GoogleAPICallError as e:
        logger.error(f"[{pipeline_id}] BigQuery API Error: {e}")
        raise
    except Exception as e:
        logger.error(f"[{pipeline_id}] General Error: {e}")
        raise
    finally:
        # Cleanup temp file
        if os.path.exists(temp_path):
            os.remove(temp_path)

def main():
    """
    Main entry point.
    1. Determines config source (Sheets vs YAML).
    2. Loads Pipelines and Schemas.
    3. Iterates through pipelines and runs them.
    4. Logs results back to Sheets (if applicable).
    """
    
    # 1. Initialize Configuration Provider
    try:
        provider = get_config_provider()
        logger.info(f"Using Config Provider: {type(provider).__name__}")
        
        config_data = provider.get_configs()
        pipelines = config_data.get('pipelines', [])
        schemas = config_data.get('schemas', {})
        
        logger.info(f"Loaded {len(pipelines)} pipelines and {len(schemas)} schema definitions.")
        
    except Exception as e:
        logger.critical(f"Failed to initialize configuration: {e}")
        return

    # 2. Pipeline Execution Loop
    success_count = 0
    fail_count = 0

    for pipeline in pipelines:
        p_id = pipeline.get('id', 'unknown_id')
        export_type = pipeline.get('export_type')
        
        logger.info(f"--- Processing Pipeline: {p_id} ({export_type}) ---")

        # Validation: Check if schema exists for this export type
        current_schema = schemas.get(export_type)
        if not current_schema:
            error_msg = f"MISSING SCHEMA: No schema found for export_type '{export_type}'. Skipping pipeline."
            logger.error(error_msg)
            
            # Log failure to Sheets if applicable
            if hasattr(provider, 'log_run'):
                provider.log_run(
                    pipeline_id=p_id, 
                    status="SKIPPED", 
                    rows_loaded=0, 
                    error_msg=error_msg
                )
            fail_count += 1
            continue

        # Execution Block
        try:
            # --- RUN THE JOB ---
            rows_count = process_pipeline(pipeline, current_schema)
            # -------------------

            # Log Success to Sheets
            if hasattr(provider, 'log_run'):
                provider.log_run(
                    pipeline_id=p_id, 
                    status="SUCCESS", 
                    rows_loaded=rows_count
                )
            success_count += 1

        except Exception as e:
            logger.error(f"FAILURE: Pipeline {p_id} crashed.", exc_info=True)
            
            # Log Error to Sheets
            if hasattr(provider, 'log_run'):
                provider.log_run(
                    pipeline_id=p_id, 
                    status="ERROR", 
                    rows_loaded=0, 
                    error_msg=str(e)
                )
            fail_count += 1

    # 3. Final Summary
    logger.info("="*30)
    logger.info(f"BATCH COMPLETE. Success: {success_count} | Failed: {fail_count}")
    logger.info("="*30)

if __name__ == "__main__":
    main()