"""
Improved Google Sheets Configuration Provider

This provider loads pipeline and schema configurations from Google Sheets.
It includes comprehensive validation and support for all configuration options.

Expected Sheet Structure:

Pipeline_Config tab:
  | pipeline_id | export_type | csv_url | bq_table_id | write_disposition | delimiter | encoding | skip_leading_rows | load_mode | window_days | dedupe_mode | timeout_sec | retries | active |
  | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
  | pipe_1 | orders | https://... | project.dataset.table | WRITE_APPEND | ; | utf-8 | 1 | auto | 30 | auto_dedupe | 300 | 3 | TRUE |

Schema_Config tab:
  | export_type | name | type | mode | description | source | parse_logic |
  | --- | --- | --- | --- | --- | --- | --- |
  | orders | id | STRING | NULLABLE | Order ID | id | string |
  | orders | amount | FLOAT | NULLABLE | Order Amount | amount | float |

Run_Logs tab (auto-created):
  | timestamp | pipeline_id | status | rows_loaded | error_message |
"""

import gspread
import logging
import datetime
from google.oauth2.service_account import Credentials
from config_loader import ConfigProvider

logger = logging.getLogger(__name__)


class GoogleSheetsConfigProvider(ConfigProvider):
    """Load pipeline and schema configs from Google Sheets."""
    
    def __init__(self, sheet_id: str):
        """
        Initialize the Google Sheets config provider.
        
        Args:
            sheet_id: Google Sheets ID (from the URL)
        """
        self.sheet_id = sheet_id
        
        # Authenticate using the environment's default Service Account
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        
        try:
            self.client = gspread.service_account(scopes=scopes)
            self.sheet = self.client.open_by_key(self.sheet_id)
            logger.info(f"Connected to Google Sheets: {self.sheet.title}")
        except Exception as e:
            logger.error(f"Failed to connect to Google Sheets: {e}")
            raise

    def _clean_bool(self, val) -> bool:
        """Helper to safely parse boolean strings from Sheets."""
        return str(val).strip().upper() == 'TRUE'

    def _get_worksheet_safe(self, tab_name: str, create_if_missing: bool = False) -> gspread.Worksheet:
        """
        Get a worksheet by name, optionally creating it if missing.
        
        Args:
            tab_name: Name of the worksheet
            create_if_missing: Whether to create the tab if it doesn't exist
        
        Returns:
            gspread.Worksheet object
        
        Raises:
            ValueError if tab is missing and create_if_missing is False
        """
        try:
            return self.sheet.worksheet(tab_name)
        except gspread.exceptions.WorksheetNotFound:
            if create_if_missing:
                logger.info(f"Creating missing tab: {tab_name}")
                return self.sheet.add_worksheet(title=tab_name, rows=1000, cols=10)
            else:
                raise ValueError(f"Tab '{tab_name}' is missing in the Google Sheet.")

    def _load_pipelines(self) -> list:
        """Load pipeline configurations from Pipeline_Config tab."""
        try:
            pipeline_ws = self._get_worksheet_safe("Pipeline_Config")
            raw_pipelines = pipeline_ws.get_all_records()
            logger.info(f"Loaded {len(raw_pipelines)} pipeline records from Pipeline_Config tab")
        except Exception as e:
            logger.error(f"Failed to load pipelines: {e}")
            raise

        pipelines_list = []
        
        for i, row in enumerate(raw_pipelines):
            try:
                # Skip inactive pipelines
                if not self._clean_bool(row.get('active', 'TRUE')):
                    logger.debug(f"Skipping inactive pipeline: {row.get('pipeline_id', f'row_{i}')}")
                    continue
                
                # Validate required fields
                required_fields = ['pipeline_id', 'export_type', 'csv_url', 'bq_table_id']
                missing = [f for f in required_fields if not row.get(f)]
                if missing:
                    logger.error(f"Pipeline row {i} missing required fields: {missing}")
                    continue
                
                # Build pipeline definition
                pipeline_def = {
                    'id': row['pipeline_id'].strip(),
                    'export_type': row['export_type'].strip(),
                    'csv_url': row['csv_url'].strip(),
                    'bq_table_id': row['bq_table_id'].strip(),
                    'write_disposition': (row.get('write_disposition', 'WRITE_APPEND') or 'WRITE_APPEND').strip(),
                    'delimiter': (row.get('delimiter', ';') or ';').strip(),
                    'encoding': (row.get('encoding', 'utf-8') or 'utf-8').strip(),
                    'skip_leading_rows': int(row.get('skip_leading_rows', 1) or 1),
                    'load_mode': (row.get('load_mode', 'auto') or 'auto').strip().lower(),
                    'window_days': int(row.get('window_days', 30) or 30),
                    'dedupe_mode': (row.get('dedupe_mode', 'auto_dedupe') or 'auto_dedupe').strip().lower(),
                    'timeout_sec': int(row.get('timeout_sec', 300) or 300),
                    'retries': int(row.get('retries', 3) or 3),
                }
                
                pipelines_list.append(pipeline_def)
                logger.info(f"Loaded pipeline: {pipeline_def['id']}")
            
            except Exception as e:
                logger.error(f"Error parsing pipeline row {i}: {e}")
                continue
        
        logger.info(f"Successfully loaded {len(pipelines_list)} active pipelines")
        return pipelines_list

    def _load_schemas(self) -> dict:
        """Load schema configurations from Schema_Config tab."""
        try:
            schema_ws = self._get_worksheet_safe("Schema_Config")
            raw_schemas = schema_ws.get_all_records()
            logger.info(f"Loaded {len(raw_schemas)} schema records from Schema_Config tab")
        except Exception as e:
            logger.error(f"Failed to load schemas: {e}")
            raise

        # Transform Schemas: Group flat rows by 'export_type'
        schemas_map = {}
        
        for i, row in enumerate(raw_schemas):
            try:
                export_type = str(row.get('export_type', '')).strip()
                if not export_type:
                    logger.debug(f"Skipping schema row {i}: empty export_type")
                    continue
                
                # Validate required fields
                if not row.get('name'):
                    logger.error(f"Schema row {i} for export_type '{export_type}' missing 'name'")
                    continue
                if not row.get('type'):
                    logger.error(f"Schema row {i} for export_type '{export_type}' missing 'type'")
                    continue
                
                if export_type not in schemas_map:
                    schemas_map[export_type] = []
                
                # Build the schema field dictionary
                field_def = {
                    'name': row['name'].strip(),
                    'type': row['type'].strip().upper(),
                    'mode': (row.get('mode', 'NULLABLE') or 'NULLABLE').strip().upper(),
                    'description': (row.get('description') or '').strip(),
                    'source': (row.get('source') or row['name']).strip(),
                    'parse': (row.get('parse_logic') or 'string').strip().lower(),
                }
                
                schemas_map[export_type].append(field_def)
            
            except Exception as e:
                logger.error(f"Error parsing schema row {i}: {e}")
                continue
        
        logger.info(f"Successfully loaded {len(schemas_map)} schema definitions")
        return schemas_map

    def get_configs(self) -> dict:
        """
        Load all configurations from Google Sheets.
        
        Returns:
            Dictionary with 'pipelines' and 'schemas' keys
        """
        logger.info("Fetching configuration from Google Sheets...")
        
        pipelines = self._load_pipelines()
        schemas = self._load_schemas()
        
        logger.info(f"Loaded {len(pipelines)} pipelines and {len(schemas)} schema definitions")
        
        return {
            'pipelines': pipelines,
            'schemas': schemas
        }

    def log_run(self, pipeline_id: str, status: str, rows_loaded: int = 0, error_msg: str = ""):
        """
        Write execution status to the Run_Logs tab.
        
        Args:
            pipeline_id: Pipeline identifier
            status: Status (SUCCESS, ERROR, SKIPPED)
            rows_loaded: Number of rows loaded
            error_msg: Error message if applicable
        """
        try:
            # Create tab if it doesn't exist
            log_ws = self._get_worksheet_safe("Run_Logs", create_if_missing=True)
            
            # Add header if this is a new tab
            if log_ws.row_count == 1:
                log_ws.append_row(["timestamp", "pipeline_id", "status", "rows_loaded", "error_message"])
            
            timestamp = datetime.datetime.now().isoformat()
            log_ws.append_row([
                timestamp,
                pipeline_id,
                status,
                rows_loaded,
                str(error_msg)[:500]  # Limit error message length
            ])
            
            logger.info(f"Logged run for pipeline {pipeline_id}: {status}")
        
        except Exception as e:
            logger.warning(f"Could not write to Run_Logs: {e}")

    def validate_configs(self) -> list:
        """
        Validate loaded configurations.
        
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        
        try:
            config_data = self.get_configs()
            pipelines = config_data.get('pipelines', [])
            schemas = config_data.get('schemas', {})
            
            # Validate pipelines
            if not pipelines:
                errors.append("No active pipelines found in Pipeline_Config tab")
            
            for pipeline in pipelines:
                export_type = pipeline.get('export_type')
                if export_type not in schemas:
                    errors.append(f"Pipeline '{pipeline.get('id')}' references missing schema: {export_type}")
            
            # Validate schemas
            if not schemas:
                errors.append("No schemas found in Schema_Config tab")
            
            for export_type, fields in schemas.items():
                if not fields:
                    errors.append(f"Schema '{export_type}' has no fields")
        
        except Exception as e:
            errors.append(f"Validation error: {e}")
        
        return errors