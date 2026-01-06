import gspread
import logging
import datetime
from google.oauth2.service_account import Credentials
from config_loader import ConfigProvider

logger = logging.getLogger(__name__)

class GoogleSheetsConfigProvider(ConfigProvider):
    def __init__(self, sheet_id):
        self.sheet_id = sheet_id
        
        # Authenticate using the environment's default Service Account
        # This assumes GOOGLE_APPLICATION_CREDENTIALS is set (standard for BQ tools)
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        
        try:
            self.client = gspread.service_account(scopes=scopes)
            self.sheet = self.client.open_by_key(self.sheet_id)
        except Exception as e:
            logger.error(f"Failed to connect to Google Sheets: {e}")
            raise

    def _clean_bool(self, val):
        """Helper to safely parse boolean strings from Sheets"""
        return str(val).strip().upper() == 'TRUE'

    def get_configs(self):
        logger.info("Fetching configuration from Google Sheets...")
        
        # 1. Fetch Pipelines
        try:
            pipeline_ws = self.sheet.worksheet("Pipeline_Config")
            raw_pipelines = pipeline_ws.get_all_records()
        except gspread.exceptions.WorksheetNotFound:
            raise ValueError("Tab 'Pipeline_Config' is missing in the Google Sheet.")

        # 2. Fetch Schemas
        try:
            schema_ws = self.sheet.worksheet("Schema_Config")
            raw_schemas = schema_ws.get_all_records()
        except gspread.exceptions.WorksheetNotFound:
            raise ValueError("Tab 'Schema_Config' is missing in the Google Sheet.")

        # 3. Transform Schemas: Group flat rows by 'export_type'
        schemas_map = {}
        for row in raw_schemas:
            e_type = str(row.get('export_type', '')).strip()
            if not e_type: 
                continue # Skip empty rows
                
            if e_type not in schemas_map:
                schemas_map[e_type] = []
            
            # Build the schema field dictionary
            field_def = {
                'name': row['name'],
                'type': row['type'],
                'mode': row.get('mode', 'NULLABLE'),
                'description': row.get('description', ''),
                'source_column': row.get('source_column', row['name']),
                'parse_logic': row.get('parse_logic', None)
            }
            schemas_map[e_type].append(field_def)

        # 4. Transform Pipelines
        pipelines_list = []
        for row in raw_pipelines:
            # Check Active Flag
            if not self._clean_bool(row.get('active', 'TRUE')):
                continue

            pipeline_def = {
                'id': row['pipeline_id'],
                'export_type': row['export_type'],
                'source_url': row['source_url'],
                'destination_table': row['destination_table'],
                'write_disposition': row.get('write_disposition', 'WRITE_APPEND'),
                # Add CSV options if they exist, otherwise defaults
                'delimiter': row.get('delimiter', ','),
                'encoding': row.get('encoding', 'utf-8'),
                'skip_leading_rows': int(row['skip_leading_rows']) if row.get('skip_leading_rows') else 1
            }
            pipelines_list.append(pipeline_def)

        return {
            'pipelines': pipelines_list,
            'schemas': schemas_map
        }

    def log_run(self, pipeline_id, status, rows_loaded=0, error_msg=""):
        """Writes execution status to the Run_Logs tab"""
        try:
            # Create tab if it doesn't exist (optional safety)
            try:
                log_ws = self.sheet.worksheet("Run_Logs")
            except:
                log_ws = self.sheet.add_worksheet(title="Run_Logs", rows=1000, cols=10)
                log_ws.append_row(["timestamp", "pipeline_id", "status", "rows_loaded", "error_message"])

            timestamp = datetime.datetime.now().isoformat()
            log_ws.append_row([
                timestamp, 
                pipeline_id, 
                status, 
                rows_loaded, 
                str(error_msg)
            ])
        except Exception as e:
            logger.warning(f"Could not write to Run_Logs: {e}")