import os
from abc import ABC, abstractmethod

# Abstract Base Class defines what a Config Provider must do
class ConfigProvider(ABC):
    @abstractmethod
    def get_configs(self):
        """
        Must return a dict:
        {
            'pipelines': [ {id: '..', export_type: '..', ...}, ... ],
            'schemas': { 'export_type_key': [ {name: '..', type: '..'}, ... ] }
        }
        """
        pass

# Factory to choose the correct provider
def get_config_provider():
    use_sheets = os.getenv('USE_SOURCE_SHEETS', 'FALSE').upper() == 'TRUE'
    
    if use_sheets:
        sheet_id = os.getenv('CONFIG_SHEET_ID')
        if not sheet_id:
            raise ValueError("USE_SOURCE_SHEETS is True, but CONFIG_SHEET_ID is missing.")
        
        # Import here to avoid dependencies if not used
        from sheets_provider import GoogleSheetsConfigProvider
        return GoogleSheetsConfigProvider(sheet_id)
    else:
        bucket = os.getenv('GCS_BUCKET_NAME')
        prefix = os.getenv('GCS_CONFIG_PATH', 'configs/')
        
        from yaml_provider import GCSYamlConfigProvider
        return GCSYamlConfigProvider(bucket, prefix)