import yaml
import logging
from google.cloud import storage
from config_loader import ConfigProvider

logger = logging.getLogger(__name__)

class GCSYamlConfigProvider(ConfigProvider):
    def __init__(self, bucket_name, prefix):
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)
        self.prefix = prefix

    def get_configs(self):
        logger.info(f"Loading YAMLs from GCS: gs://{self.bucket.name}/{self.prefix}")
        
        pipelines = []
        schemas = {}

        # List all blobs
        blobs = self.client.list_blobs(self.bucket, prefix=self.prefix)
        
        for blob in blobs:
            if not blob.name.endswith(('.yaml', '.yml')):
                continue
                
            try:
                content = blob.download_as_text()
                data = yaml.safe_load(content)
                
                if not data: continue

                # LOGIC TO IDENTIFY FILE TYPE
                # (You may need to adjust this matching logic based on your exact YAML structure)
                
                # Case 1: It's a Pipeline Config (usually contains 'source_url' or is a list)
                if isinstance(data, list):
                    pipelines.extend(data)
                elif 'source_url' in data or 'pipeline_id' in data:
                    pipelines.append(data)
                
                # Case 2: It's a Schema Config (usually contains 'fields' or 'schema')
                elif 'fields' in data and 'export_type' in data:
                    export_type = data['export_type']
                    schemas[export_type] = data['fields']
                
            except Exception as e:
                logger.error(f"Error parsing YAML {blob.name}: {e}")

        return {
            'pipelines': pipelines,
            'schemas': schemas
        }