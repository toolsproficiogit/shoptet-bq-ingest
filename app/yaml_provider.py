"""
Improved YAML Configuration Provider

This provider loads pipeline and schema configurations from YAML files in GCS.
It correctly handles the YAML structure used by the optimized service.

Expected YAML Structure:

config.yaml:
  pipelines:
    - id: pipeline_1
      export_type: orders
      csv_url: https://...
      bq_table_id: project.dataset.table
      ...

schemas.yaml:
  orders:
    - name: id
      type: STRING
      source: id
      ...
  customers:
    - name: id
      type: STRING
      source: id
      ...
"""

import yaml
import logging
from google.cloud import storage
from config_loader import ConfigProvider

logger = logging.getLogger(__name__)


class GCSYamlConfigProvider(ConfigProvider):
    """Load pipeline and schema configs from YAML files in GCS."""
    
    def __init__(self, bucket_name: str, prefix: str = "configs/"):
        """
        Initialize the YAML config provider.
        
        Args:
            bucket_name: GCS bucket name
            prefix: Path prefix for config files (default: configs/)
        """
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)
        self.prefix = prefix
        self.bucket_name = bucket_name

    def _load_yaml_file(self, blob_name: str) -> dict:
        """Load and parse a YAML file from GCS."""
        try:
            blob = self.bucket.blob(blob_name)
            content = blob.download_as_text()
            data = yaml.safe_load(content)
            logger.info(f"Loaded YAML file: {blob_name}")
            return data or {}
        except Exception as e:
            logger.error(f"Error loading YAML file {blob_name}: {e}")
            raise

    def _load_pipelines(self) -> list:
        """Load pipeline configurations from config.yaml."""
        pipelines = []
        
        # Try to load config.yaml
        config_file = f"{self.prefix}config.yaml"
        try:
            data = self._load_yaml_file(config_file)
            
            if not data:
                logger.warning(f"Config file {config_file} is empty")
                return pipelines
            
            # Extract pipelines from the config
            if isinstance(data, dict) and "pipelines" in data:
                pipelines_data = data["pipelines"]
                if isinstance(pipelines_data, list):
                    pipelines.extend(pipelines_data)
                    logger.info(f"Loaded {len(pipelines)} pipelines from {config_file}")
                else:
                    logger.error(f"Expected 'pipelines' to be a list in {config_file}")
            else:
                logger.error(f"Expected 'pipelines' key in {config_file}")
        
        except Exception as e:
            logger.error(f"Failed to load pipelines from {config_file}: {e}")
            raise
        
        return pipelines

    def _load_schemas(self) -> dict:
        """Load schema configurations from schemas.yaml."""
        schemas = {}
        
        # Try to load schemas.yaml
        schema_file = f"{self.prefix}schemas.yaml"
        try:
            data = self._load_yaml_file(schema_file)
            
            if not data:
                logger.warning(f"Schema file {schema_file} is empty")
                return schemas
            
            # Extract schemas from the config
            if isinstance(data, dict) and "schemas" in data:
                schemas_data = data["schemas"]
                if isinstance(schemas_data, dict):
                    # Validate each schema
                    for export_type, schema_fields in schemas_data.items():
                        if isinstance(schema_fields, list):
                            schemas[export_type] = schema_fields
                            logger.info(f"Loaded schema for export_type '{export_type}' with {len(schema_fields)} fields")
                        else:
                            logger.error(f"Expected schema for '{export_type}' to be a list")
                else:
                    logger.error(f"Expected 'schemas' to be a dict in {schema_file}")
            else:
                logger.error(f"Expected 'schemas' key in {schema_file}")
        
        except Exception as e:
            logger.error(f"Failed to load schemas from {schema_file}: {e}")
            raise
        
        return schemas

    def get_configs(self) -> dict:
        """
        Load all configurations.
        
        Returns:
            Dictionary with 'pipelines' and 'schemas' keys
        """
        logger.info(f"Loading YAMLs from GCS: gs://{self.bucket_name}/{self.prefix}")
        
        pipelines = self._load_pipelines()
        schemas = self._load_schemas()
        
        logger.info(f"Loaded {len(pipelines)} pipelines and {len(schemas)} schema definitions")
        
        return {
            "pipelines": pipelines,
            "schemas": schemas
        }

    def validate_configs(self) -> list:
        """
        Validate loaded configurations.
        
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        
        try:
            config_data = self.get_configs()
            pipelines = config_data.get("pipelines", [])
            schemas = config_data.get("schemas", {})
            
            # Validate pipelines
            for i, pipeline in enumerate(pipelines):
                if not isinstance(pipeline, dict):
                    errors.append(f"Pipeline {i} is not a dictionary")
                    continue
                
                required_fields = ["id", "export_type", "csv_url", "bq_table_id"]
                for field in required_fields:
                    if field not in pipeline:
                        errors.append(f"Pipeline {i} ({pipeline.get('id', 'unknown')}) missing required field: {field}")
                
                # Check if schema exists for this export_type
                export_type = pipeline.get("export_type")
                if export_type and export_type not in schemas:
                    errors.append(f"Pipeline {i} ({pipeline.get('id')}) references missing schema: {export_type}")
            
            # Validate schemas
            for export_type, schema_fields in schemas.items():
                if not isinstance(schema_fields, list):
                    errors.append(f"Schema '{export_type}' is not a list")
                    continue
                
                for j, field in enumerate(schema_fields):
                    if not isinstance(field, dict):
                        errors.append(f"Schema '{export_type}' field {j} is not a dictionary")
                        continue
                    
                    if "name" not in field:
                        errors.append(f"Schema '{export_type}' field {j} missing 'name'")
                    if "type" not in field:
                        errors.append(f"Schema '{export_type}' field {j} missing 'type'")
        
        except Exception as e:
            errors.append(f"Validation error: {e}")
        
        return errors