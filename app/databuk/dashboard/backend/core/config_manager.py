import yaml
import os
from pathlib import Path
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field

class EndpointConfig(BaseModel):
    """Pydantic model for endpoint configuration validation"""
    reload_interval: int = Field(..., gt=0, description="Reload interval in seconds")
    schema_file: str = Field(..., description="Path to schema file")
    rel_path: str = Field(..., description="Relative path to Zarr store inside bucket")
    store_url: str = Field(..., description="Full S3 store URL")
    description: str = Field(..., description="Endpoint description")
    store_type: str = Field(default="zarr", description="Store type")
    version: str = Field(default="1.0.0", description="Version")
    # S3 credentials and settings are handled by zarr_fuse.open_store logic
    # from schema, environment variables, and passed arguments

def load_endpoints(config_path: Optional[str] = None) -> Dict[str, EndpointConfig]:
    """Load endpoints from YAML config file - pure function approach"""
    if config_path is None:
        config_path = Path(__file__).parent.parent / "config" / "endpoints.yaml"
    else:
        config_path = Path(config_path)
    
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    try:
        with open(config_path, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML format: {e}")
    except Exception as e:
        raise RuntimeError(f"Failed to load configuration: {e}")
    
    endpoints = {}
    for endpoint_name, endpoint_data in config.items():
        if endpoint_name.startswith('#'):  # Skip comments
            continue
        try:
            # Process environment variables
            processed_data = _process_environment_variables(endpoint_data)
            # Compose store_url from S3_BUCKET_NAME and rel_path
            s3_bucket = os.getenv("S3_BUCKET_NAME")
            rel_path = processed_data.get("rel_path")
            if not s3_bucket:
                raise ValueError("Environment variable S3_BUCKET_NAME not found")
            if not rel_path:
                raise ValueError(f"rel_path not found for endpoint '{endpoint_name}'")
            store_url = f"s3://{s3_bucket}/{rel_path}"
            processed_data["store_url"] = store_url
            endpoint_config = EndpointConfig(**processed_data)
            endpoints[endpoint_name] = endpoint_config
        except Exception as e:
            print(f"Warning: Invalid configuration for endpoint '{endpoint_name}': {e}")
    return endpoints

def _process_environment_variables(data: Dict[str, Any]) -> Dict[str, Any]:
    """Process environment variables in configuration data"""
    processed = {}
    for key, value in data.items():
        if isinstance(value, str):
            # Replace all environment variables in the string
            processed_value = value
            while "${" in processed_value and "}" in processed_value:
                start = processed_value.find("${")
                end = processed_value.find("}", start)
                if start != -1 and end != -1:
                    env_var = processed_value[start+2:end]
                    env_value = os.getenv(env_var)
                    print(f"Processing env var: {env_var} = {env_value}")
                    if env_value is None:
                        raise ValueError(f"Environment variable {env_var} not found")
                    processed_value = processed_value.replace(f"${{{env_var}}}", env_value)
            processed[key] = processed_value
        else:
            processed[key] = value
    return processed

def get_first_endpoint(config_path: Optional[str] = None) -> Optional[EndpointConfig]:
    """Get the first available endpoint (for single endpoint mode)"""
    endpoints = load_endpoints(config_path)
    if endpoints:
        return list(endpoints.values())[0]
    return None
