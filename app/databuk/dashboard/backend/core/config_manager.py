import yaml
import os
from pathlib import Path
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field

class EndpointConfig(BaseModel):
    """Pydantic model for endpoint configuration validation"""
    reload_interval: int = Field(..., gt=0, description="Reload interval in seconds")
    schema_file: str = Field(..., description="Path to schema file")
    store_url: str = Field(..., description="S3 store URL")
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
