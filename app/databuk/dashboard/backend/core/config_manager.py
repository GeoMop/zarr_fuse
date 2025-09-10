import yaml
import os
from pathlib import Path
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field

class EndpointConfig(BaseModel):
    """Pydantic model for endpoint configuration validation"""
    Reload_interval: int = Field(..., gt=0, description="Reload interval in seconds")
    Schema_file: str = Field(..., description="Path to schema file")
    STORE_URL: str = Field(..., description="S3 store URL")
    S3_ENDPOINT_URL: str = Field(..., description="S3 endpoint URL")
    S3_access_key: str = Field(..., description="S3 access key")
    S3_secret_key: str = Field(..., description="S3 secret key")
    S3_region: Optional[str] = Field(default="us-east-1", description="S3 region")
    S3_use_ssl: bool = Field(default=True, description="Use SSL for S3")
    S3_verify_ssl: bool = Field(default=True, description="Verify SSL for S3")
    Description: str = Field(..., description="Endpoint description")
    Store_type: str = Field(default="zarr", description="Store type")
    Version: str = Field(default="1.0.0", description="Version")

class ConfigManager:
    """Manages YAML configuration for S3 endpoints"""
    
    def __init__(self, config_path: Optional[str] = None):
        if config_path is None:
            # Default to backend/config/endpoints.yaml
            self.config_path = Path(__file__).parent.parent / "config" / "endpoints.yaml"
        else:
            self.config_path = Path(config_path)
        
        self._config_cache: Optional[Dict[str, Any]] = None
        self._endpoints_cache: Optional[Dict[str, EndpointConfig]] = None
    
    def load_config(self) -> Dict[str, Any]:
        """Load YAML configuration file"""
        if self._config_cache is not None:
            return self._config_cache
        
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        try:
            with open(self.config_path, 'r', encoding='utf-8') as file:
                config = yaml.safe_load(file)
                self._config_cache = config
                return config
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML format: {e}")
        except Exception as e:
            raise RuntimeError(f"Failed to load configuration: {e}")
    
    def get_endpoints(self) -> Dict[str, EndpointConfig]:
        """Get all endpoints with validation"""
        if self._endpoints_cache is not None:
            return self._endpoints_cache
        
        config = self.load_config()
        endpoints = {}
        
        for endpoint_name, endpoint_data in config.items():
            if endpoint_name.startswith('#'):  # Skip comments
                continue
            try:
                # Replace environment variables
                processed_data = self._process_environment_variables(endpoint_data)
                endpoint_config = EndpointConfig(**processed_data)
                endpoints[endpoint_name] = endpoint_config
            except Exception as e:
                print(f"Warning: Invalid configuration for endpoint '{endpoint_name}': {e}")
        
        self._endpoints_cache = endpoints
        return endpoints
    
    def _process_environment_variables(self, data: Dict[str, Any]) -> Dict[str, Any]:
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
    
    def get_endpoint(self, endpoint_name: str) -> Optional[EndpointConfig]:
        """Get specific endpoint configuration"""
        endpoints = self.get_endpoints()
        return endpoints.get(endpoint_name)
    
    def get_first_endpoint(self) -> Optional[EndpointConfig]:
        """Get the first available endpoint (for single endpoint mode)"""
        endpoints = self.get_endpoints()
        if endpoints:
            return list(endpoints.values())[0]
        return None
    
    def reload_config(self):
        """Reload configuration from file"""
        self._config_cache = None
        self._endpoints_cache = None
        return self.load_config()

# Global config manager instance
config_manager = ConfigManager()
