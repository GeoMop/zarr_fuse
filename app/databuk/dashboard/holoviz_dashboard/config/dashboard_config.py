import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import yaml


@dataclass
class EndpointConfig:
    reload_interval: int
    schema_file: str
    store_url: str
    description: str
    store_type: str = "zarr"
    version: str = "1.0.0"


def _process_environment_variables(data: Dict[str, Any]) -> Dict[str, Any]:
    processed: Dict[str, Any] = {}
    for key, value in data.items():
        if isinstance(value, str):
            processed_value = value
            while "${" in processed_value and "}" in processed_value:
                start = processed_value.find("${")
                end = processed_value.find("}", start)
                if start != -1 and end != -1:
                    env_var = processed_value[start + 2:end]
                    env_value = os.getenv(env_var)
                    if env_value is None:
                        raise ValueError(f"Environment variable {env_var} not found")
                    processed_value = processed_value.replace(f"${{{env_var}}}", env_value)
            processed[key] = processed_value
        else:
            processed[key] = value
    return processed


def load_endpoints(config_path: Path) -> Dict[str, EndpointConfig]:
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file) or {}

    endpoints: Dict[str, EndpointConfig] = {}
    for endpoint_name, endpoint_data in config.items():
        if isinstance(endpoint_name, str) and endpoint_name.startswith("#"):
            continue
        processed_data = _process_environment_variables(endpoint_data)
        endpoints[endpoint_name] = EndpointConfig(**processed_data)
    return endpoints


def get_endpoint_config(config_path: Path, endpoint_name: Optional[str] = None) -> EndpointConfig:
    endpoints = load_endpoints(config_path)
    if not endpoints:
        raise ValueError("No endpoints configured")

    if endpoint_name is None:
        return next(iter(endpoints.values()))

    if endpoint_name not in endpoints:
        raise KeyError(f"Endpoint '{endpoint_name}' not found in {config_path}")

    return endpoints[endpoint_name]
