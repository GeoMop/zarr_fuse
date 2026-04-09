import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

import yaml


@dataclass
class SourceConfig:
    type: str
    store_type: str
    uri: str
    schema_path: Optional[str] = None


@dataclass
class SchemaFieldsConfig:
    lat: Optional[str] = None
    lon: Optional[str] = None
    time: Optional[str] = None
    vertical: Optional[str] = None
    entity: Optional[str] = None


@dataclass
class SchemaConfig:
    file: str
    fields: SchemaFieldsConfig = field(default_factory=SchemaFieldsConfig)


@dataclass
class SchemaDisplayConfig:
    display_variable: Optional[str] = None
    display_unit: Optional[str] = None
    entity_name: Optional[str] = None
    vertical_name: Optional[str] = None


@dataclass
class DefaultsConfig:
    display_variable: Optional[str] = None
    group_path: Optional[str] = None


@dataclass
class MapConfig:
    center_lat: Optional[float] = None
    center_lon: Optional[float] = None
    zoom: Optional[int] = None
    title: Optional[str] = None
    point_size: Optional[int] = None
    alpha: Optional[float] = None


@dataclass
class TimeSeriesConfig:
    middle_window_days: Optional[int] = None
    right_window_hours: Optional[int] = None


@dataclass
class OverlayConfig:
    enabled: bool = False
    image_path: Optional[str] = None
    georef_path: Optional[str] = None
    tile_url: Optional[str] = None


@dataclass
class TileBuildConfig:
    enabled: bool = False
    source_image: Optional[str] = None
    georef_file: Optional[str] = None
    vrt_file: Optional[str] = None
    warped_tif: Optional[str] = None
    rgba_vrt: Optional[str] = None
    tiles_dir: Optional[str] = None
    tile_scheme: Optional[str] = None
    min_zoom: Optional[int] = None
    max_zoom: Optional[int] = None
    target_srs: Optional[str] = None
    gcp_srs: Optional[str] = None
    resampling: Optional[str] = None
    add_alpha: Optional[bool] = None


@dataclass
class VisualizationConfig:
    map: MapConfig = field(default_factory=MapConfig)
    timeseries: TimeSeriesConfig = field(default_factory=TimeSeriesConfig)
    overlay: OverlayConfig = field(default_factory=OverlayConfig)


@dataclass
class EndpointConfig:
    name: str
    reload_interval: int
    description: str
    version: str
    source: SourceConfig
    schema: SchemaConfig
    schema_display: SchemaDisplayConfig = field(default_factory=SchemaDisplayConfig)
    defaults: DefaultsConfig = field(default_factory=DefaultsConfig)
    visualization: VisualizationConfig = field(default_factory=VisualizationConfig)
    tile_build: TileBuildConfig = field(default_factory=TileBuildConfig)


def _process_environment_variables(data: Any) -> Any:
    if isinstance(data, dict):
        return {key: _process_environment_variables(value) for key, value in data.items()}

    if isinstance(data, list):
        return [_process_environment_variables(item) for item in data]

    if isinstance(data, str):
        processed_value = data
        while "${" in processed_value and "}" in processed_value:
            start = processed_value.find("${")
            end = processed_value.find("}", start)
            if start != -1 and end != -1:
                env_var = processed_value[start + 2:end]
                env_value = os.getenv(env_var)
                if env_value is None:
                    raise ValueError(f"Environment variable {env_var} not found")
                processed_value = processed_value.replace(f"${{{env_var}}}", env_value)
        return processed_value

    return data


def _read_schema_display(schema_path: Path, display_variable: Optional[str]) -> SchemaDisplayConfig:
    with schema_path.open("r", encoding="utf-8") as file:
        schema = yaml.safe_load(file)

    group_name = next(key for key in schema.keys() if key != "ATTRS")

    group_data = schema[group_name]
    vars_data = group_data["VARS"]
    coords_data = group_data["COORDS"]

    variable_data = vars_data[display_variable]
    entity_data = coords_data["borehole"]
    vertical_data = coords_data["depth"]

    return SchemaDisplayConfig(
        display_variable=display_variable,
        display_unit=variable_data["unit"],
        entity_name=entity_data["df_col"],
        vertical_name=vertical_data["df_col"],
    )


def _build_endpoint_config(endpoint_name: str, endpoint_data: Dict[str, Any], base_dir: Path) -> EndpointConfig:
    source_data = endpoint_data["source"]
    schema_data = endpoint_data["variable_map"]
    schema_fields_data = schema_data["fields"]
    defaults_data = endpoint_data["defaults"]
    visualization_data = endpoint_data["visualization"]
    map_data = visualization_data["map"]
    timeseries_data = visualization_data["timeseries"]
    overlay_data = visualization_data["overlay"]
    tile_build_data = endpoint_data["tile_build"]

    required_source_fields = ["type", "store_type", "uri"]
    for field_name in required_source_fields:
        if not source_data.get(field_name):
            raise ValueError(f"Endpoint '{endpoint_name}' is missing source.{field_name}")

    schema_file = source_data["schema_path"]

    schema_file_path = Path(schema_file)
    if not schema_file_path.is_absolute():
        schema_file_path = base_dir / schema_file_path

    schema_display = _read_schema_display(schema_file_path, defaults_data.get("display_variable"))

    return EndpointConfig(
        name=endpoint_name,
        reload_interval=endpoint_data["reload_interval"],
        description=endpoint_data["description"],
        version=endpoint_data["version"],
        source=SourceConfig(
            type=source_data["type"],
            store_type=source_data["store_type"],
            uri=source_data["uri"],
            schema_path=schema_file,
        ),
        schema=SchemaConfig(
            file=schema_file,
            fields=SchemaFieldsConfig(
                lat=schema_fields_data["lat"],
                lon=schema_fields_data["lon"],
                time=schema_fields_data["time"],
                vertical=schema_fields_data["vertical"],
                entity=schema_fields_data["entity"],
            ),
        ),
        schema_display=schema_display,
        defaults=DefaultsConfig(
            display_variable=defaults_data["display_variable"],
            group_path=defaults_data["group_path"],
        ),
        visualization=VisualizationConfig(
            map=MapConfig(
                center_lat=map_data["center_lat"],
                center_lon=map_data["center_lon"],
                zoom=map_data["zoom"],
                title=map_data["title"],
                point_size=map_data["point_size"],
                alpha=map_data["alpha"],
            ),
            timeseries=TimeSeriesConfig(
                middle_window_days=timeseries_data["middle_window_days"],
                right_window_hours=timeseries_data["right_window_hours"],
            ),
            overlay=OverlayConfig(
                enabled=overlay_data["enabled"],
                image_path=overlay_data["image_path"],
                georef_path=overlay_data["georef_path"],
                tile_url=overlay_data["tile_url"],
            ),
        ),
        tile_build=TileBuildConfig(
            enabled=tile_build_data["enabled"],
            source_image=tile_build_data["source_image"],
            georef_file=tile_build_data["georef_file"],
            vrt_file=tile_build_data["vrt_file"],
            warped_tif=tile_build_data["warped_tif"],
            rgba_vrt=tile_build_data["rgba_vrt"],
            tiles_dir=tile_build_data["tiles_dir"],
            tile_scheme=tile_build_data["tile_scheme"],
            min_zoom=tile_build_data["min_zoom"],
            max_zoom=tile_build_data["max_zoom"],
            target_srs=tile_build_data["target_srs"],
            gcp_srs=tile_build_data["gcp_srs"],
            resampling=tile_build_data["resampling"],
            add_alpha=tile_build_data["add_alpha"],
        ),
    )


def load_endpoints(config_path: Path) -> Dict[str, EndpointConfig]:
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    if not isinstance(config, dict):
        raise ValueError(f"Invalid endpoint configuration format in {config_path}")

    base_dir = config_path.parent.parent
    endpoints: Dict[str, EndpointConfig] = {}
    for endpoint_name, endpoint_data in config.items():
        if not isinstance(endpoint_data, dict):
            raise ValueError(f"Endpoint '{endpoint_name}' must be a mapping/object")

        processed_data = _process_environment_variables(endpoint_data)
        endpoints[endpoint_name] = _build_endpoint_config(endpoint_name, processed_data, base_dir)

    return endpoints


def get_endpoint_config(config_path: Path, endpoint_name: Optional[str] = None) -> EndpointConfig:
    endpoints = load_endpoints(config_path)

    if not endpoints:
        raise ValueError("No endpoints configured")

    if not endpoint_name:
        raise ValueError("endpoint_name is required; implicit default endpoint is not allowed")

    if endpoint_name not in endpoints:
        raise KeyError(f"Endpoint '{endpoint_name}' not found in {config_path}")

    return endpoints[endpoint_name]