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


@dataclass
class SchemaFieldsConfig:
    lat: Optional[str] = None
    lon: Optional[str] = None
    time: Optional[str] = None
    depth: Optional[str] = None
    entity: Optional[str] = None


@dataclass
class SchemaConfig:
    file: str
    fields: SchemaFieldsConfig = field(default_factory=SchemaFieldsConfig)


@dataclass
class DefaultsConfig:
    metric: Optional[str] = None
    group_path: Optional[str] = None


@dataclass
class LabelsConfig:
    metric: Optional[str] = None
    y_axis: Optional[str] = None
    entity: Optional[str] = None
    depth_unit: Optional[str] = None


@dataclass
class MapConfig:
    center_lat: Optional[float] = None
    center_lon: Optional[float] = None
    zoom: Optional[int] = None
    title: Optional[str] = None
    cmap: Optional[str] = None
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
    defaults: DefaultsConfig = field(default_factory=DefaultsConfig)
    labels: LabelsConfig = field(default_factory=LabelsConfig)
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


def _build_endpoint_config(endpoint_name: str, endpoint_data: Dict[str, Any]) -> EndpointConfig:
    source_data = endpoint_data.get("source")
    schema_data = endpoint_data.get("schema")
    schema_fields_data = schema_data.get("fields", {}) if isinstance(schema_data, dict) else {}
    defaults_data = endpoint_data.get("defaults", {})
    labels_data = endpoint_data.get("labels", {})
    visualization_data = endpoint_data.get("visualization", {})
    map_data = visualization_data.get("map", {})
    timeseries_data = visualization_data.get("timeseries", {})
    overlay_data = visualization_data.get("overlay", {})
    tile_build_data = endpoint_data.get("tile_build", {})

    if not isinstance(source_data, dict):
        raise ValueError(f"Endpoint '{endpoint_name}' is missing required 'source' block")

    if not isinstance(schema_data, dict):
        raise ValueError(f"Endpoint '{endpoint_name}' is missing required 'schema' block")

    required_source_fields = ["type", "store_type", "uri"]
    for field_name in required_source_fields:
        if not source_data.get(field_name):
            raise ValueError(f"Endpoint '{endpoint_name}' is missing source.{field_name}")

    if not schema_data.get("file"):
        raise ValueError(f"Endpoint '{endpoint_name}' is missing schema.file")

    return EndpointConfig(
        name=endpoint_name,
        reload_interval=endpoint_data.get("reload_interval", 300),
        description=endpoint_data.get("description", endpoint_name),
        version=endpoint_data.get("version", "1.0.0"),
        source=SourceConfig(
            type=source_data["type"],
            store_type=source_data["store_type"],
            uri=source_data["uri"],
        ),
        schema=SchemaConfig(
            file=schema_data["file"],
            fields=SchemaFieldsConfig(
                lat=schema_fields_data.get("lat"),
                lon=schema_fields_data.get("lon"),
                time=schema_fields_data.get("time"),
                depth=schema_fields_data.get("depth"),
                entity=schema_fields_data.get("entity"),
            ),
        ),
        defaults=DefaultsConfig(
            metric=defaults_data.get("metric"),
            group_path=defaults_data.get("group_path"),
        ),
        labels=LabelsConfig(
            metric=labels_data.get("metric"),
            y_axis=labels_data.get("y_axis"),
            entity=labels_data.get("entity"),
            depth_unit=labels_data.get("depth_unit"),
        ),
        visualization=VisualizationConfig(
            map=MapConfig(
                center_lat=map_data.get("center_lat"),
                center_lon=map_data.get("center_lon"),
                zoom=map_data.get("zoom"),
                title=map_data.get("title"),
                cmap=map_data.get("cmap"),
                point_size=map_data.get("point_size"),
                alpha=map_data.get("alpha"),
            ),
            timeseries=TimeSeriesConfig(
                middle_window_days=timeseries_data.get("middle_window_days"),
                right_window_hours=timeseries_data.get("right_window_hours"),
            ),
            overlay=OverlayConfig(
                enabled=overlay_data.get("enabled", False),
                image_path=overlay_data.get("image_path"),
                georef_path=overlay_data.get("georef_path"),
                tile_url=overlay_data.get("tile_url"),
            ),
        ),
        tile_build=TileBuildConfig(
            enabled=tile_build_data.get("enabled", False),
            source_image=tile_build_data.get("source_image"),
            georef_file=tile_build_data.get("georef_file"),
            vrt_file=tile_build_data.get("vrt_file"),
            warped_tif=tile_build_data.get("warped_tif"),
            rgba_vrt=tile_build_data.get("rgba_vrt"),
            tiles_dir=tile_build_data.get("tiles_dir"),
            tile_scheme=tile_build_data.get("tile_scheme"),
            min_zoom=tile_build_data.get("min_zoom"),
            max_zoom=tile_build_data.get("max_zoom"),
            target_srs=tile_build_data.get("target_srs"),
            gcp_srs=tile_build_data.get("gcp_srs"),
            resampling=tile_build_data.get("resampling"),
            add_alpha=tile_build_data.get("add_alpha"),
        ),
    )


def load_endpoints(config_path: Path) -> Dict[str, EndpointConfig]:
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file) or {}

    if not isinstance(config, dict):
        raise ValueError(f"Invalid endpoint configuration format in {config_path}")

    endpoints: Dict[str, EndpointConfig] = {}
    for endpoint_name, endpoint_data in config.items():
        if not isinstance(endpoint_data, dict):
            raise ValueError(f"Endpoint '{endpoint_name}' must be a mapping/object")

        processed_data = _process_environment_variables(endpoint_data)
        endpoints[endpoint_name] = _build_endpoint_config(endpoint_name, processed_data)

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