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
    group_fields: Dict[str, SchemaFieldsConfig] = field(default_factory=dict)


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


FIELD_NAMES = {"lat", "lon", "time", "vertical", "entity"}
REQUIRED_FIELD_NAMES = {"lat", "lon", "time", "entity"}


def _normalize_group_path(group_path: Optional[str]) -> str:
    if not group_path or group_path == "/":
        return ""
    return "/".join(part for part in str(group_path).strip("/").split("/") if part)


def _build_schema_fields(fields_data: Dict[str, Any], context: str) -> SchemaFieldsConfig:
    if not isinstance(fields_data, dict):
        raise ValueError(f"{context} must be a mapping/object")

    missing = [name for name in REQUIRED_FIELD_NAMES if name not in fields_data]
    if missing:
        raise ValueError(f"{context} is missing required keys: {', '.join(sorted(missing))}")

    return SchemaFieldsConfig(
        lat=fields_data.get("lat"),
        lon=fields_data.get("lon"),
        time=fields_data.get("time"),
        vertical=fields_data.get("vertical"),
        entity=fields_data.get("entity"),
    )


def _collect_group_fields(variable_map: Dict[str, Any], endpoint_name: str) -> Dict[str, SchemaFieldsConfig]:
    group_fields: Dict[str, SchemaFieldsConfig] = {}

    def walk(node: Any, path_parts: list[str]) -> None:
        if not isinstance(node, dict):
            return

        if FIELD_NAMES.intersection(node.keys()):
            group_path = "/".join(path_parts)
            if not group_path:
                raise ValueError(
                    f"Endpoint '{endpoint_name}' uses grouped variable_map, but a field mapping was found at the root."
                )
            group_fields[group_path] = _build_schema_fields(
                node,
                f"Endpoint '{endpoint_name}' variable_map.{group_path}",
            )
            return

        for key, value in node.items():
            if key == "fields":
                continue
            walk(value, path_parts + [key])

    walk(variable_map, [])
    return group_fields


def resolve_schema_fields(schema: SchemaConfig, group_path: Optional[str]) -> SchemaFieldsConfig:
    normalized = _normalize_group_path(group_path)
    path = normalized

    while True:
        if path in schema.group_fields:
            return schema.group_fields[path]
        if not path:
            break
        path = path.rsplit("/", 1)[0] if "/" in path else ""

    return schema.fields


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


def _read_schema_display(
    schema_path: Path,
    display_variable: Optional[str],
    entity_field: Optional[str],
    vertical_field: Optional[str],
    group_path: Optional[str],
) -> SchemaDisplayConfig:
    with schema_path.open("r", encoding="utf-8") as file:
        schema = yaml.safe_load(file)

    def _find_data_node(node: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(node, dict):
            return None
        if "VARS" in node and "COORDS" in node:
            return node
        for key, value in node.items():
            if key == "ATTRS":
                continue
            found = _find_data_node(value)
            if found is not None:
                return found
        return None

    group_data: Optional[Dict[str, Any]] = None
    path_parts = [p for p in (group_path or "").strip("/").split("/") if p]
    if path_parts:
        current: Any = schema
        for part in path_parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                current = None
                break
        group_data = _find_data_node(current)

    if group_data is None:
        group_data = _find_data_node(schema)

    if group_data is None:
        return SchemaDisplayConfig(
            display_variable=display_variable,
            display_unit=None,
            entity_name=entity_field,
            vertical_name=vertical_field,
        )

    vars_data = group_data.get("VARS", {})
    coords_data = group_data.get("COORDS", {})

    variable_data = vars_data.get(display_variable or "", {})

    entity_name = entity_field
    if entity_field and entity_field in coords_data and isinstance(coords_data[entity_field], dict):
        entity_name = coords_data[entity_field].get("df_col", entity_field)

    vertical_name = vertical_field
    if vertical_field and vertical_field in coords_data and isinstance(coords_data[vertical_field], dict):
        vertical_name = coords_data[vertical_field].get("df_col", vertical_field)

    return SchemaDisplayConfig(
        display_variable=display_variable,
        display_unit=variable_data.get("unit"),
        entity_name=entity_name,
        vertical_name=vertical_name,
    )


def _build_endpoint_config(endpoint_name: str, endpoint_data: Dict[str, Any], base_dir: Path) -> EndpointConfig:
    source_data = endpoint_data["source"]
    schema_data = endpoint_data["variable_map"]
    defaults_data = endpoint_data["defaults"]
    visualization_data = endpoint_data["visualization"]
    map_data = visualization_data["map"]
    timeseries_data = visualization_data["timeseries"]
    overlay_data = visualization_data["overlay"]
    tile_build_data = endpoint_data.get("tile_build", {"enabled": False})

    if not isinstance(schema_data, dict):
        raise ValueError(f"Endpoint '{endpoint_name}' variable_map must be a mapping/object")

    root_fields = None
    if isinstance(schema_data.get("fields"), dict):
        root_fields = _build_schema_fields(schema_data["fields"], f"Endpoint '{endpoint_name}' variable_map.fields")

    group_fields = _collect_group_fields(schema_data, endpoint_name)
    if root_fields is None and not group_fields:
        raise ValueError(
            f"Endpoint '{endpoint_name}' must define either variable_map.fields or nested group mappings."
        )

    required_source_fields = ["type", "store_type", "uri"]
    for field_name in required_source_fields:
        if not source_data.get(field_name):
            raise ValueError(f"Endpoint '{endpoint_name}' is missing source.{field_name}")

    schema_file = source_data["schema_path"]

    schema_file_path = Path(schema_file)
    if not schema_file_path.is_absolute():
        schema_file_path = base_dir / schema_file_path

    schema_for_display = SchemaConfig(
        file=schema_file,
        fields=root_fields or SchemaFieldsConfig(),
        group_fields=group_fields,
    )
    selected_fields = resolve_schema_fields(schema_for_display, defaults_data.get("group_path"))
    schema_display = _read_schema_display(
        schema_file_path,
        defaults_data.get("display_variable"),
        selected_fields.entity,
        selected_fields.vertical,
        defaults_data.get("group_path"),
    )

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
            fields=root_fields or SchemaFieldsConfig(),
            group_fields=group_fields,
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
                tile_url=overlay_data.get("tile_url"),
            ),
        ),
        tile_build=TileBuildConfig(
            enabled=tile_build_data["enabled"],
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
