import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from dotenv import load_dotenv

VIEWS_ENV_VAR = "ZF_VIEW_PATH"
LEGACY_ENDPOINTS_ENV_VAR = "ENDPOINTS_PATH"
SCHEMAS_ENV_VAR = "SCHEMAS_PATH"

# ---------------------------------------------------------------------------
# Single-parse cache for zf_view.yaml
# ---------------------------------------------------------------------------
_view_config_cache: dict[Path, dict] = {}


def _parse_view_config(config_path: Path) -> dict:
    """Parse zf_view.yaml once and cache the result for the process lifetime."""
    key = config_path.resolve()
    if key not in _view_config_cache:
        with key.open("r", encoding="utf-8") as f:
            _view_config_cache[key] = yaml.safe_load(f) or {}
    return _view_config_cache[key]


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
    cluster_enabled: bool = True
    cluster_eps_factor: float = 0.05
    cluster_buffer_factor: float = 0.1
    cluster_size_scale: float = 3.0


@dataclass
class TimeSeriesConfig:
    middle_window_days: Optional[int] = None
    right_window_hours: Optional[int] = None


@dataclass
class OverlayConfig:
    enabled: bool = False
    tile_url: Optional[str] = None


@dataclass
class TileS3Config:
    bucket: Optional[str] = None
    prefix: Optional[str] = None


@dataclass
class TileBuildConfig:
    source_image: Optional[str] = None
    georef_file: Optional[str] = None
    vrt_file: Optional[str] = None
    warped_tif: Optional[str] = None
    rgba_vrt: Optional[str] = None
    tiles_dir: Optional[str] = None
    min_zoom: int = 0
    max_zoom: int = 20
    target_srs: str = "EPSG:3857"
    gcp_srs: str = "EPSG:4326"
    resampling: str = "near"
    s3: TileS3Config = field(default_factory=TileS3Config)


@dataclass
class VisualizationConfig:
    map: MapConfig = field(default_factory=MapConfig)
    timeseries: TimeSeriesConfig = field(default_factory=TimeSeriesConfig)
    overlay: OverlayConfig = field(default_factory=OverlayConfig)


@dataclass
class ViewConfig:
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


def find_view_file() -> Path:
    """Locate the zf_view.yaml file and return its absolute path.

    Resolution order:
    1. ZF_VIEW_PATH env var
    2. ENDPOINTS_PATH env var (deprecated fallback)
    3. Search upward from current working directory for:
       - dashboard/config/zf_view.yaml
       - config/zf_view.yaml
       - app/databuk/config/zf_view.yaml
    """
    env_path = os.getenv(VIEWS_ENV_VAR)
    env_label = VIEWS_ENV_VAR

    if not env_path:
        env_path = os.getenv(LEGACY_ENDPOINTS_ENV_VAR)
        env_label = LEGACY_ENDPOINTS_ENV_VAR
        if env_path:
            print("[config] find_view_file: ENDPOINTS_PATH is deprecated; use ZF_VIEW_PATH")

    if env_path:
        path = Path(env_path).expanduser().resolve()
        print(
            f"[config] find_view_file: "
            f"from ENV {env_label}={env_path} -> {path}"
        )

        if not path.exists():
            raise FileNotFoundError(
                f"{env_label} does not exist: {path}"
            )

        return path

    cwd = Path.cwd().resolve()
    for base in [cwd, *cwd.parents]:
        for candidate in (
            base / "dashboard" / "config" / "zf_view.yaml",
            base / "config" / "zf_view.yaml",
            base / "app" / "databuk" / "config" / "zf_view.yaml",
        ):
            if candidate.exists():
                return candidate

    raise FileNotFoundError(
        "Could not find zf_view.yaml. Checked:\n"
        "1. ZF_VIEW_PATH env var\n"
        "2. dashboard/config/zf_view.yaml\n"
        "3. config/zf_view.yaml\n"
        "4. app/databuk/config/zf_view.yaml"
    )


def _resolve_env_file_path(config_path: Path, env_file: str) -> Path:
    path = Path(env_file).expanduser()
    if path.is_absolute():
        return path

    base_dir = config_path.parent.parent
    return (base_dir / path).resolve()


def load_environment_from_config(config_path: Path) -> Path | None:
    """Load the .env file referenced by the _dashboard.env_file config section."""
    if not config_path.exists():
        return None

    config = _parse_view_config(config_path)

    dashboard_meta = config.get("_dashboard")
    if not isinstance(dashboard_meta, dict):
        return None
    env_file = dashboard_meta.get("env_file")
    if not isinstance(env_file, str) or not env_file.strip():
        return None

    env_path = _resolve_env_file_path(config_path, env_file.strip())
    if not env_path.exists():
        print(f"Configured env_file does not exist: {env_path}; continuing with existing environment variables")
        return None

    load_dotenv(env_path, override=False)
    return env_path


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


def _collect_group_fields(variable_map: Dict[str, Any], view_name: str) -> Dict[str, SchemaFieldsConfig]:
    group_fields: Dict[str, SchemaFieldsConfig] = {}

    def walk(node: Any, path_parts: list[str]) -> None:
        if not isinstance(node, dict):
            return

        if FIELD_NAMES.intersection(node.keys()):
            group_path = "/".join(path_parts)
            if not group_path:
                raise ValueError(
                    f"View '{view_name}' uses grouped variable_map, but a field mapping was found at the root."
                )
            group_fields[group_path] = _build_schema_fields(
                node,
                f"View '{view_name}' variable_map.{group_path}",
            )
            return

        for key, value in node.items():
            if key == "fields":
                continue
            walk(value, path_parts + [key])

    walk(variable_map, [])
    return group_fields


def _resolve_fields_for_group_raw(schema_config: dict, group_path: str | None) -> dict:
    """Resolve the effective fields dict for a group path by walking upward.

    This is the raw-dict version (used at runtime with untyped view config).
    The typed counterpart is :func:`resolve_schema_fields`.
    """
    fields = schema_config.get("fields", {})
    group_fields = schema_config.get("group_fields", {})
    normalized = "/".join(part for part in (group_path or "").strip("/").split("/") if part)

    path = normalized
    while True:
        if path in group_fields:
            return group_fields[path]
        if not path:
            break
        path = path.rsplit("/", 1)[0] if "/" in path else ""

    return fields


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
    print(f"[config] _read_schema_display: opening schema_path={schema_path} exists={schema_path.exists()}")
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


def read_variable_metadata(
    schema_path: Path,
    variable_name: str,
    group_path: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
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
        return None

    vars_data = group_data.get("VARS", {})
    variable_data = vars_data.get(variable_name)
    if not variable_data:
        return None

    coords = variable_data.get("coords", [])
    if isinstance(coords, str):
        coords = [coords]

    return {
        "name": variable_name,
        "description": variable_data.get("description", ""),
        "unit": variable_data.get("unit", ""),
        "coords": coords,
        "df_col": variable_data.get("df_col", ""),
    }


def _build_view_config(view_name: str, view_data: Dict[str, Any], base_dir: Path) -> ViewConfig:
    source_data = view_data["source"]
    schema_data = view_data["variable_map"]
    defaults_data = view_data["defaults"]
    visualization_data = view_data["visualization"]
    map_data = visualization_data["map"]
    timeseries_data = visualization_data["timeseries"]
    overlay_data = visualization_data["overlay"]
    tile_build_data = view_data.get("tile_build", {"enabled": False})

    if not isinstance(schema_data, dict):
        raise ValueError(f"View '{view_name}' variable_map must be a mapping/object")

    root_fields = None
    if isinstance(schema_data.get("fields"), dict):
        root_fields = _build_schema_fields(schema_data["fields"], f"View '{view_name}' variable_map.fields")

    group_fields = _collect_group_fields(schema_data, view_name)
    if root_fields is None and not group_fields:
        raise ValueError(
            f"View '{view_name}' must define either variable_map.fields or nested group mappings."
        )

    required_source_fields = ["type", "store_type", "uri"]
    for field_name in required_source_fields:
        if not source_data.get(field_name):
            raise ValueError(f"View '{view_name}' is missing source.{field_name}")

    schema_file = source_data["schema_path"]
    schemas_path = os.getenv(SCHEMAS_ENV_VAR)

    if schemas_path:
        schemas_directory = Path(
            schemas_path
        ).expanduser().resolve()

        if not schemas_directory.is_dir():
            raise FileNotFoundError(
                f"{SCHEMAS_ENV_VAR} is not a directory: "
                f"{schemas_directory}"
            )

        schema_file_path = (
            schemas_directory / Path(schema_file).name
        )
    else:
        schema_file_path = Path(schema_file)

        if not schema_file_path.is_absolute():
            schema_file_path = base_dir / schema_file_path

    if not schema_file_path.is_file():
        raise FileNotFoundError(
            f"Schema file does not exist: {schema_file_path}"
        )

    print(
        f"[config] view={view_name} "
        f"schema_path(raw)={schema_file} "
        f"base_dir={base_dir} "
        f"resolved={schema_file_path}"
    )

    schema_for_display = SchemaConfig(
        file=str(schema_file_path),
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

    # Support both flattened cluster keys and nested `cluster` mapping in zf_view.yaml
    cluster_section = map_data.get("cluster") if isinstance(map_data.get("cluster"), dict) else {}

    def _cluster_get(key, default):
        return cluster_section.get(key, map_data.get(key, default))

    return ViewConfig(
        name=view_name,
        reload_interval=view_data["reload_interval"],
        description=view_data["description"],
        version=view_data["version"],
        source=SourceConfig(
            type=source_data["type"],
            store_type=source_data["store_type"],
            uri=source_data["uri"],
            schema_path=schema_file,
        ),
        schema=SchemaConfig(
            file=str(schema_file_path),
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
                center_lat=map_data.get("center_lat"),
                center_lon=map_data.get("center_lon"),
                zoom=map_data.get("zoom"),
                title=map_data["title"],
                point_size=map_data["point_size"],
                alpha=map_data["alpha"],
                cluster_enabled=_cluster_get("cluster_enabled", True),
                cluster_eps_factor=_cluster_get("cluster_eps_factor", 0.05),
                cluster_buffer_factor=_cluster_get("cluster_buffer_factor", 0.1),
                cluster_size_scale=_cluster_get("cluster_size_scale", 3.0),
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
            source_image=tile_build_data.get("source_image"),
            georef_file=tile_build_data.get("georef_file"),
            vrt_file=tile_build_data.get("vrt_file"),
            warped_tif=tile_build_data.get("warped_tif"),
            rgba_vrt=tile_build_data.get("rgba_vrt"),
            tiles_dir=tile_build_data.get("tiles_dir"),
            min_zoom=tile_build_data.get("min_zoom", 0),
            max_zoom=tile_build_data.get("max_zoom", 20),
            target_srs=tile_build_data.get("target_srs", "EPSG:3857"),
            gcp_srs=tile_build_data.get("gcp_srs", "EPSG:4326"),
            resampling=tile_build_data.get("resampling", "near"),
            s3=_build_tile_s3_config(tile_build_data.get("s3")),
        ),
    )


def _build_tile_s3_config(s3_data: Any) -> TileS3Config:
    """Build the nested tile_build.s3 publish-target config (bucket, prefix)."""
    if not isinstance(s3_data, dict):
        return TileS3Config()
    return TileS3Config(
        bucket=s3_data.get("bucket"),
        prefix=s3_data.get("prefix"),
    )


def load_views(config_path: Path) -> Dict[str, ViewConfig]:
    """Load and validate all views from zf_view.yaml into typed config objects."""
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    config = _parse_view_config(config_path)

    if not isinstance(config, dict):
        raise ValueError(f"Invalid view configuration format in {config_path}")

    base_dir = config_path.parent.parent
    print(f"[config] load_views: config_path={config_path} config_path.parent={config_path.parent} base_dir={base_dir}")

    views: Dict[str, ViewConfig] = {}
    for view_name, view_data in config.items():
        if view_name == "env_file" or (isinstance(view_name, str) and view_name.startswith("_")):
            continue

        if not isinstance(view_data, dict):
            raise ValueError(f"View '{view_name}' must be a mapping/object")

        processed_data = _process_environment_variables(view_data)
        views[view_name] = _build_view_config(view_name, processed_data, base_dir)

    return views


def get_default_endpoint_name(config_path: Path) -> Optional[str]:
    """Return the default endpoint name from the _dashboard section, if configured."""
    if not config_path.exists():
        return None

    config = _parse_view_config(config_path)

    meta = config.get("_dashboard")
    if not isinstance(meta, dict):
        return None

    default_endpoint = meta.get("default_endpoint")
    if not isinstance(default_endpoint, str):
        return None

    view_config = config.get(default_endpoint)
    if isinstance(view_config, dict):
        return default_endpoint

    return None


def load_view_config(config_path: Path, view_name: Optional[str] = None) -> ViewConfig:
    """Load ONE view config; falls back to the configured default view name."""
    views = load_views(config_path)

    if not views:
        raise ValueError(f"No views configured in {config_path}")

    name = view_name or get_default_endpoint_name(config_path)
    if not name:
        raise ValueError("view_name required; no default view configured")

    if name not in views:
        raise KeyError(f"View '{name}' not found in {config_path}")

    return views[name]


def schema_endpoint_url(config_path: Path, view_name: Optional[str] = None) -> Optional[str]:
    """Resolve the S3 endpoint URL strictly from the view schema file.

    The S3 endpoint URL is owned by the schema (ATTRS.S3_ENDPOINT_URL); no
    environment fallback is applied here. When ``view_name`` is omitted the
    configured default view is used.
    """
    if not config_path.exists():
        return None

    try:
        view = load_view_config(config_path, view_name)
    except (ValueError, KeyError):
        return None

    schema_path = Path(view.schema.file)
    if not schema_path.exists():
        return None

    try:
        with schema_path.open("r", encoding="utf-8") as f:
            schema = yaml.safe_load(f) or {}
    except Exception:
        return None

    attrs = schema.get("ATTRS")
    if not isinstance(attrs, dict):
        return None

    endpoint_url = attrs.get("S3_ENDPOINT_URL")
    if not isinstance(endpoint_url, str) or not endpoint_url.strip():
        return None
    return endpoint_url.strip()


def overlay_enabled(config_path: Path, view_name: Optional[str] = None) -> bool:
    """Check whether the overlay/tile feature is enabled for the given view.

    Resolution order (mirrors the original map_views logic):
    1. ``HV_OVERLAY_ENABLED`` env var — if set to a false-y value (``0``,
       ``false``, ``no``), overlay is disabled regardless of the view config.
    2. ``visualization.overlay.enabled`` in the view config.
    3. Returns ``False`` on any missing/invalid config or file error.
    """
    raw = os.getenv("HV_OVERLAY_ENABLED", "1")
    if raw.strip().lower() in {"0", "false", "no"}:
        return False

    if not config_path.exists():
        return False

    try:
        config = _parse_view_config(config_path)
    except Exception:
        return False

    if view_name is None:
        view_name = get_default_endpoint_name(config_path)

    view_data = config.get(view_name) if view_name else None
    if not isinstance(view_data, dict):
        return False

    visualization = view_data.get("visualization")
    if not isinstance(visualization, dict):
        return False

    overlay = visualization.get("overlay")
    if not isinstance(overlay, dict):
        return False

    enabled = overlay.get("enabled")
    return bool(enabled)