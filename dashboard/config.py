import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, NewType, Optional
from urllib.parse import urlparse

import yaml
from dotenv import load_dotenv
from zarr_fuse import schema as zf_schema

VIEWS_ENV_VAR = "ZF_VIEW_PATH"
LEGACY_ENDPOINTS_ENV_VAR = "ENDPOINTS_PATH"
SCHEMAS_ENV_VAR = "SCHEMAS_PATH"

# Storage URI such as s3://bucket/prefix; distinct from plain textual values (gcp_srs, resampling, ...).
StoreURI = NewType("StoreURI", str)


def parse_s3_uri(uri: str) -> tuple[str, str] | None:
    """Split an ``s3://bucket/prefix`` URI into ``(bucket, prefix)``.

    Returns ``None`` when the URI is not a valid ``s3://`` URI (missing scheme
    or missing bucket). ``prefix`` is the URI path without leading/trailing
    slashes and is empty when the URI targets the bucket root. Shared by config
    validation, the tile build script and the tile service so the urlparse
    logic exists only once.
    """
    parsed = urlparse(uri.strip())
    if parsed.scheme != "s3" or not parsed.netloc:
        return None
    return parsed.netloc, parsed.path.strip("/")

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
    """``source`` section of zf_view.yaml: where the view's dataset store lives.

    ``uri`` is the only required key and the only one opened (via zarr_fuse).
    ``schema_path`` is required and used to locate the schema file; the
    resolved absolute path is stored in ``SchemaConfig.file``, not here.
    """
    uri: StoreURI  # store location opened by zarr_fuse, e.g. s3://bucket/store.zarr


@dataclass
class SchemaFieldsConfig:
    """One field-mapping entry of ``variable_map``: standard field -> dataset name.

    Values are variable/column names as stored in the dataset; ``None`` means
    the dataset does not carry that field (e.g. no ``vertical`` coordinate).
    """
    lat: Optional[str] = None  # dataset name of the latitude field
    lon: Optional[str] = None  # dataset name of the longitude field
    time: Optional[str] = None  # dataset name of the time coordinate
    vertical: Optional[str] = None  # dataset name of the depth/level coordinate (optional)
    entity: Optional[str] = None  # dataset name of the site/borehole identity field


@dataclass
class SchemaConfig:
    """Resolved schema file and field mapping of one view (from ``variable_map``)."""
    file: str  # absolute path of the schema YAML (kept as str, not Path)
    fields: SchemaFieldsConfig = field(default_factory=SchemaFieldsConfig)  # used when no group path matches
    group_fields: Dict[str, SchemaFieldsConfig] = field(default_factory=dict)  # group path -> field mapping


@dataclass
class SchemaDisplayConfig:
    """UI labels derived from the schema for the configured display variable.

    Built by ``_read_schema_display`` from the schema's VARS/COORDS metadata;
    ``display_variable`` is passed through from ``defaults``.
    """
    display_variable: Optional[str] = None  # selected display variable (defaults.display_variable)
    entity_name: Optional[str] = None  # dataset column shown as the site/entity label
    vertical_name: Optional[str] = None  # dataset column shown as the depth/level label


@dataclass
class DefaultsConfig:
    """``defaults`` section: selections applied when the view first loads."""
    display_variable: Optional[str] = None  # variable selected in the dropdown at startup
    group_path: Optional[str] = None  # variable_map group selected at startup ("/" = root)
    default_site: Optional[str] = None  # site preselected at startup (matched against site_id)


@dataclass
class MapConfig:
    """``visualization.map`` section: marker look and clustering.

    ``title`` and ``point_size`` are required keys; all fields are read by
    ``map_views.build_map_view``.
    """
    title: Optional[str] = None  # map title (required); also shown on the points layer
    point_size: Optional[int] = None  # marker size of a single point / base size of a cluster (required)
    cluster_enabled: bool = True  # group nearby markers into grid cells
    cluster_eps_factor: float = 0.05  # cluster grid size as a fraction of the current view width
    cluster_buffer_factor: float = 0.1  # margin around the view (fraction of width) still clustered
    cluster_size_scale: float = 3.0  # marker size added per member of a cluster


@dataclass
class TimeSeriesConfig:
    """``visualization.timeseries`` section: x-axis windows of the time panes.

    Both keys are required in zf_view.yaml (the dataclass defaults are for
    standalone construction only).
    """
    middle_window_days: Optional[int] = None  # time span of the middle (month-scale) pane, in days
    right_window_hours: Optional[int] = None  # time span of the right (day-scale) pane, in hours


@dataclass
class OverlayConfig:
    """``visualization.overlay`` section: XYZ raster tile overlay on the base map.

    Read at runtime from the raw view dict (``map_views`` / ``tile_service``);
    the typed object is what ``load_views`` returns.
    """
    enabled: bool = False  # master switch (HV_OVERLAY_ENABLED=0 forces it off)
    tile_url: Optional[str] = None  # XYZ template, e.g. /tiles/{Z}/{X}/{Y}.png (HV_OVERLAY_TILE_URL fallback)
    source_uri: Optional[StoreURI] = None  # s3:// prefix holding the tiles; tile service signs URLs from it


@dataclass
class TileBuildConfig:
    """Public ``tile_build`` section of zf_view.yaml (overlay tile pipeline).

    ``base_dir`` is the directory above the config's directory
    (``zf_view.yaml.parent.parent``); relative paths in this section
    resolve against it. Intermediate raster paths and the target CRS are
    internal to dashboard/scripts/build_overlay_tiles.py and are not
    configurable.
    """

    source_image: Optional[Path] = None  # source raster; path relative to base_dir
    georef_file: Optional[Path] = None  # QGIS-style GCP JSON (sourceX/sourceY pixels, mapX/mapY ground)
    gcp_srs: str = "EPSG:4326"  # CRS of mapX/mapY in georef_file (gdal_translate -a_srs)
    min_zoom: int = 0  # lowest XYZ zoom level built; overlay is missing below it
    max_zoom: int = 20  # highest XYZ zoom level built; overlay is missing above it
    warp_resampling: str = "near"  # gdalwarp -r during reprojection to EPSG:3857
    tile_resampling: Optional[str] = None  # gdal2tiles -r; None -> use warp_resampling
    target_url: Optional[StoreURI] = None  # publish target, e.g. s3://bucket/prefix/


@dataclass
class VisualizationConfig:
    """``visualization`` section: grouping of the three UI subsections."""
    map: MapConfig = field(default_factory=MapConfig)  # base map and markers
    timeseries: TimeSeriesConfig = field(default_factory=TimeSeriesConfig)  # time axis windows
    overlay: OverlayConfig = field(default_factory=OverlayConfig)  # raster tile overlay


@dataclass
class ViewConfig:
    """One named view of zf_view.yaml: the full dashboard configuration for it.

    The YAML key of the view is stored as ``name``. Required sections:
    ``source``, ``variable_map``, ``defaults`` and ``visualization``; every
    other section defaults to an empty config.
    """
    name: str  # view key in zf_view.yaml; also the ?view= URL parameter
    source: SourceConfig  # dataset store location
    schema: SchemaConfig  # resolved schema file + field mapping
    schema_display: SchemaDisplayConfig = field(default_factory=SchemaDisplayConfig)  # labels/units from schema
    defaults: DefaultsConfig = field(default_factory=DefaultsConfig)  # startup selections
    visualization: VisualizationConfig = field(default_factory=VisualizationConfig)  # map/timeseries/overlay
    tile_build: TileBuildConfig = field(default_factory=TileBuildConfig)  # overlay tile build pipeline


FIELD_NAMES = {"lat", "lon", "time", "vertical", "entity"}
REQUIRED_FIELD_NAMES = {"lat", "lon", "time", "entity"}

TILE_BUILD_ALLOWED_KEYS = frozenset({
    "source_image",
    "georef_file",
    "gcp_srs",
    "min_zoom",
    "max_zoom",
    "warp_resampling",
    "tile_resampling",
    "target_url",
})


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
            entity_name=entity_field,
            vertical_name=vertical_field,
        )

    coords_data = group_data.get("COORDS", {})

    entity_name = entity_field
    if entity_field and entity_field in coords_data and isinstance(coords_data[entity_field], dict):
        entity_name = coords_data[entity_field].get("df_col", entity_field)

    vertical_name = vertical_field
    if vertical_field and vertical_field in coords_data and isinstance(coords_data[vertical_field], dict):
        vertical_name = coords_data[vertical_field].get("df_col", vertical_field)

    return SchemaDisplayConfig(
        display_variable=display_variable,
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
    tile_build_raw = view_data.get("tile_build")
    tile_build_data = tile_build_raw if tile_build_raw is not None else {}
    if not isinstance(tile_build_data, dict):
        raise ValueError(f"View '{view_name}' tile_build must be a mapping/object")

    unknown_tile_build_keys = sorted(set(tile_build_data) - TILE_BUILD_ALLOWED_KEYS)
    if unknown_tile_build_keys:
        raise ValueError(
            f"View '{view_name}' tile_build contains unsupported key(s): "
            f"{', '.join(unknown_tile_build_keys)}. "
            f"Allowed keys: {', '.join(sorted(TILE_BUILD_ALLOWED_KEYS))}."
        )

    target_url_raw = tile_build_data.get("target_url") or None
    if target_url_raw is not None and parse_s3_uri(str(target_url_raw)) is None:
        raise ValueError(
            f"View '{view_name}' tile_build.target_url must be an s3:// URI with a bucket, "
            "e.g. s3://bucket/prefix/."
        )

    source_image_raw = tile_build_data.get("source_image")
    georef_file_raw = tile_build_data.get("georef_file")

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

    if not source_data.get("uri"):
        raise ValueError(f"View '{view_name}' is missing source.uri")

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
        source=SourceConfig(
            uri=StoreURI(source_data["uri"]),
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
            default_site=defaults_data.get("default_site"),
        ),
            visualization=VisualizationConfig(
            map=MapConfig(
                title=map_data["title"],
                point_size=map_data["point_size"],
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
                source_uri=(
                    StoreURI(overlay_data["source_uri"])
                    if overlay_data.get("source_uri")
                    else None
                ),
            ),
        ),
        tile_build=TileBuildConfig(
            source_image=Path(str(source_image_raw)) if source_image_raw else None,
            georef_file=Path(str(georef_file_raw)) if georef_file_raw else None,
            gcp_srs=tile_build_data.get("gcp_srs", "EPSG:4326"),
            min_zoom=tile_build_data.get("min_zoom", 0),
            max_zoom=tile_build_data.get("max_zoom", 20),
            warp_resampling=tile_build_data.get("warp_resampling", "near"),
            tile_resampling=tile_build_data.get("tile_resampling"),
            target_url=StoreURI(str(target_url_raw)) if target_url_raw is not None else None,
        ),
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
    """Return the default view name from the _dashboard section, if configured."""
    if not config_path.exists():
        return None

    config = _parse_view_config(config_path)

    meta = config.get("_dashboard")
    if not isinstance(meta, dict):
        return None

    default_view = meta.get("default_view")
    if not isinstance(default_view, str):
        return None

    view_config = config.get(default_view)
    if isinstance(view_config, dict):
        return default_view

    return None


def load_view_config(config_path: Path, view_name: str) -> ViewConfig:
    """Load one explicitly named view configuration."""
    views = load_views(config_path)

    if not views:
        raise ValueError(f"No views configured in {config_path}")

    if not view_name:
        raise ValueError("view_name is required")

    if view_name not in views:
        raise KeyError(f"View '{view_name}' not found in {config_path}")

    return views[view_name]


def schema_endpoint_url(config_path: Path, view_name: str) -> Optional[str]:
    """Resolve the S3 endpoint URL strictly from the view schema file.

    The S3 endpoint URL is owned by the schema (ATTRS.S3_ENDPOINT_URL), and
    the view name must be provided explicitly.
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
        schema = zf_schema.deserialize(schema_path)
    except Exception:
        return None

    attrs = schema.ds.ATTRS
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