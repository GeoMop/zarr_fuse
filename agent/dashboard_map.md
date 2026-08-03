<!-- Meta
scope: dashboard/ — HoloViz Panel dashboard for Zarr Fuse endpoints
readers: developers, maintainers, AI coding agents
branch: feature/dashboard-multiple-selection
commit_sha: 2e7b2d92dd7f5b031ee66d44239d34910a81447b
last_reviewed: 03 08 2026
verification:
  - static code inspection
  - tests: pytest dashboard/test/ -v --ignore=dashboard/test/s3_tile_resolver_test.py --ignore=dashboard/test/s3_tile_server-out.py --ignore=dashboard/test/store_structure.py --ignore=dashboard/test/upload_s3.py → 115 passed, 1 failed (test_dataset_variable_report needs S3_ACCESS_KEY env)
  - runtime verification: not performed
-->

# dashboard/ Map

Component of the [zarr_fuse monorepo](repo_map.md).

## Purpose

Navigation and change-impact map for the `dashboard/` module. Use this page to
find entry points, state owners, I/O boundaries, rendering modules, configuration,
tests, and the files most likely to be affected by a change.

For runtime behavior and state transitions, see
[dashboard architecture](dashboard_architecture.md).

## Quick Orientation

- **Server entry:** `serve_dashboard.py` → `main()` starts Panel and registers tile routes.
- **Composition root:** `composed.py` → `build_dashboard()` creates and wires one dashboard session.
- **Configuration:** `config.py` resolves `endpoints.yaml`, schemas, environment substitution, and typed endpoint config.
- **State and I/O:** `data.py` owns `DashboardData` / `LocalClient`; `plot_selection.py` owns canonical site/depth selection state.
- **Rendering:** `map_views.py`, `multi_time_views.py`, `sidebar.py`, and `plot_styles.py`.
- **Tests:** `dashboard/test/`; the documented test command is listed below.

## Directory Overview

| Path | Role |
|---|---|
| `dashboard/*.py` | Runtime dashboard package: server, composition, config, data access, state, and rendering |
| `dashboard/charts/holoviz/` | Helm chart used to deploy the dashboard |
| `dashboard/docs/` | Dashboard-specific developer and deployment documentation |
| `dashboard/scripts/` | Helper/validation scripts |
| `dashboard/test/` | Unit tests plus several developer utilities that are excluded from the main pytest command |

## Entry Points

| File | Entry | Role |
|---|---|---|
| `serve_dashboard.py` | `main()` | Loads environment settings, starts `pn.serve`, exposes `/` and `/app`, and registers the tile route |
| `composed.py` | `build_dashboard()` | Builds one browser session: sidebar, map, selection table, linked timeseries views, callbacks, and GoldenLayout template |
| `data.py` | `load_data()` | Creates `DashboardData` and its `LocalClient` for a selected endpoint |
| `config.py` | `load_endpoints()` | Parses `endpoints.yaml` into typed endpoint configuration objects |

## Component Map

| File | Responsibility | Key symbols | Collaborators | Notes |
|---|---|---|---|---|
| `serve_dashboard.py` | Server/bootstrap entry | `main()`, `ROUTES` | `config`, `composed`, `tile_service`, Panel | Maps legacy `ZF_*` S3 variables where needed and starts `pn.serve` |
| `composed.py` | Composition root and callback wiring | `build_dashboard()` | `config`, `data`, `sidebar`, `map_views`, `multi_time_views`, `plot_selection` | Main coupling point between dashboard modules; owns refresh/change callbacks and template assembly |
| `config.py` | YAML parsing, path resolution, env substitution, schema/field resolution | `EndpointConfig`, `SourceConfig`, `SchemaConfig`, `MapConfig`, `VisualizationConfig`, `resolve_endpoints_path()`, `load_endpoints()`, `get_endpoint_config()`, `resolve_schema_fields()` | `yaml`, `dotenv`, `os` | Reads `ENDPOINTS_PATH` and `SCHEMAS_PATH`; `_resolve_fields_for_group_raw()` is also imported by `multi_time_views.py` |
| `data.py` | Zarr data access and per-session caches | `DashboardData`, `LocalClient`, `EndpointHandle`, `load_data()` | `config`, `zarr_fuse`, `numpy`, `pandas`, `xarray` | Synchronous store reads; map/timeseries failures are returned as status dictionaries |
| `sidebar.py` | Controller/sidebar widgets | `build_sidebar()` | Panel, pandas | Leaf dashboard module; returns the widgets later wired by `composed.py` |
| `map_views.py` | Geographic map, clustering, overlays, marker metadata, tap source | `build_map_view()` | GeoViews, HoloViews, Cartopy; receives dashboard data object | Returns `(map_view, map_state)`; no dashboard-module import |
| `multi_time_views.py` | Linked year/month/day timeseries views and map-tap selection | `build_timeseries_views()`, `create_timeseries_view()` | `config`, `plot_selection`, dashboard data/state objects | Imports private `config._resolve_fields_for_group_raw`; changing that symbol has cross-module impact |
| `plot_selection.py` | Canonical site/depth selection state and Tabulator matrix | `SelectionState`, `build_plot_selection_panel()`, `build_assignment_matrix()`, `resolve_available_dimensions()` | `plot_styles`, `config`, Panel/Param | Canonical checked key is `(site_id, depth)`; table orientation must not change that identity |
| `plot_styles.py` | Stable visual mappings | `COLORS`, `MARKER_SHAPES`, `SHAPE_TO_SVG` | none | Leaf module; no cross-module imports |
| `tile_service.py` | HTTP tile redirect/proxy and presigned URL cache | `S3TileHandler`, `get_tile_url()`, `tile_id()`, `tile_key()` | boto3, Tornado, YAML | Tile cache is process-global and persisted to `tile_url_cache.json` |

## Data Flow (high level)

```text
serve_dashboard.main()
  → build_dashboard()
  → load_endpoints()
  → load_data() / LocalClient
  → build sidebar + selection table + map + timeseries
  → return Panel template
```

```text
map tap
  → timeseries fetch through LocalClient
  → SelectionState.add_site()
  → selection version changes
  → table / linked timeseries redraw
```

```text
endpoint / group / variable selector
  → callback in composed.py
  → update DashboardData / SelectionState
  → refresh map and timeseries views
```

## Configuration

| Setting | Location | Type | Effect |
|---|---|---|---|
| `ENDPOINTS_PATH` | `config.py` `resolve_endpoints_path()` | env var | Explicit path to `endpoints.yaml`; used before repository/ancestor fallback search |
| `SCHEMAS_PATH` | `config.py` schema resolution | env var | Directory containing schema files mounted/provided to the dashboard |
| `env_file` / `_dashboard.env_file` | `config.py` `load_environment_from_config()` | YAML key | Loads a `.env` file with `load_dotenv(override=False)` |
| `_dashboard.default_endpoint` | `config.py` endpoint loading | YAML key | Chooses the default endpoint when multiple endpoints are configured |
| `SERVE_BIND` | `serve_dashboard.py` | env var | Panel bind address; default `0.0.0.0` |
| `SERVE_PORT` | `serve_dashboard.py` | env var | Panel port; default `5006` |
| `BOKEH_ALLOW_WS_ORIGIN` | `serve_dashboard.py` | env var | Comma-separated websocket origins |
| `ZF_S3_ACCESS_KEY` / `S3_ACCESS_KEY` | `serve_dashboard.py` | env var | S3 credential input / backward-compatible mapping |
| `TILE_BUCKET`, `TILE_PREFIX` | `tile_service.py` | env var | S3 tile bucket and prefix |
| `ZF_CACHE_DIR` | `tile_service.py` | env var | Tile URL cache directory |

Deployment note: the reusable dashboard workflow mounts the endpoint config and
schemas into the container and sets `ENDPOINTS_PATH` / `SCHEMAS_PATH`. Changes to
those paths must stay consistent with `config.py`.

## Testing

| Area | Command or file | Notes |
|---|---|---|
| Main test command | `pytest dashboard/test/ -v --ignore=dashboard/test/s3_tile_resolver_test.py --ignore=dashboard/test/s3_tile_server-out.py --ignore=dashboard/test/store_structure.py --ignore=dashboard/test/upload_s3.py` | Observed during review: 115 passed, 1 failed because `S3_ACCESS_KEY` was not set |
| Unit | `dashboard/test/test_dataset_variable_report.py` | Config/schema display parsing; environment-dependent in the reviewed run |
| Unit | `dashboard/test/test_map_marker_payload.py` | `get_map_data()` payload shape |
| Unit | `dashboard/test/test_plot_selection_state.py` | `SelectionState` invariants and checked-state behavior |
| Unit | `dashboard/test/test_plot_style_mapping.py` | Stable plot style mappings |
| Unit | `dashboard/test/test_tabulator_matrix.py` | Selection matrix construction |
| Unit | `dashboard/test/test_validation_phase2.py` | Configuration validation |
| Dev utilities (not main pytest tests) | `s3_tile_resolver_test.py`, `s3_tile_server-out.py`, `store_structure.py`, `upload_s3.py` | Explicitly excluded by the documented test command |

## Change Guide

| Change | Files to touch | Tests/checks to run | Watch out for |
|---|---|---|---|
| Add or change endpoint configuration | `config.py` (dataclasses, parser, path/field resolution) | `test_dataset_variable_report.py`, `test_validation_phase2.py` | Verify YAML parsing, env substitution, defaults, schema paths, and backward compatibility |
| Change selection semantics | `plot_selection.py`; possibly `multi_time_views.py` | `test_plot_selection_state.py`, `test_tabulator_matrix.py` | Keep canonical identity `(site_id, depth)` independent of table row/column orientation |
| Change map data or clustering | `map_views.py`, `data.py` | `test_map_marker_payload.py` | Marker metadata and entity index are consumed by timeseries selection |
| Change timeseries rendering or interaction | `multi_time_views.py`; possibly `plot_selection.py` | main dashboard test command | It depends on selection-state versioning and the private config field-resolution helper |
| Change endpoint/group/variable refresh behavior | `composed.py`, then affected state/data/view modules | main dashboard test command | `composed.py` coordinates state replacement, cache clearing, and view refreshes |
| Change layout or sidebar wiring | `composed.py`, `sidebar.py` | main dashboard test command | `build_sidebar()` return order/names are consumed directly by `composed.py`; template pane names must remain consistent |
| Change tile routing/cache | `tile_service.py`, `serve_dashboard.py` | main dashboard test command plus tile-specific manual/dev checks | Tile cache is process-global and persisted; route errors become HTTP errors |
| Change deployment config mounts | `.github/workflows/dashboard-reusable-workflow.yaml`, `dashboard/charts/holoviz/`, `config.py` | workflow CI / deployment check | Keep mounted paths and `ENDPOINTS_PATH` / `SCHEMAS_PATH` consistent; avoid duplicating pod paths in Python |

## Related Documentation

- [dashboard architecture](dashboard_architecture.md) — runtime behavior, state ownership, interaction workflows, and invariants
- [map template](map_template.md) — schema and maintenance rules for module maps
- [repo map](repo_map.md) — monorepo-level navigation
- [zarr_fuse map](zarr_fuse_map.md) — core storage/schema library map
