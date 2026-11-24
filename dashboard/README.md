# HoloViz Dashboard

Panel + HoloViews dashboard for Zarr Fuse views configured via YAML and environment variables.

## Installation

### From monorepo (development)

```bash
cd zarr_fuse/dashboard
pip install -e .
```

This installs the package in editable mode with the dependencies specified in `pyproject.toml`.

### As a standalone package

```bash
pip install zarr_fuse.dashboard
```

## Configuration

The dashboard uses two configuration sources:

1. **YAML views file** (`zf_view.yaml`) — defines data sources and views.
2. **Environment variables** — configure runtime behavior and S3 credentials.

### Configuration and environment variables

The default view is set using `_dashboard.default_view` in `zf_view.yaml`.

The following variables can be placed in a `.env` file or set in the process environment:

```dotenv
# Optional: explicit path to the views configuration
ZF_VIEW_PATH=/path/to/your/zf_view.yaml

# S3 credentials (when using S3 data sources)
ZF_S3_ACCESS_KEY=your_access_key
ZF_S3_SECRET_KEY=your_secret_key
ZF_S3_ENDPOINT_URL=https://s3.example.com

# Optional: server binding
SERVE_BIND=0.0.0.0
SERVE_PORT=5006

# Optional: tile service configuration
TILE_BUCKET=my-bucket
TILE_PREFIX=my_tiles/
ZF_CACHE_DIR=/tmp/zf_tiles

# Optional: profiling and diagnostics (disabled by default)
DASHBOARD_PROFILE=0
DASHBOARD_DEBUG=0
```

Default server values are `SERVE_BIND=0.0.0.0` and `SERVE_PORT=5006`. The default tile settings are `TILE_BUCKET=app-databuk-test-service`, `TILE_PREFIX=test_tiles/`, and a system temporary directory for `ZF_CACHE_DIR`.

### Performance profiling and debug logging

The dashboard includes optional, lightweight instrumentation to investigate performance without enabling verbose diagnostic output during normal use.

**Performance profiling** is controlled by `DASHBOARD_PROFILE`. Enable it *before starting the dashboard*:

```bash
# Linux / macOS
DASHBOARD_PROFILE=1 zf-dashboard
```

```powershell
# PowerShell (Windows)
$env:DASHBOARD_PROFILE = "1"
zf-dashboard
```

Profiling sends timing and event records to the `dashboard.performance` logger. The profiling modules support:

- Timing configuration and schema parsing and data access stages.
- Counting configuration, store, and data cache hits.
- Measuring map-view construction and plot-selection operations.
- Measuring outgoing Bokeh `PATCH-DOC` WebSocket messages, including serialized message size and server-side send time.
- Aggregating measurements by operation when an operation summary is active.

For example, a logged timing record may look like:

```text
[perf] stage=build_map_view duration_ms=120.450 status=ok
```

The WebSocket probe (`patch_probe.py`) is installed by `serve_dashboard.py` and is inactive unless profiling is enabled. Reported send time covers server-side serialization and writing to the connection; it does not represent complete browser rendering time.

**Debug logging** is controlled independently by `DASHBOARD_DEBUG`:

```bash
# Linux / macOS
DASHBOARD_DEBUG=1 zf-dashboard
```

```powershell
# PowerShell (Windows)
$env:DASHBOARD_DEBUG = "1"
zf-dashboard
```

Debug messages use the `dashboard.debug` logger. For example, configuration diagnostics can report where `zf_view.yaml` was found. Disabled debug calls avoid unnecessary message formatting or expensive diagnostic calculations.

Both flags are disabled by default and are evaluated when the respective Python modules are imported. To change them reliably, restart the dashboard process. They can also be enabled simultaneously.

### Quick start (using default Bukov config)

From the dashboard folder in the monorepo:

```bash
# Create .env with your S3 credentials
cp .env.example .env
# Edit .env and set the ZF_S3_* values

# Start the dashboard
zf-dashboard
```

**On Windows:**

```powershell
# Use the PowerShell helper script
.\scripts\start_dashboard.ps1
```

## Using Custom Data Sources

For a new project with your own data, provide a `zf_view.yaml` file:

```bash
# Point to your configuration
export ZF_VIEW_PATH=/path/to/my_project/config/zf_view.yaml

# Start the dashboard
zf-dashboard
```

Your `zf_view.yaml` should follow this structure:

```yaml
my_view:
  description: "My data source"
  version: "1.0.0"

  source:
    type: "s3"
    store_type: "zarr"
    uri: "s3://my-bucket/my-store.zarr"

  schema:
    file: "schemas/my_schema.yaml"
    fields:
      lat: "latitude"
      lon: "longitude"
      time: "time"
      depth: "depth"
      entity: "station"

  defaults:
    display_variable: "temperature"
    group_path: "/"
```

## Environment Variables (.env)

Instead of exporting environment variables, you can create a `.env` file in your working directory:

```bash
cp .env.example .env
# Edit .env and set your values
```

The dashboard uses [python-dotenv](https://pypi.org/project/python-dotenv/) to load these variables at startup.

## Configuration Files

- `config/zf_view.yaml` — view definitions (packaged default).
- `config.py` — configuration parsing and validation.
- `schemas/` — Zarr schema files referenced from `zf_view.yaml`.
- `.env.example` — environment variable template.

## Further Docs

The detailed guides are in [docs/](docs):

- [New Project Setup (Templates + Workflow)](docs/TEMPLATE.md)
- [Deployment](docs/DEPLOYMENT.md)
- [Config Packaging](docs/CONFIG_PACKAGING.md)
- [Tile Pyramid Guide](docs/tile_pyramid_README.md)
- [Docs Index](docs/DOCS_INDEX.md)

## Building Tiles (Optional)

For map overlays, tiles can be pre-built with `scripts/build_overlay_tiles.py`.
Build parameters (source paths, zoom range, resampling, and S3 target) come from the `tile_build` section of the selected view:

```yaml
tile_build:
  source_image: "my_overlay.png"
  georef_file: "my_georef.json"
  gcp_srs: "EPSG:4326"     # default: CRS of the georeferencing control points
  min_zoom: 0              # default
  max_zoom: 20             # default
  warp_resampling: "near"  # default: reprojection (gdalwarp) resampling
  tile_resampling: "average"
  s3:
    bucket: "my-bucket"
    prefix: "overlays/my-project/"
```

Relative paths resolve against `base_dir`, the directory above the configuration directory (`zf_view.yaml.parent.parent`). The section is validated strictly: unknown or removed keys (for example, `vrt_file`, `warped_tif`, `rgba_vrt`, `tiles_dir`, `target_srs`, `resampling`) raise a `ValueError` naming them. Intermediate rasters and the target CRS (EPSG:3857) are internal to the build script; intermediate files are written under `workdir/tile_build/<view_name>/`.

Include the `tile_build` section to build and upload tiles if the S3 prefix does not yet contain any. Use `--force` to rebuild them. See [Tile Pyramid Guide](docs/tile_pyramid_README.md) for details.

## Scripts

Developer utilities are in [scripts/](scripts). Scripts read `ZF_S3_*` credentials from the environment (including the gitignored `scripts/.env` when present). Their module docstrings contain detailed usage instructions.

- `start_dashboard.ps1` — start the dashboard on Windows from the repository root using `python -m dashboard.serve_dashboard`, as an alternative to `zf-dashboard`.
- `check_view_stores.py` — validate views in `zf_view.yaml` and their data stores: reachability, group paths, variables/coordinates, and finite sample values.
- `check_s3_bucket_access.py` — interactively set S3 credentials, scan buckets, check access, and inspect policies.
- `scan_store_health.py` — produce a `df.info()`-style report for a schema-described store, including dtypes, dimensions, shapes, sizes, and missing values.
- `build_overlay_tiles.py` — build (GDAL VRT/warp/RGBA/gdal2tiles) and upload overlay tiles to S3; supports `--force`, `--dry-run`, and `--delete`.
- `setup_gdal_env.ps1` / `setup_gdal_env.sh` — create a conda-forge `gdal-test` environment with GDAL, needed by `build_overlay_tiles.py` (Windows / Linux / macOS).

## File Organization

- `app.py` — main Panel app (called by `zf-dashboard`).
- `composed.py` — dashboard layout and widget wiring.
- `data.py` — Zarr Fuse data loading helpers and data-access instrumentation.
- `map_views.py` — geographic map visualizations and map timing.
- `multi_time_views.py` — time-series plots.
- `plot_selection.py` — plot selection state and performance counters.
- `profiling.py` — optional structured performance logging (`DASHBOARD_PROFILE`).
- `perf_summary.py` — per-operation aggregation of timing measurements and counters.
- `patch_probe.py` — optional outgoing Bokeh WebSocket patch measurements.
- `debug_log.py` — optional debug diagnostics (`DASHBOARD_DEBUG`).
- `sidebar.py` — sidebar controls and depth selector.
- `tile_service.py` — S3 tile URL presigning (Tornado handler).
- `serve_dashboard.py` — console-script entry point and WebSocket probe setup.
- `config.py` — configuration parsing and caching.
- `api/main.py` — FastAPI application (experimental).

## Requirements

- Python >= 3.11.
- GeoViews uses Cartopy, Proj, and GEOS, which may require system packages on some platforms.
- On Windows, ensure binary wheels are available for your chosen Python version.
- If Cartopy installation fails, verify that your Python distribution and version have compatible wheels.

## Troubleshooting

### "ZF_VIEW_PATH not found"

Set `ZF_VIEW_PATH` to the path of your `zf_view.yaml` file.

### S3 connection fails

Verify that `ZF_S3_ACCESS_KEY`, `ZF_S3_SECRET_KEY`, and `ZF_S3_ENDPOINT_URL` are set correctly.

### Profiling or debug output does not appear

Set `DASHBOARD_PROFILE=1` or `DASHBOARD_DEBUG=1` before starting the dashboard and restart the process. Profiling records appear only for instrumented operations; counters collected outside an active operation summary may not produce output.
