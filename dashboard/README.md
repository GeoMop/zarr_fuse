# HoloViz Dashboard

Panel + HoloViews dashboard for Zarr Fuse views configured via YAML and environment variables.

## Installation

### From monorepo (development)

```bash
cd zarr_fuse/dashboard
pip install -e .
```

This installs the package in editable mode with all dependencies from `pyproject.toml`.

### As standalone package

```bash
pip install zarr_fuse.dashboard
```

## Configuration

The dashboard is configured via:
1. **YAML views file** (`zf_view.yaml`) - defines data sources
2. **Environment variables** - runtime config and S3 credentials

### Configuration and Environment Variables

```bash
# The default view is configured in the _dashboard.default_view key
# of zf_view.yaml.

# Optional: Explicit path to your zf_view.yaml file
ZF_VIEW_PATH=/path/to/your/zf_view.yaml

# S3 credentials (if using S3 data sources)
ZF_S3_ACCESS_KEY=your_access_key
ZF_S3_SECRET_KEY=your_secret_key
ZF_S3_ENDPOINT_URL=https://s3.example.com  # optional

# Optional: Customize server binding
SERVE_BIND=0.0.0.0        # default: 0.0.0.0
SERVE_PORT=5006           # default: 5006

# Optional: Tile service configuration
TILE_BUCKET=my-bucket           # default: app-databuk-test-service
TILE_PREFIX=my_tiles/           # default: test_tiles/
ZF_CACHE_DIR=/tmp/zf_tiles      # default: system temp dir

# Optional: performance profiling
DASHBOARD_PROFILE=1             # enables structured timing output via
                                # logging.getLogger("dashboard.performance")

# Optional: verbose diagnostics (disabled by default)
DASHBOARD_DEBUG=1               # enables [config]/[timeseries]/[ylim] records
                                # via logging.getLogger("dashboard.debug")
```

### Performance profiling

Set `DASHBOARD_PROFILE=1` (before launching the server) to emit records through
`logging.getLogger("dashboard.performance")`. Output has two layers.

Major, once-per-operation phases use `stage=` records (milliseconds and an
outcome `status=ok` / `status=error`):

```
[perf] stage=borehole.select duration_ms=1450.210 site=L5-42L op=42 session=s-9 status=ok
[perf] stage=refresh_views duration_ms=640.200 rebuild_map=True op=42 status=ok
```

Fine-grained probes that run many times per operation (per site, per entity,
per plot redraw, per config/schema read) are aggregated into one summary per
user operation, capped to a handful of records. `op.stage` lines carry
`count=`, `total_ms=` (sum) and `max_ms=`:

```
[perf] event=op.summary op=42 session=s-9 operation=borehole.select sites=3 total_ms=1450.2
[perf] event=op.stage op=42 stage=ts.read count=3 total_ms=912.4 max_ms=501.2
[perf] event=op.counters op=42 plot.redraws=6 store.cache_hit=3 config.cache_hit=5
```

Hot-path probes never log on their own; outside an active operation they record
nothing, and when profiling is disabled they read no clocks. Probes are wired
through `perf_summary.py` (which leaves `profiling.py` untouched).

User-triggered operations group everything under one `op=<n>` id; a session
block (or `op.wrap` restoring deferred callbacks) adds `session=<id>`.
Instrumented user operations: `dashboard.initial_load`, `borehole.select`,
`variable.switch`, `view.switch`, `group.switch`, `data.reload`,
`selection.edit`, `center.update`.

Nested durations are not additive: an inner span is part of the outer span.

### Debug diagnostics

Repetitive diagnostic messages (the `[config]` loader/schema traces and the
`[timeseries]`/`[ylim]` plot traces) are sent through
`logging.getLogger("dashboard.debug")` at `DEBUG` level. They are disabled by
default; set `DASHBOARD_DEBUG=1` (before launching the server) to print them:

```
[config] load_views: config_path=... base_dir=...
[timeseries] Returning overlay with 6 curves
[ylim] view=left xlim=(..., ...) n_visible=42 ylim=(...)
```

Messages use lazy `%`-formatting and any expensive preparation (filesystem
checks, `numpy` reductions) is skipped while debugging is disabled, so the
disabled path stays cheap. This layer is independent of `DASHBOARD_PROFILE`.

Other diagnostic `[map]`/`[fetch_ts]`/`[fill_ts]`/`[variables]`/`[cache]` prints
remain unconditional `print()` output and are not part of either stream.

### Quick Start (Using default Bukov config)

From dashboard folder (monorepo):
```bash
# Create .env with your S3 credentials
cp .env.example .env
# Edit .env and set ZF_S3_* values

# Start dashboard
zf-dashboard
```

**On Windows:**
```powershell
# Use the PowerShell helper script
.\scripts\start_dashboard.ps1
```

## Using Custom Data Sources

For a new project with your own data, provide your own `zf_view.yaml`:

```bash
# Point to your config
export ZF_VIEW_PATH=/path/to/my_project/config/zf_view.yaml

# Start dashboard
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

You can also use a `.env` file in your working directory instead of exporting env vars:

```bash
cp .env.example .env
# Edit .env and set your values
```

The dashboard uses [python-dotenv](https://pypi.org/project/python-dotenv/) to auto-load these at startup.

## Configuration Files

- `config/zf_view.yaml` - View definitions (packaged default)
- `config.py` - Config parsing and validation logic
- `schemas/` - Zarr schema files referenced in zf_view.yaml
- `.env.example` - Template for environment variables

## Further Docs

The detailed guides live in [docs/](docs):

- [New Project Setup (Templates + Workflow)](docs/TEMPLATE.md)
- [Deployment](docs/DEPLOYMENT.md)
- [Config Packaging](docs/CONFIG_PACKAGING.md)
- [Tile Pyramid Guide](docs/tile_pyramid_README.md)
- [Docs Index](docs/DOCS_INDEX.md)

## Building Tiles (Optional)

For map overlay support, tiles can be pre-built with `scripts/build_overlay_tiles.py`.
Build parameters (source paths, zoom range, resampling, S3 target) come from the
`tile_build` section of the selected view:

```yaml
tile_build:
  source_image: "my_overlay.png"
  georef_file: "my_georef.json"
  gcp_srs: "EPSG:4326"     # default: CRS of the georef control points
  min_zoom: 0              # default
  max_zoom: 20             # default
  warp_resampling: "near"  # default: reprojection (gdalwarp) resampling
  tile_resampling: "average"
  s3:
    bucket: "my-bucket"
    prefix: "overlays/my-project/"
```

Relative paths resolve against `base_dir`, the directory above the config's directory
(`zf_view.yaml.parent.parent`). The section is validated strictly: unknown or removed keys
(for example `vrt_file`, `warped_tif`, `rgba_vrt`, `tiles_dir`, `target_srs`, `resampling`)
raise a `ValueError` naming them. Intermediate rasters and the target CRS (EPSG:3857) are
internal to the build script; it writes them under `workdir/tile_build/<view_name>/`.

Insert the `tile_build` block above to build and upload tiles whenever the S3
prefix does not yet contain any (ensure-semantics; `--force` rebuilds).
See [docs/tile_pyramid_README.md](docs/tile_pyramid_README.md) for details.

## Scripts

Developer utilities in [scripts/](scripts). All scripts read `ZF_S3_*`
credentials from the general environment (filled from the gitignored
`scripts/.env` when present); their module docstrings carry full usage:

- `start_dashboard.ps1` - Windows: start the dashboard from the repository root
  via `python -m dashboard.serve_dashboard`; alternative to `zf-dashboard`.
- `check_view_stores.py` - Validate the views in `zf_view.yaml` and their data
  stores: reachability, group paths, expected variables/coordinates, and finite
  sample values.
- `check_s3_bucket_access.py` - Interactive S3 access tool: set credentials,
  full bucket scan, access check, inspect access policy.
- `scan_store_health.py` - `df.info()`-style health report for a
  schema-described store: dtype, dims, shape, size, missing counts per
  coordinate and data variable.
- `build_overlay_tiles.py` - Ensure overlay tiles exist on S3: build (GDAL
  VRT/warp/RGBA/gdal2tiles) and upload as one operation; parameters come from
  the `tile_build` section of the selected view; supports `--force`,
  `--dry-run`, `--delete`.
- `setup_gdal_env.ps1` / `setup_gdal_env.sh` - Create a conda `gdal-test`
  environment (conda-forge only) with GDAL, required by
  `build_overlay_tiles.py` (Windows / Linux-macOS).

## File Organization

- `app.py` - Main Panel app (called by `zf-dashboard`)
- `composed.py` - Dashboard layout and widget wiring
- `data.py` - Zarr Fuse data loading helpers
- `profiling.py` - Configurable performance instrumentation (`DASHBOARD_PROFILE`)
- `perf_summary.py` - Per-operation aggregation of fine-grained perf probes
- `debug_log.py` - Opt-in `DEBUG` diagnostics (`DASHBOARD_DEBUG`)
- `map_views.py` - Geographic map visualizations
- `multi_time_views.py` - Time-series plots
- `sidebar.py` - Sidebar controls and depth selector
- `tile_service.py` - S3 tile URL presigning (Tornado handler)
- `serve_dashboard.py` - Entrypoint for console script
- `config.py` - Configuration parsing
- `api/main.py` - FastAPI application (experimental)

## Requirements

- Python >= 3.11
- GeoViews uses Cartopy/Proj/GEOS which may require system packages on some systems
- On Windows, ensure you have binary wheel support for geospatial packages
- If Cartopy install fails, verify you're using a Python distribution that supports wheels

## Troubleshooting

### "ZF_VIEW_PATH not found"
Set `ZF_VIEW_PATH` env var pointing to your `zf_view.yaml` file.

### S3 connection fails
Verify `ZF_S3_ACCESS_KEY`, `ZF_S3_SECRET_KEY`, and `ZF_S3_ENDPOINT_URL` are set correctly.
