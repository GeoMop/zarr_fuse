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

### Required Environment Variables

```bash
# Required: Which view to load from zf_view.yaml
HV_DASHBOARD_VIEW=bukov_endpoint

# Optional: Path to your zf_view.yaml file
# Default: packaged config/zf_view.yaml
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
```

### Quick Start (Using default Bukov config)

From dashboard folder (monorepo):
```bash
# Create .env with your S3 credentials
cp .env.example .env
# Edit .env and set ZF_S3_* values

# Set which view to use
export HV_DASHBOARD_VIEW=bukov_endpoint

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
export HV_DASHBOARD_VIEW=my_view

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
Build parameters (paths, zoom range, CRS, resampling, S3 target) come from the
`tile_build` section of the selected view:

```yaml
tile_build:
  source_image: "my_overlay.png"
  georef_file: "my_georef.json"
  vrt_file: "tiles/source_gcps.vrt"
  warped_tif: "tiles/source_3857.tif"
  rgba_vrt: "tiles/source_3857_rgba.vrt"
  tiles_dir: "tiles"
  min_zoom: 0              # default
  max_zoom: 20             # default
  target_srs: "EPSG:3857"  # default
  gcp_srs: "EPSG:4326"     # default
  resampling: "near"       # default
  tile_resampling: "average"
  s3:
    bucket: "my-bucket"
    prefix: "overlays/my-project/"
```

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

### "HV_DASHBOARD_VIEW is required"
Set `HV_DASHBOARD_VIEW` env var to match a view name in your `zf_view.yaml`.

### S3 connection fails
Verify `ZF_S3_ACCESS_KEY`, `ZF_S3_SECRET_KEY`, and `ZF_S3_ENDPOINT_URL` are set correctly.
