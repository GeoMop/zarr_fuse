# HoloViz Dashboard

Panel + HoloViews dashboard for Zarr Fuse endpoints configured via YAML and environment variables.

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
1. **YAML endpoints file** (`endpoints.yaml`) - defines data sources
2. **Environment variables** - runtime config and S3 credentials

### Required Environment Variables

```bash
# Required: Which endpoint to load from endpoints.yaml
HV_DASHBOARD_ENDPOINT=bukov_endpoint

# Optional: Path to your endpoints.yaml file
# Default: packaged config/endpoints.yaml
ENDPOINTS_PATH=/path/to/your/endpoints.yaml

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

# Set which endpoint to use
export HV_DASHBOARD_ENDPOINT=bukov_endpoint

# Start dashboard
zf-dashboard
```

**On Windows:**
```powershell
# Use the PowerShell helper script
.\scripts\start_dashboard.ps1
```

## Using Custom Data Sources

For a new project with your own data, provide your own `endpoints.yaml`:

```bash
# Point to your config
export ENDPOINTS_PATH=/path/to/my_project/config/endpoints.yaml
export HV_DASHBOARD_ENDPOINT=my_endpoint

# Start dashboard
zf-dashboard
```

Your `endpoints.yaml` should follow this structure:

```yaml
my_endpoint:
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
    metric: "temperature"
    group_path: "/"
  
  labels:
    metric: "Temperature (°C)"
    depth_unit: "meters"
```

## Environment Variables (.env)

You can also use a `.env` file in your working directory instead of exporting env vars:

```bash
cp .env.example .env
# Edit .env and set your values
```

The dashboard uses [python-dotenv](https://pypi.org/project/python-dotenv/) to auto-load these at startup.

## Configuration Files

- `config/endpoints.yaml` - Endpoint definitions (packaged default)
- `config/dashboard_config.py` - Config parsing and validation logic
- `schemas/` - Zarr schema files referenced in endpoints.yaml
- `.env.example` - Template for environment variables

## Building Tiles (Optional)

For map overlay support, tiles can be pre-built:

```yaml
tile_build:
  enabled: true
  source_image: "my_overlay.png"
  georef_file: "my_georef.json"
  tiles_dir: "config/tiles"
```

See `tile_pyramid_README.md` for details.

## File Organization

- `app.py` - Main Panel app (called by `zf-dashboard`)
- `composed.py` - Dashboard layout and widget wiring
- `data.py` - Zarr Fuse data loading helpers
- `map_views.py` - Geographic map visualizations
- `multi_time_views.py` - Time-series plots
- `ui.py` - Sidebar controls and depth selector
- `tile_service.py` - S3 tile URL presigning (Tornado handler)
- `serve_dashboard.py` - Entrypoint for console script
- `config/dashboard_config.py` - Configuration parsing
- `api/main.py` - FastAPI application (experimental)

## Requirements

- Python >= 3.11
- GeoViews uses Cartopy/Proj/GEOS which may require system packages on some systems
- On Windows, ensure you have binary wheel support for geospatial packages
- If Cartopy install fails, verify you're using a Python distribution that supports wheels

## Troubleshooting

### "ENDPOINTS_PATH not found"
Set `ENDPOINTS_PATH` env var pointing to your `endpoints.yaml` file.

### "HV_DASHBOARD_ENDPOINT is required"
Set `HV_DASHBOARD_ENDPOINT` env var to match an endpoint name in your `endpoints.yaml`.

### S3 connection fails
Verify `ZF_S3_ACCESS_KEY`, `ZF_S3_SECRET_KEY`, and `ZF_S3_ENDPOINT_URL` are set correctly.
