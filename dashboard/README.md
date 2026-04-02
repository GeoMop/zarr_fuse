# HoloViz Dashboard


Panel + HoloViews dashboard for Bukov Zarr data over S3.

## Environment Variables (.env)

The dashboard supports configuration via a `.env` file in this directory. Copy `.env.example` to `.env` and fill in your S3 credentials and endpoint:

```
cp .env.example .env
# Edit .env and set your values
```

The dashboard uses [python-dotenv](https://pypi.org/project/python-dotenv/) to load these variables automatically.

## Quick start

From this folder:

```
./setup_env
panel serve app.py --show
```

## PowerShell helper

Use scripts/start_dashboard.ps1 to start the dashboard on Windows.


## S3 configuration

The dashboard reads S3 settings from the endpoint config and the following environment variables (set in your `.env` file):

- `ZF_S3_ACCESS_KEY`
- `ZF_S3_SECRET_KEY`
- `ZF_S3_ENDPOINT_URL` (optional if schema already includes it)

Endpoint config: `dashboard/config/endpoints.yaml`
Schema files: `dashboard/schemas/*.yaml`

Set the endpoint (Windows):
```
set HV_DASHBOARD_ENDPOINT=bukov_endpoint
```

## Bukov overlay

The Bukov map overlay is loaded from dashboard/config/bukov_endpoint.
Detailed information can be found here: 
- dashboard\tile_pyramid_README.md

You can override or disable it with:
- HV_OVERLAY_GEOREF: path to a georef JSON file
- HV_OVERLAY_IMAGE: path to a PNG image
- HV_OVERLAY_MAX_PIXELS: optional max pixel count before downscaling (default 25,000,000)
- HV_OVERLAY_ENABLED: set to 0/false/no to disable the overlay

## What setup_env does

- Creates a local venv in ./venv
- Installs requirements from requirements.txt
- Registers a Jupyter kernel named "HoloViz Dashboard"


## Requirements notes

- The dashboard requires `python-dotenv` (now included in requirements.txt) for .env support.
- GeoViews uses Cartopy/Proj/GEOS. On Windows, these may require additional system packages.
- If Cartopy install fails, verify that your Python distribution can install binary wheels.
- The requirements install the local zarr_fuse package from the repo root.

## Files

- app.py: Panel app entrypoint
- data.py: Zarr Fuse data loading helpers
- map_views.py: Map view builder
- multi_time_views.py: Multi-scale time-series views
- composed.py: Dashboard composition and wiring
- ui.py: Sidebar and depth controls
- requirements.txt: Python dependencies
- setup_env: Bash setup helper
