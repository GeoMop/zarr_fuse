# HoloViz Dashboard

Panel + HoloViews dashboard for Bukov Zarr data over S3.

## Quick start

From this folder:

```
./setup_env
panel serve app.py --show
```

## PowerShell helper

Use scripts/start_dashboard.ps1 to start the dashboard on Windows.

## S3 configuration

The dashboard reads S3 settings from the endpoint config and zarr_fuse environment variables.

- Endpoint config: app/databuk/dashboard/holoviz_dashboard/config/endpoints.yaml
- Schema files: app/databuk/dashboard/holoviz_dashboard/schemas/*.yaml
- Environment variables used by zarr_fuse:
  - ZF_S3_ACCESS_KEY
  - ZF_S3_SECRET_KEY
  - ZF_S3_ENDPOINT_URL (optional if schema already includes it)

Set the endpoint:

```
set HV_DASHBOARD_ENDPOINT=bukov_endpoint
```

## Bukov overlay

The Bukov map overlay is loaded from dashboard/config/bukov_endpoint.
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
