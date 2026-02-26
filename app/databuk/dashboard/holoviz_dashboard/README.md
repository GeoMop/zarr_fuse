# HoloViz Dashboard

Local Panel + HoloViews dashboard for Bukov Zarr data.

## Quick start

From this folder:

```
./setup_env
panel serve app.py --show
```

## S3 configuration

The dashboard reads S3 settings from the endpoint config and zarr_fuse environment variables.

- Endpoint config: config/endpoints.yaml
- Schema files: schemas/*.yaml (copy from backend or provide a local schema)
- Environment variables used by zarr_fuse:
	- ZF_S3_ACCESS_KEY
	- ZF_S3_SECRET_KEY
	- ZF_S3_ENDPOINT_URL (optional if schema already includes it)

To switch sources:

```
set HV_DASHBOARD_SOURCE=s3
set HV_DASHBOARD_ENDPOINT=bukov_endpoint
```

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
- data.py: Bukov data loading helpers
- plots.py: Map and time-series builders
- ui.py: Sidebar and depth controls
- requirements.txt: Python dependencies
- setup_env: Bash setup helper
