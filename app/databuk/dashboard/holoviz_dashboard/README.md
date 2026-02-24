# HoloViz Dashboard

Local Panel + HoloViews dashboard for Bukov Zarr data.

## Quick start

From this folder:

```
./setup_env
panel serve app.py --show
```

## What setup_env does

- Creates a local venv in ./venv
- Installs requirements from requirements.txt
- Registers a Jupyter kernel named "HoloViz Dashboard"

## Requirements notes

- GeoViews uses Cartopy/Proj/GEOS. On Windows, these may require additional system packages.
- If Cartopy install fails, verify that your Python distribution can install binary wheels.

## Files

- app.py: Panel app entrypoint
- data.py: Bukov data loading helpers
- plots.py: Map and time-series builders
- ui.py: Sidebar and depth controls
- requirements.txt: Python dependencies
- setup_env: Bash setup helper
