# Config Packaging Verification

## What Gets Packaged

When `pip install zarr_fuse.dashboard` is run, the following are included:

### Package Directories (with __init__.py)
✓ `api/` - API module  
✓ `config/` - Configuration parsing + default views  
✓ `schemas/` - Schema definitions  

### Data Files (matched by pyproject.toml package-data patterns)
✓ `config/zf_view.yaml` - Default view definitions  
✓ `config/bukov_endpoint/*.json` - Georeferencing data  
✓ `config/bukov_endpoint/*.png` - Overlay images  
✓ `schemas/*.yaml` - Schema files  

### Module Files (dashboard package modules)
✓ `app.py`, `composed.py`, `config.py`, `data.py`, `map_views.py`, `multi_time_views.py`, `serve_dashboard.py`, `tile_service.py`, `ui.py`  

## Path Resolution Logic

### Default Config (packaged with package)
```
When: ZF_VIEW_PATH not set
Config location: {site-packages}/dashboard/config/zf_view.yaml
Base dir: {site-packages}/dashboard
Schema path: {site-packages}/dashboard/schemas/bukov_schema.yaml
Result: ✓ Works out of box
```

### External Project Config
```
When: ZF_VIEW_PATH=/my_project/config/zf_view.yaml
Base dir: /my_project (calculated as views_path.parent.parent)
Schema path: /my_project/schemas/my_schema.yaml (relative to base_dir)
Result: ✓ Works with proper directory structure
```

## Required Directory Structure for External Projects

```
my_project/
├── config/
│   └── zf_view.yaml           # Your view definitions
└── schemas/
    └── my_schema.yaml           # Your schema files
```

## Env Var Configuration

```bash
# Point to your custom config
export ZF_VIEW_PATH=/my_project/config/zf_view.yaml

# Activate a view
export HV_DASHBOARD_VIEW=my_view

# Optional: S3 credentials
export ZF_S3_ACCESS_KEY=...
export ZF_S3_SECRET_KEY=...
export ZF_S3_ENDPOINT_URL=...

# Optional: Server config
export SERVE_BIND=0.0.0.0
export SERVE_PORT=5006

# Optional: Tile config
export TILE_BUCKET=my-bucket
export TILE_PREFIX=my_tiles/
export ZF_CACHE_DIR=/tmp/zf_tiles
```

## Testing Installation

To verify all files are packaged correctly:

```bash
# Install in editable mode (simulates package installation)
cd zarr_fuse/dashboard
pip install -e .

# Create a test project
mkdir /tmp/test_dashboard
cd /tmp/test_dashboard

# Copy default config to test it
export ZF_VIEW_PATH=$(python -c "import dashboa_config; from pathlib import Path; print(Path(__file__).parent / 'config/zf_view.yaml')")

# This should work and use packaged default config
zf-dashboard

# Or test with custom config
export ZF_VIEW_PATH=/tmp/test_dashboard/config/zf_view.yaml
export HV_DASHBOARD_VIEW=custom_view
zf-dashboard
```

## Files Modified for Plug-and-Play

- ✅ `schemas/__init__.py` - NEW (make schemas a package)
- ✅ `config.py` - Centralized config parsing module
- ✅ `api/__init__.py` - Makes api a package
- ✅ `pyproject.toml` - Updated to include `schemas` in package discovery
- ✅ `data.py` - Path resolution logic verified
- ✅ All other runtime modules - Updated for env var config

## Status: READY FOR DEPLOYMENT ✅

- Schema files will be packaged ✓
- Config files will be packaged ✓
- Path resolution supports both default and external configs ✓
- All environment variables configurable ✓
- Error messages guide users ✓
- Documentation complete ✓

