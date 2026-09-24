# Using the Dashboard in a New Project: Templates and Workflow

This guide takes you from scratch to a running dashboard with your own data. It combines the
complete workflow (planning → setup → validation → run) with ready-to-copy template files.

## Overview

```
Your New Project
    ↓
    ├─→ 1. Planning     what you need to know about your data
    ├─→ 2. Setup        create structure, install, write config files
    ├─→ 3. Validation   verify schema loads and data is readable
    ├─→ 4. Run          zf-dashboard
    ↓
Dashboard opens at http://localhost:5006
```

## Prerequisites

- Python 3.11+
- Access to your Zarr data (local or S3)
- Knowledge of your data schema (fields, dimensions, coordinates)

## Step 1: Planning Phase (Know This Before Starting)

| Question | Example Answer |
|----------|----------------|
| Where is your data? | `s3://my-bucket/data.zarr` or `/local/path/data.zarr` |
| What's your latitude variable? | `latitude` or `lat` |
| What's your longitude variable? | `longitude` or `lon` |
| What's your time dimension? | `time` or `date_time` |
| What's your depth dimension? | `depth` or `height` |
| What's your location/entity ID? | `station_id` or `borehole` |
| What's your main data variable? | `temperature` or `pressure` |

**How to find this out:**

```python
import xarray as xr

# For S3 (needs ZF_S3_* env vars or boto3 credentials)
ds = xr.open_zarr('s3://your-bucket/your-store.zarr')
print(ds)                        # Shows coordinates, data_vars, dims

# For local files
ds = xr.open_zarr('/path/to/local/data.zarr')
print(ds)

print("Coordinates:", list(ds.coords))
print("Variables:", list(ds.data_vars))
print("Dimensions:", dict(ds.dims))
```

## Step 2: Create Your Project Structure

```bash
mkdir my-data-dashboard
cd my-data-dashboard

# Create subdirectories for configuration and schema
mkdir config schemas
```

Your project should look like:

```
my-data-dashboard/
├── README.md
├── .env                      # Environment variables (create in Step 4)
├── requirements.txt          # Optional: pin versions
├── config/
│   └── zf_view.yaml          # Your view definitions
├── schemas/
│   └── my_schema.yaml        # Your data schema
└── venv/                     # Virtual environment (create in Step 3)
```

## Step 3: Install Dependencies

Install both **zarr_fuse** and **zarr_fuse.dashboard**:

```bash
# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate    # On Windows: venv\Scripts\activate

# Install both packages
pip install zarr-fuse>=0.2.0 zarr_fuse.dashboard

# Verify both are installed
pip list | grep zarr
```

Expected output:

```
zarr-fuse               0.2.0
zarr_fuse.dashboard     0.1.0
```

## Step 4: Templates (Copy and Customize)

Copy the files below as starting points and customize for your data.

### File 1: `.env`

```bash
# .env - Environment Configuration (DO NOT commit this file)

# REQUIRED: Your view name (must match zf_view.yaml key)
HV_DASHBOARD_VIEW=my_view

# REQUIRED: Absolute path to your zf_view.yaml
ZF_VIEW_PATH=/absolute/path/to/my-data-dashboard/config/zf_view.yaml

# S3 Credentials (if your data is on S3; leave blank for AWS S3)
ZF_S3_ACCESS_KEY=your_key_here
ZF_S3_SECRET_KEY=your_secret_here
ZF_S3_ENDPOINT_URL=https://s3.example.com

# Optional: Server configuration
SERVE_BIND=0.0.0.0
SERVE_PORT=5006

# Optional: Tile configuration (for map overlays)
TILE_BUCKET=my-bucket
TILE_PREFIX=my_tiles/
# ZF_CACHE_DIR=/tmp/zf_tiles
```

> For local Zarr files you don't need the S3 vars. Just set `HV_DASHBOARD_VIEW` and
> `ZF_VIEW_PATH`.

### File 2: `config/zf_view.yaml`

```yaml
# config/zf_view.yaml - Define your data sources

# The key (e.g., "my_view") is what you put in HV_DASHBOARD_VIEW
my_view:
  description: "My Scientific Dataset"
  version: "1.0.0"
  reload_interval: 300

  # Your data location
  source:
    type: "s3"                            # or "local" for local files
    store_type: "zarr"
    # S3:    uri: "s3://bucket-name/path/to/store.zarr"
    # Local: uri: "/local/path/to/store.zarr"
    uri: "s3://my-bucket/my-store.zarr"

  # Link to your schema file (relative to this file)
  schema:
    file: "schemas/my_schema.yaml"
    fields:
      # Map your data variable names to common names
      lat: "latitude"                     # Your lat variable
      lon: "longitude"                    # Your lon variable
      time: "time"                        # Your time dimension
      depth: "depth"                      # Your depth dimension
      entity: "station_id"                # Your location/entity ID

  # Defaults when the dashboard loads
  defaults:
    display_variable: "temperature"       # Which variable to show first
    group_path: "/"                       # Start from root

  # Map display settings
  visualization:
    map:
      center_lat: 50.0                    # Map center
      center_lon: 14.0                    # Map center
      zoom: 8
      title: "My Dataset Map"
      point_size: 10
      alpha: 0.8
      cluster:
        cluster_enabled: true
        cluster_eps_factor: 0.05
        cluster_buffer_factor: 0.1
        cluster_size_scale: 3.0

    timeseries:
      middle_window_days: 30
      right_window_hours: 24
```

### File 3: `schemas/my_schema.yaml`

```yaml
# schemas/my_schema.yaml - Describe your Zarr data structure
# See zarr_fuse documentation for the full schema format.

root:
  description: "My dataset"

  ds:
    ATTRS:
      STORE_URL: ""                       # Left empty, filled at runtime

  # Your coordinate variables
  coordinates:
    latitude:
      ATTRS: {}
    longitude:
      ATTRS: {}
    time:
      ATTRS: {}
    depth:
      ATTRS: {}
    station_id:                           # Your entity/location ID
      ATTRS: {}

  # Your data variables
  data_vars:
    temperature:                          # Change to your variable names
      dimensions: ["time", "depth", "station_id"]
      dtype: "float32"
      ATTRS:
        long_name: "Temperature"
        units: "Celsius"

    humidity:
      dimensions: ["time", "depth", "station_id"]
      dtype: "float32"
      ATTRS:
        long_name: "Relative Humidity"
        units: "%"

    # Add more variables as needed...
```

### File 4: `requirements.txt` (Optional - for reproducibility)

```text
# requirements.txt - Pin exact versions

zarr-fuse==0.2.0
zarr_fuse.dashboard==0.1.0

# These are already dashboard dependencies, but can be pinned if needed:
# panel==1.4.0
# holoviews==1.19.0
# xarray==0.19.0
```

## Step 5: Quick Setup (Copy-Paste Commands)

```bash
# 1. Create project
mkdir my-data-dashboard
cd my-data-dashboard
mkdir config schemas

# 2. Create virtual environment and install
python -m venv venv
source venv/bin/activate                  # On Windows: venv\Scripts\activate
pip install zarr-fuse>=0.2.0 zarr_fuse.dashboard

# 3. Create .env (replace the absolute path with YOUR project path)
cat > .env << 'EOF'
HV_DASHBOARD_VIEW=my_view
ZF_VIEW_PATH=/absolute/path/to/my-data-dashboard/config/zf_view.yaml
ZF_S3_ACCESS_KEY=your_key
ZF_S3_SECRET_KEY=your_secret
ZF_S3_ENDPOINT_URL=https://s3.example.com
EOF

# 4. Create config/zf_view.yaml from "File 2" above, customize for your data

# 5. Create schemas/my_schema.yaml from "File 3" above, customize for your data

# 6. Run dashboard
zf-dashboard
```

> The `.env` file is read literally by python-dotenv: use a real absolute path, not
> `$(pwd)` (that only expands in a shell `export`, not inside `.env`).

## Step 6: Validation Phase (Verify Before Running)

### Verification checklist

Before running, verify:

- [ ] `config/zf_view.yaml` exists and has the correct `uri` pointing to your data
- [ ] `schemas/my_schema.yaml` exists and describes your Zarr structure
- [ ] `.env` has the correct `ZF_VIEW_PATH`
- [ ] `.env` has `HV_DASHBOARD_VIEW` matching the zf_view.yaml key
- [ ] S3 credentials set (if using S3)
- [ ] Virtual environment activated
- [ ] Both packages installed: `pip list | grep zarr`

### Quick test

```bash
python << 'EOF'
from pathlib import Path
import zarr_fuse as zf
from dotenv import load_dotenv

load_dotenv()

# 1. Check schema loads
try:
    schema = zf.schema.deserialize(Path('schemas/my_schema.yaml'))
    print("✓ Schema loads successfully")
except Exception as e:
    print(f"✗ Schema error: {e}")
    exit(1)

# 2. Check data is accessible (tests S3/local access)
try:
    node = zf.open_store(schema, MODE='r')
    print("✓ Data store opens successfully")
except Exception as e:
    print(f"✗ Data access error: {e}")
    print("  - Check ZF_VIEW_PATH and S3 credentials")
    exit(1)

# 3. Check variables exist
try:
    ds = node.dataset
    print(f"✓ Dataset loaded, variables: {list(ds.data_vars.keys())}")
except Exception as e:
    print(f"✗ Dataset error: {e}")
    exit(1)

print("\n✓ All checks passed! Ready to run dashboard")
EOF
```

### Check file structure

```bash
ls -la config/zf_view.yaml      # Should exist
ls -la schemas/my_schema.yaml   # Should exist
ls -la .env                     # Should exist
grep ZF_VIEW_PATH .env          # Should show your path
```

## Step 7: Run Phase (Start the Dashboard)

```bash
# Make sure the virtual environment is active
source venv/bin/activate        # On Windows: venv\Scripts\activate

# Run the dashboard (auto-loads .env)
zf-dashboard

# Open browser to http://localhost:5006
```

**For quick testing without `.env`**, set the vars explicitly:

```bash
export HV_DASHBOARD_VIEW=my_view
export ZF_VIEW_PATH=/absolute/path/to/my-data-dashboard/config/zf_view.yaml
zf-dashboard
```

**What you can now do:**

- See your data on a map
- Click map locations to view time series
- Browse different depth levels
- Compare multiple variables (if configured)

## Step 8: Troubleshooting Quick Reference

### "zarr_fuse not found"

Install it explicitly:

```bash
pip install zarr-fuse zarr_fuse.dashboard
```

### "ZF_VIEW_PATH not set" / "Views file not found"

```bash
# Check .env is loaded:
grep ZF_VIEW_PATH .env

# Or set directly:
export ZF_VIEW_PATH=$(pwd)/config/zf_view.yaml
zf-dashboard
```

### "Schema file not found: schemas/my_schema.yaml"

The path in `zf_view.yaml` is relative to that file:

```bash
grep "file:" config/zf_view.yaml    # Should show: file: schemas/my_schema.yaml
ls schemas/my_schema.yaml           # File should exist
```

Or use an absolute path in zf_view.yaml:

```yaml
schema:
  file: "/absolute/path/to/schemas/my_schema.yaml"
```

### "Cannot open zarr store" / "S3 connection failed"

1. **Check the data path:**

   ```bash
   # For S3:
   aws s3 ls s3://my-bucket/my-store.zarr/

   # For local:
   ls -la /path/to/local/data.zarr/
   ```

2. **Check the S3 credentials:**

   ```bash
   echo $ZF_S3_ACCESS_KEY    # Should not be empty
   echo $ZF_S3_SECRET_KEY
   echo $ZF_S3_ENDPOINT_URL
   ```

3. **Test S3 directly:**

   ```bash
   python -c "
   import boto3, os
   s3 = boto3.client('s3',
     aws_access_key_id=os.getenv('ZF_S3_ACCESS_KEY'),
     aws_secret_access_key=os.getenv('ZF_S3_SECRET_KEY'),
     endpoint_url=os.getenv('ZF_S3_ENDPOINT_URL'))
   print('S3 OK')
   "
   ```

### "HV_DASHBOARD_VIEW is required"

The view name in `.env` must match the key in zf_view.yaml:

```yaml
my_view:            # <-- This must match HV_DASHBOARD_VIEW
  source: ...
```

```bash
export HV_DASHBOARD_VIEW=my_view   # Must match!
```

### "Variable 'temperature' not found"

Field names in zf_view.yaml must match your actual Zarr variables:

```bash
python << 'EOF'
import xarray as xr
ds = xr.open_zarr('s3://bucket/data.zarr')  # or your local path
print("Available variables:", list(ds.data_vars.keys()))
print("Available coordinates:", list(ds.coords.keys()))
EOF

# Update zf_view.yaml with the correct names
```

### "Dashboard won't start" or is slow/freezing

1. Check the data is not too large
2. Try reducing the time range in defaults
3. Check S3 connectivity
4. Try a local Zarr file first for testing

## Step 9: Common Customizations

**Change which variable shows first:**

```yaml
# In config/zf_view.yaml
defaults:
  display_variable: "humidity"   # Instead of "temperature"
```

**Change map center/zoom:**

```yaml
visualization:
  map:
    center_lat: 40.0
    center_lon: -95.0
    zoom: 5
```

**Change point size or opacity:**

```yaml
visualization:
  map:
    point_size: 12
    alpha: 0.6
```

**Tune map point clustering:**

```yaml
visualization:
  map:
    cluster:
      cluster_enabled: true
      cluster_eps_factor: 0.1      # Higher merges more points
      cluster_buffer_factor: 0.2
      cluster_size_scale: 4.0
```

**Use local Zarr instead of S3:**

```yaml
# In zf_view.yaml:
source:
  uri: "/path/to/local/data.zarr"
```

And remove the S3 vars (or leave them blank) in `.env`.

## Step 10: Deployment

### Development (simple testing)

```bash
zf-dashboard   # Starts on port 5006
```

### Production (Gunicorn + Panel)

```bash
pip install gunicorn

gunicorn --worker-class gthread --workers 1 --threads 4 \
  --bind 0.0.0.0:5006 \
  --env ZF_VIEW_PATH=/path/config/zf_view.yaml \
  --env HV_DASHBOARD_VIEW=my_view \
  'dashboard.composed:build_dashboard'
```

### In Docker

```dockerfile
FROM python:3.11-slim

WORKDIR /ui

# Install dependencies
RUN pip install zarr-fuse>=0.2.0 zarr_fuse.dashboard

# Copy your config
COPY config/ /ui/config/
COPY schemas/ /ui/schemas/

# Set required env vars
ENV HV_DASHBOARD_VIEW=my_view
ENV ZF_VIEW_PATH=/ui/config/zf_view.yaml

# Run dashboard
CMD ["zf-dashboard"]
```

Build and run:

```bash
docker build -t my-dashboard .
docker run -p 5006:5006 my-dashboard
```

For production setups (multiple instances, environment management, health checks) see
[DEPLOYMENT.md](DEPLOYMENT.md).

## File Checklist

Before running, you should have:

```
my-data-dashboard/
├── config/
│   └── zf_view.yaml           ✓ Points to YOUR data
├── schemas/
│   └── my_schema.yaml         ✓ Describes YOUR Zarr structure
├── .env                       ✓ YOUR S3 credentials (if using S3)
├── venv/                      ✓ Virtual environment, activated
└── [your data on S3 or local] ✓ Actually exists
```

## Success Indicators

- Dashboard starts without errors
- Map loads with your data locations
- Clicking the map updates time series
- Depth selector works
- No console errors

## Quick Reference: Environment Variables

```bash
# Required
HV_DASHBOARD_VIEW=my_view
ZF_VIEW_PATH=/path/to/zf_view.yaml

# S3 (if using)
ZF_S3_ACCESS_KEY=xxx
ZF_S3_SECRET_KEY=xxx
ZF_S3_ENDPOINT_URL=https://s3.example.com

# Optional
SERVE_BIND=0.0.0.0              # Default: 0.0.0.0
SERVE_PORT=5006                 # Default: 5006
TILE_BUCKET=my-bucket           # For overlays
TILE_PREFIX=tiles/
ZF_CACHE_DIR=/tmp/cache
```

## Next Steps

1. **Add more variables:** Add more data_vars to your schema
2. **Add multiple views:** Define multiple datasets in zf_view.yaml
3. **Deploy to production:** See [DEPLOYMENT.md](DEPLOYMENT.md)
4. **Customize the UI:** Adjust defaults, map settings, clustering
5. **Monitor performance:** Log usage, track errors