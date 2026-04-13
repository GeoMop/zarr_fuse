# Complete Setup Guide: Using Zarr Fuse Dashboard in a New Project

This guide walks you through setting up the dashboard in a new project from scratch with your own data.

## Prerequisites

- Python 3.11+
- Access to your Zarr data (local or S3)
- Know your data schema (fields, dimensions, coordinates)

## Step 1: Create Your Project Structure

```bash
mkdir my-data-dashboard
cd my-data-dashboard

# Create subdirectories for configuration and schema
mkdir config schemas
```

Your project should look like:
```
my-data-dashboard/
├── config/
│   └── endpoints.yaml        # Your endpoint definitions
├── schemas/
│   └── my_schema.yaml        # Your data schema
└── .env                       # Environment variables (create later)
```

## Step 2: Install Dependencies

Install both **zarr_fuse** and **zarr_fuse.dashboard**:

```bash
# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate    # On Windows: venv\Scripts\activate

# Install zarr_fuse (core package)
pip install zarr-fuse>=0.2.0

# Install zarr_fuse.dashboard
pip install zarr_fuse.dashboard

# Verify both are installed
pip list | grep zarr
```

Expected output:
```
zarr-fuse         0.2.0
zarr_fuse.dashboard  0.1.0
```

## Step 3: Understand Your Data

Before creating config files, know your Zarr structure:

```python
# Python script to inspect your Zarr data
import zarr
import xarray as xr

# For local Zarr store
store = zarr.open_group('s3://your-bucket/your-store.zarr', mode='r')
print("Arrays in store:", list(store.keys()))

# Or use xarray to inspect
ds = xr.open_zarr('s3://your-bucket/your-store.zarr')
print(ds)
print("Coordinates:", list(ds.coords))
print("Variables:", list(ds.data_vars))
print("Dimensions:", dict(ds.dims))
```

**Key info you need:**
- Latitude variable name (e.g., 'latitude', 'lat', 'y')
- Longitude variable name (e.g., 'longitude', 'lon', 'x')
- Time dimension name (e.g., 'time', 'date_time')
- Depth dimension name (e.g., 'depth', 'height_above_ground')
- Entity/location variable (e.g., 'station_id', 'borehole')
- Main data variable to visualize (e.g., 'temperature', 'pressure')

## Step 4: Create Your Zarr Fuse Schema

Create `schemas/my_schema.yaml` describing your data:

```yaml
# schemas/my_schema.yaml
root:
  description: "My scientific dataset"
  
  ds:
    ATTRS:
      STORE_URL: ""  # Will be filled by config
  
  coordinates:
    latitude:
      ATTRS: {}
    longitude:
      ATTRS: {}
    time:
      ATTRS: {}
    depth:
      ATTRS: {}
  
  data_vars:
    temperature:
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
```

**See zarr_fuse documentation for detailed schema format.**

## Step 5: Create Dashboard Endpoints Config

Create `config/endpoints.yaml` pointing to your data:

```yaml
# config/endpoints.yaml
my_data:
  description: "My Data Source"
  version: "1.0.0"
  reload_interval: 300
  
  # Point to your Zarr store (local or S3)
  source:
    type: "s3"
    store_type: "zarr"
    uri: "s3://my-bucket/my-store.zarr"
    # For local: uri: "/local/path/to/data.zarr"
  
  # Reference your schema
  schema:
    file: "schemas/my_schema.yaml"
    fields:
      lat: "latitude"           # Your latitude variable name
      lon: "longitude"          # Your longitude variable name
      time: "time"              # Your time dimension name
      depth: "depth"            # Your depth dimension name
      entity: "station_id"      # Your location/entity variable name
  
  # Default values when dashboard starts
  defaults:
    metric: "temperature"       # Which variable to show first
    group_path: "/"            # Root of your data
  
  # Labels for the dashboard UI
  labels:
    metric: "Temperature (°C)"
    y_axis: "Temperature"
    entity: "Weather Station"
    depth_unit: "meters"
  
  # Map visualization settings
  visualization:
    map:
      center_lat: 50.0
      center_lon: 14.0
      zoom: 8
      title: "My Data Map"
      cmap: "viridis"
      point_size: 10
      alpha: 0.8
    
    timeseries:
      middle_window_days: 30
      right_window_hours: 24
```

## Step 6: Set Up Environment Variables

Create `.env` file in your project directory:

```bash
# .env
# REQUIRED: Which endpoint to load
HV_DASHBOARD_ENDPOINT=my_data

# REQUIRED: Path to your endpoints.yaml
ENDPOINTS_PATH=/path/to/my-data-dashboard/config/endpoints.yaml

# S3 Configuration (if using S3)
ZF_S3_ACCESS_KEY=your_access_key_here
ZF_S3_SECRET_KEY=your_secret_key_here
ZF_S3_ENDPOINT_URL=https://s3.example.com

# Optional: Server configuration
SERVE_BIND=0.0.0.0
SERVE_PORT=5006

# Optional: Tile configuration
TILE_BUCKET=my-bucket
TILE_PREFIX=my_tiles/
```

**For local Zarr files**, you don't need S3 env vars. Just set:
```bash
HV_DASHBOARD_ENDPOINT=my_data
ENDPOINTS_PATH=/path/to/my-data-dashboard/config/endpoints.yaml
```

## Step 7: Test Your Setup

### Option A: Using .env file (recommended)

```bash
# Make sure you're in your project directory
cd my-data-dashboard

# Activate virtual environment
source venv/bin/activate    # On Windows: venv\Scripts\activate

# Run the dashboard (will auto-load .env)
zf-dashboard

# Dashboard opens at http://localhost:5006
```

### Option B: Using explicit env vars (for testing)

```bash
export HV_DASHBOARD_ENDPOINT=my_data
export ENDPOINTS_PATH=/path/to/my-data-dashboard/config/endpoints.yaml
zf-dashboard
```

### Option C: For debugging

If you get errors, run with verbose output:

```bash
# Check if zarr_fuse can read your data
python -c "
from pathlib import Path
import zarr_fuse as zf

schema = zf.schema.deserialize(Path('schemas/my_schema.yaml'))
print('Schema loaded:', schema)

# Try to open your store
node = zf.open_store(schema, MODE='r')
print('Store opened successfully')
print('Available groups:', list(node.children.keys()))
"
```

## Step 8: Deployment

### Development Server (already done with zf-dashboard)

```bash
zf-dashboard
```

### Production Server (Gunicorn + Panel)

```bash
pip install gunicorn

# Run with gunicorn
gunicorn --worker-class gthread --workers 1 --threads 4 \
  --bind 0.0.0.0:5006 \
  --env ENDPOINTS_PATH=/path/config/endpoints.yaml \
  --env HV_DASHBOARD_ENDPOINT=my_data \
  'dashboard.composed:build_dashboard'
```

### In Docker

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install zarr-fuse>=0.2.0 zarr_fuse.dashboard

# Copy your config
COPY config/ /app/config/
COPY schemas/ /app/schemas/

# Set required env vars
ENV HV_DASHBOARD_ENDPOINT=my_data
ENV ENDPOINTS_PATH=/app/config/endpoints.yaml

# Run dashboard
CMD ["zf-dashboard"]
```

Build and run:
```bash
docker build -t my-dashboard .
docker run -p 5006:5006 my-dashboard
```

## Troubleshooting

### Error: "zarr_fuse not found"

**Solution:** Install it explicitly:
```bash
pip install zarr-fuse
```

### Error: "Endpoints file not found"

**Solution:** Set correct ENDPOINTS_PATH:
```bash
export ENDPOINTS_PATH=$(pwd)/config/endpoints.yaml
zf-dashboard
```

### Error: "Schema file not found: schemas/my_schema.yaml"

**Solution:** Verify the path is relative to `config/endpoints.yaml`:
```bash
# Your structure should be:
config/
├── endpoints.yaml
└── ../schemas/
    └── my_schema.yaml
```

Or use absolute path in endpoints.yaml:
```yaml
schema:
  file: "/absolute/path/to/schemas/my_schema.yaml"
```

### Error: "S3 connection failed"

**Solution:** Check S3 credentials:
```bash
# Test S3 access
python -c "
import boto3
import os

s3 = boto3.client('s3',
    aws_access_key_id=os.getenv('ZF_S3_ACCESS_KEY'),
    aws_secret_access_key=os.getenv('ZF_S3_SECRET_KEY'),
    endpoint_url=os.getenv('ZF_S3_ENDPOINT_URL')
)
print('S3 connection successful')
"
```

### Error: "HV_DASHBOARD_ENDPOINT is required"

**Solution:** Endpoint name in .env must match endpoints.yaml:
```yaml
# endpoints.yaml - endpoint name is the key:
my_data:           # <-- This must match HV_DASHBOARD_ENDPOINT
  source: ...
```

```bash
export HV_DASHBOARD_ENDPOINT=my_data  # Must match!
```

## Next Steps

1. ✅ Install zarr-fuse and zarr_fuse.dashboard
2. ✅ Create config/endpoints.yaml
3. ✅ Create schemas/my_schema.yaml
4. ✅ Set environment variables
5. ✅ Run `zf-dashboard`

Your dashboard is now using your own data!

## Additional Resources

- **Zarr Fuse Documentation:** See root [zarr_fuse README](../../../README.md)
- **Dashboard Configuration:** See [CONFIG_PACKAGING.md](CONFIG_PACKAGING.md)
- **Deployment Guide:** See [DEPLOYMENT.md](DEPLOYMENT.md)

