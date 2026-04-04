# Project Template: Copy and Use

Use this as a template for your new dashboard project. Copy/paste these files as starting points and customize for your data.

## Directory Structure

```
my-dashboard-project/
├── README.md                          # Document your project
├── .env                               # Local config (DO NOT commit)
├── requirements.txt                   # Optional: pin versions
├── config/
│   └── endpoints.yaml                 # YOUR ENDPOINTS HERE
├── schemas/
│   └── my_schema.yaml                 # YOUR SCHEMA HERE
└── venv/                              # Virtual environment
```

## File 1: .env (Copy and Customize)

```bash
# .env - Environment Configuration
# Copy this and fill in YOUR values

# REQUIRED: Your dataset name (must match endpoints.yaml key)
HV_DASHBOARD_ENDPOINT=my_data

# REQUIRED: Path to your endpoints.yaml
# Use absolute path or $(pwd)/config/endpoints.yaml
ENDPOINTS_PATH=/path/to/my-dashboard-project/config/endpoints.yaml

# S3 Credentials (if your data is on S3)
ZF_S3_ACCESS_KEY=your_key_here
ZF_S3_SECRET_KEY=your_secret_here
ZF_S3_ENDPOINT_URL=https://s3.example.com  # Leave blank for AWS S3

# Optional: Server Configuration  
SERVE_BIND=0.0.0.0
SERVE_PORT=5006

# Optional: Tile Configuration
TILE_BUCKET=my-bucket
TILE_PREFIX=my_tiles/
# ZF_CACHE_DIR=/tmp/zf_tiles
```

## File 2: config/endpoints.yaml (Copy and Customize)

```yaml
# config/endpoints.yaml - Define your data sources

# The key (e.g., "my_data") is what you put in HV_DASHBOARD_ENDPOINT
my_data:
  description: "My Scientific Dataset"
  version: "1.0.0"
  reload_interval: 300
  
  # Your data location
  source:
    type: "s3"                              # or "local" for local files
    store_type: "zarr"
    # S3: uri: "s3://bucket-name/path/to/store.zarr"
    # Local: uri: "/local/path/to/store.zarr"
    uri: "s3://my-bucket/my-store.zarr"
  
  # Link to your schema file
  schema:
    file: "schemas/my_schema.yaml"          # Relative to this file
    fields:
      # Map your data variables to common names
      # Change these to match YOUR variable names in the Zarr file
      lat: "latitude"                       # Your lat variable
      lon: "longitude"                      # Your lon variable
      time: "time"                          # Your time dimension
      depth: "depth"                        # Your depth dimension
      entity: "station_id"                  # Your location/entity ID
  
  # Start defaults when dashboard loads
  defaults:
    metric: "temperature"                   # Which variable to show first
    group_path: "/"                         # Start from root
  
  # Label customization
  labels:
    metric: "Temperature (°C)"
    y_axis: "Temperature"
    entity: "Weather Station"
    depth_unit: "meters"
  
  # Map display settings
  visualization:
    map:
      center_lat: 50.0                      # Map center
      center_lon: 14.0                      # Map center
      zoom: 8
      title: "My Dataset Map"
      cmap: "viridis"                       # Color scheme
      point_size: 10
      alpha: 0.8
    
    timeseries:
      middle_window_days: 30
      right_window_hours: 24
```

## File 3: schemas/my_schema.yaml (Copy and Customize)

**IMPORTANT:** This describes your Zarr structure. See zarr_fuse documentation for full format.

```yaml
# schemas/my_schema.yaml - Describe your Zarr data structure

root:
  description: "My dataset"
  
  ds:
    ATTRS:
      STORE_URL: ""                         # Left empty, filled at runtime
  
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
    station_id:                             # Your entity/location ID
      ATTRS: {}
  
  # Your data variables
  data_vars:
    temperature:                            # Change to your variable names
      dimensions: ["time", "depth", "station_id"]
      dtype: "float32"
      ATTRS:
        long_name: "Temperature"
        units: "Celsius"
    
    humidity:                               # Add all your variables
      dimensions: ["time", "depth", "station_id"]
      dtype: "float32"
      ATTRS:
        long_name: "Relative Humidity"
        units: "%"
    
    # Add more variables as needed...
```

## File 4: requirements.txt (Optional - for reproducibility)

```text
# requirements.txt - Pin exact versions

zarr-fuse==0.2.0
zarr-fuse-dashboard==0.1.0

# These are already in dashboard dependencies, but can pin if needed:
# panel==1.4.0
# holoviews==1.19.0
# xarray==0.19.0
```

## Quick Setup (Copy-Paste Commands)

```bash
# 1. Create project
mkdir my-dashboard-project
cd my-dashboard-project

# 2. Create subdirectories
mkdir config schemas

# 3. Create virtual environment
python -m venv venv
source venv/bin/activate          # On Windows: venv\Scripts\activate

# 4. Install packages
pip install zarr-fuse zarr-fuse-dashboard

# 5. Create .env file (copy from File 1 above and customize)
cat > .env << 'EOF'
HV_DASHBOARD_ENDPOINT=my_data
ENDPOINTS_PATH=$(pwd)/config/endpoints.yaml
ZF_S3_ACCESS_KEY=your_key
ZF_S3_SECRET_KEY=your_secret
ZF_S3_ENDPOINT_URL=https://s3.example.com
EOF

# 6. Copy config/endpoints.yaml from File 2 above, customize for your data

# 7. Copy schemas/my_schema.yaml from File 3 above, customize for your data

# 8. Run dashboard
zf-dashboard
```

## Verification Checklist

Before running, verify:

✓ `config/endpoints.yaml` exists and has correct `uri` pointing to your data
✓ `schemas/my_schema.yaml` exists and describes your Zarr structure
✓ `.env` has correct `ENDPOINTS_PATH`
✓ `.env` has correct `HV_DASHBOARD_ENDPOINT` matching endpoints.yaml key
✓ S3 credentials set (if using S3)
✓ Virtual environment activated
✓ Both packages installed: `pip list | grep zarr`

## Quick Test

```bash
# Test that zarr-fuse can read your data:
python << 'EOF'
import os
from pathlib import Path
import zarr_fuse as zf
from dotenv import load_dotenv

load_dotenv()

# Load your schema
schema_path = Path('schemas/my_schema.yaml')
schema = zf.schema.deserialize(schema_path)
print("✓ Schema loaded")

# Try to open your store (this tests S3/local access)
try:
    node = zf.open_store(schema, MODE='r')
    print("✓ Store opened successfully")
    print(f"  Available groups: {list(node.children.keys())}")
except Exception as e:
    print(f"✗ Failed to open store: {e}")
    print("  Check ENDPOINTS_PATH, S3 credentials, and schema")
EOF
```

## Running

```bash
# Start dashboard (uses .env automatically)
zf-dashboard

# Open browser to http://localhost:5006
```

## Customization Tips

**Change which variable shows first:**
```yaml
defaults:
  metric: "humidity"  # Instead of "temperature"
```

**Change map center/zoom:**
```yaml
visualization:
  map:
    center_lat: 40.0
    center_lon: -95.0
    zoom: 5
```

**Change color scheme:**
```yaml
visualization:
  map:
    cmap: "plasma"  # Instead of "viridis"
    # Other options: "viridis", "plasma", "inferno", "turbo", "twilight"
```

**Use local Zarr instead of S3:**
```bash
# In endpoints.yaml:
source:
  uri: "/path/to/local/data.zarr"
  
# In .env, comment out or remove S3 variables:
# ZF_S3_ACCESS_KEY=...
# ZF_S3_SECRET_KEY=...
```

## Help

If it doesn't work:

1. Check dashboard output for error messages
2. Verify `schemas/my_schema.yaml` matches your actual Zarr structure
3. Verify `config/endpoints.yaml` has correct field names
4. Run the verification test above
5. See QUICKSTART.md troubleshooting section

