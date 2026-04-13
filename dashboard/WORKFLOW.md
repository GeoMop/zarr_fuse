# Using Dashboard in Another Project: Complete Workflow

## Overview

```
Your New Project
    ↓
    ├─→ pip install zarr-fuse + zarr_fuse.dashboard
    ├─→ Create config/endpoints.yaml
    ├─→ Create schemas/my_schema.yaml
    ├─→ Create .env file
    ├─→ zf-dashboard
    ↓
Dashboard opens at http://localhost:5006
```

## 1. Planning Phase (What you need to know)

Before you start, answer these questions:

| Question | Example Answer |
|----------|-----------------|
| Where is your data? | `s3://my-bucket/data.zarr` or `/local/path/data.zarr` |
| What's your latitude variable? | `latitude` or `lat` |
| What's your longitude variable? | `longitude` or `lon` |
| What's your time dimension? | `time` or `date_time` |
| What's your depth dimension? | `depth` or `height` |
| What's your location/entity ID? | `station_id` or `borehole` |
| What's your main data variable? | `temperature` or `pressure` |

**How to find this:**
```python
import xarray as xr
ds = xr.open_zarr('s3://your-bucket/data.zarr')
print(ds)  # Shows all coordinates and variables
```

## 2. Setup Phase (Commands to run)

### Create project structure
```bash
# Create directory
mkdir my-dashboard
cd my-dashboard

# Create subdirectories
mkdir config schemas
```

### Install packages
```bash
# Virtual environment
python -m venv venv
source venv/bin/activate    # Windows: venv\Scripts\activate

# Install both packages!
pip install zarr-fuse zarr_fuse.dashboard
```

### Create configuration files

**A. Create `config/endpoints.yaml`**
```bash
cat > config/endpoints.yaml << 'EOF'
my_data:
  source:
    type: s3
    store_type: zarr
    uri: s3://my-bucket/data.zarr
  schema:
    file: schemas/my_schema.yaml
    fields:
      lat: latitude
      lon: longitude
      time: time
      depth: depth
      entity: station_id
  defaults:
    metric: temperature
    group_path: /
  labels:
    metric: "Temperature (°C)"
    entity: Weather Station
  visualization:
    map:
      center_lat: 50
      center_lon: 14
      zoom: 8
      cmap: viridis
EOF
```

**B. Create `schemas/my_schema.yaml`**
(Use template from TEMPLATE.md, customize for your schema)

**C. Create `.env`**
```bash
cat > .env << 'EOF'
HV_DASHBOARD_ENDPOINT=my_data
ENDPOINTS_PATH=$(pwd)/config/endpoints.yaml
ZF_S3_ACCESS_KEY=your_key
ZF_S3_SECRET_KEY=your_secret
ZF_S3_ENDPOINT_URL=https://s3.example.com
EOF
```

## 3. Validation Phase (Verify before running)

### Check your data is readable
```bash
python << 'EOF'
import os
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

# 2. Check data is accessible
try:
    node = zf.open_store(schema, MODE='r')
    print("✓ Data store opens successfully")
except Exception as e:
    print(f"✗ Data access error: {e}")
    print("  - Check S3 credentials")
    print("  - Check data path in endpoints.yaml")
    exit(1)

# 3. Check variables exist
try:
    ds = node.dataset
    print(f"✓ Dataset loaded: {list(ds.data_vars.keys())}")
except Exception as e:
    print(f"✗ Dataset error: {e}")
    exit(1)

print("\n✓ All checks passed! Ready to run dashboard")
EOF
```

### Check file structure
```bash
# Verify files exist
ls -la config/endpoints.yaml     # Should exist
ls -la schemas/my_schema.yaml    # Should exist
ls -la .env                      # Should exist
cat .env | grep ENDPOINTS_PATH   # Should show your path
```

## 4. Run Phase (Start the dashboard)

```bash
# Make sure virtual environment is active
source venv/bin/activate        # Windows: venv\Scripts\activate

# Run the dashboard
zf-dashboard

# Output should show:
# Starting server...
# Serving on http://0.0.0.0:5006
```

**Open browser to: http://localhost:5006**

## 5. Usage Phase (What you can now do)

- 📍 See your data on a map
- 📈 View time series at selected locations
- 🎚️ Browse different depth levels
- 🔍 Click on map to select locations
- 📊 Compare multiple variables (if configured)

## 6. Troubleshooting Quick Reference

### Problem: "ENDPOINTS_PATH not set"

**Solution:**
```bash
# Check .env is loaded:
cat .env | grep ENDPOINTS_PATH

# Or set directly:
export ENDPOINTS_PATH=$(pwd)/config/endpoints.yaml
zf-dashboard
```

### Problem: "Cannot open zarr store"

1. **Check data path:**
   ```bash
   # For S3:
   aws s3 ls s3://my-bucket/data.zarr/
   
   # For local:
   ls -la /path/to/data.zarr/
   ```

2. **Check S3 credentials:**
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
     endpoint_url=os.getenv('ZF_S3_ENDPOINT_URL')
   )
   print('S3 OK')
   "
   ```

### Problem: "Schema file not found"

**Solution:**
```bash
# Check path is relative to endpoints.yaml:
cat config/endpoints.yaml | grep "file:"    # Should show: file: schemas/my_schema.yaml
ls schemas/my_schema.yaml                   # File should exist
```

### Problem: "Variable 'temperature' not found"

**Solution:** Field names in endpoints.yaml must match your actual Zarr variables:
```bash
# Check what variables exist:
python << 'EOF'
import xarray as xr
ds = xr.open_zarr('s3://bucket/data.zarr')
print("Available variables:", list(ds.data_vars.keys()))
print("Available coordinates:", list(ds.coords.keys()))
EOF

# Update endpoints.yaml with correct names
```

### Problem: "Dashboard won't start or slow/freezing"

**Solutions:**
1. Check data is not too large
2. Try reducing time range in defaults
3. Check S3 connectivity
4. Try local Zarr file first for testing

## 7. Common Customizations

### Show different variable first
```yaml
# In config/endpoints.yaml
defaults:
  metric: humidity  # Change from "temperature"
```

### Change map zoom/center
```yaml
visualization:
  map:
    center_lat: 40.5
    center_lon: -74.0
    zoom: 10
```

### Use local Zarr instead of S3
```yaml
source:
  uri: /local/path/to/data.zarr
```

And remove S3 env vars from .env

## 8. Deployment

### Development (simple testing)
```bash
zf-dashboard  # Starts on port 5006
```

### Production (AWS/Docker/etc)

See DEPLOYMENT.md for:
- Docker container setup
- Gunicorn server setup
- Environment variable management
- Health checks

## File Checklist

Before running, you should have:

```
my-dashboard/
├── config/
│   └── endpoints.yaml           ✓ Points to YOUR data
├── schemas/
│   └── my_schema.yaml           ✓ Describes YOUR Zarr structure
├── .env                         ✓ YOUR S3 credentials
├── venv/                        ✓ Virtual environment, activated
└── [your data on S3 or local]   ✓ Actually exists
```

## Success Indicators

✓ Dashboard starts without errors
✓ Map loads with your data locations
✓ Clicking map updates time series
✓ Depth selector works
✓ No console errors

## Quick Reference: Environment Variables

```bash
# Required
HV_DASHBOARD_ENDPOINT=my_data
ENDPOINTS_PATH=/path/to/endpoints.yaml

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

## Next Steps After Getting It Working

1. **Add more variables:** Add more data_vars to endpoints.yaml
2. **Add multiple endpoints:** Define multiple datasets in endpoints.yaml
3. **Deploy to production:** See DEPLOYMENT.md
4. **Customize UI:** Adjust labels, colors, map settings
5. **Monitor performance:** Log usage, track errors

