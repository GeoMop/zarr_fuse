# Deploying zarr-fuse-dashboard in Another Project

This guide shows how to use the zarr-fuse-dashboard package in a new project with your own data.

## Installation

### Option 1: From PyPI (when published)

```bash
pip install zarr-fuse-dashboard
```

### Option 2: From this repository

```bash
pip install git+https://github.com/geomop/zarr_fuse.git#subdirectory=dashboard
```

### Option 3: Local editable install (for development)

```bash
cd /path/to/zarr_fuse/dashboard
pip install -e .
```

## Quick Start for Your Data

1. **Create a project directory:**

```bash
mkdir my-dashboard-project
cd my-dashboard-project
```

2. **Create your configuration structure:**

```bash
mkdir config schemas
```

3. **Create your endpoints.yaml:**

Create `config/endpoints.yaml` with your data source(s):

```yaml
my_data:
  description: "My scientific dataset"
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
      time: "timestamp"
      depth: "depth_m"
      entity: "station_id"
  
  defaults:
    metric: "temperature"
    group_path: "/"
  
  labels:
    metric: "Temperature (°C)"
    depth_unit: "meters"
    entity: "Weather Station"
  
  visualization:
    map:
      center_lat: 50.0
      center_lon: 14.0
      zoom: 8
      title: "My Data Map"
      cmap: "viridis"
```

4. **Create your schema file:**

Create `schemas/my_schema.yaml` describing your Zarr structure (see zarr_fuse documentation for format).

5. **Create .env file:**

```bash
cp /path/to/dashboard/.env.example .env
```

Edit `.env` with your configuration:

```bash
HV_DASHBOARD_ENDPOINT=my_data
ENDPOINTS_PATH=/path/to/my-dashboard-project/config/endpoints.yaml
ZF_S3_ACCESS_KEY=your_key
ZF_S3_SECRET_KEY=your_secret
ZF_S3_ENDPOINT_URL=https://s3.example.com
```

6. **Run the dashboard:**

```bash
zf-dashboard
```

The dashboard will open at `http://localhost:5006`.

## Environment Variables Reference

| Variable | Required | Default | Purpose |
|----------|----------|---------|---------|
| `HV_DASHBOARD_ENDPOINT` | ✓ | - | Endpoint name from endpoints.yaml to load |
| `ENDPOINTS_PATH` | | config/endpoints.yaml | Path to your endpoints.yaml file |
| `ZF_S3_ACCESS_KEY` | ✓ if S3 | - | AWS/S3 access key |
| `ZF_S3_SECRET_KEY` | ✓ if S3 | - | AWS/S3 secret key |
| `ZF_S3_ENDPOINT_URL` | | - | Custom S3 endpoint URL (e.g., MinIO) |
| `SERVE_BIND` | | 0.0.0.0 | Server bind address |
| `SERVE_PORT` | | 5006 | Server port |
| `TILE_BUCKET` | | app-databuk-test-service | S3 bucket for tile overlays |
| `TILE_PREFIX` | | test_tiles/ | S3 prefix for tiles |
| `ZF_CACHE_DIR` | | system temp dir | Cache directory for tile URLs |

## Using Multiple Endpoints

You can define multiple endpoints in `config/endpoints.yaml`:

```yaml
dataset_a:
  source:
    uri: "s3://bucket-a/store.zarr"
  schema:
    file: "schemas/schema_a.yaml"
  # ... rest of config

dataset_b:
  source:
    uri: "s3://bucket-b/store.zarr"
  schema:
    file: "schemas/schema_b.yaml"
  # ... rest of config
```

Then choose which one to load:

```bash
export HV_DASHBOARD_ENDPOINT=dataset_a
zf-dashboard
```

Or start with a different endpoint without restarting:
- Use the "Node Select" dropdown in the dashboard sidebar

## Troubleshooting

### Dashboard won't start: "Endpoints file not found"

**Solution:** Set `ENDPOINTS_PATH` to point to your config file:

```bash
export ENDPOINTS_PATH=/path/to/my-dashboard-project/config/endpoints.yaml
zf-dashboard
```

### "HV_DASHBOARD_ENDPOINT is required"

**Solution:** Set the environment variable:

```bash
export HV_DASHBOARD_ENDPOINT=my_data
zf-dashboard
```

### S3 connection fails silently

**Solution:** Check your credentials and endpoint:

```bash
export ZF_S3_ACCESS_KEY=your_key
export ZF_S3_SECRET_KEY=your_secret
export ZF_S3_ENDPOINT_URL=https://s3.example.com
zf-dashboard  # Check console for S3 error messages
```

### "Schema file not found"

**Solution:** Ensure schema references in `endpoints.yaml` are relative to the config directory:

```yaml
schema:
  file: "schemas/my_schema.yaml"  # Relative to config/endpoints.yaml
```

Or use absolute paths:

```yaml
schema:
  file: "/absolute/path/to/schema.yaml"
```

## Performance Tips

- **Cache directory:** Set `ZF_CACHE_DIR` to a fast local disk for tile URL caching
- **Port forwarding:** Run behind a reverse proxy (nginx) for production
- **Multiple instances:** Use different `SERVE_PORT` values for each instance

## Production Deployment

See your hosting provider's documentation for deploying Python web apps.

Suggested setup:
- Gunicorn/Uvicorn for ASGI server
- Nginx reverse proxy
- Environment variables via systemd/.env or cloud provider secrets
- Health check endpoint: GET `/health` (if API is enabled)

## Support

For issues:
1. Check environment variables are correctly set
2. Verify endpoints.yaml and schema files are in expected locations
3. Check S3 connectivity if using S3 datasources
4. Review log output for detailed error messages

