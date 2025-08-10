# ZarrFuse Ingress (Backend Only)

Backend-only Flask service that accepts CSV/JSON uploads and merges them into an S3-backed Zarr store using `zarr-fuse`.
There is **no UI**; interaction happens via HTTP endpoints (multipart uploads).

## Overview

- **Purpose:** Ingest tabular data (CSV/JSON) and merge it into a Zarr dataset on S3.
- **Where to write:** S3 endpoint and path are defined in the *schema* (`ATTRS.s3_url`, `ATTRS.store_path`).
- **How endpoints are defined:** via `endpoints_config.yaml`.
- **Auth:** S3 credentials via environment variables. HTTP auth is not implemented by default (see notes).
- **Swagger/OpenAPI:** Generated dynamically from `endpoints_config.yaml` using `tools/gen_swagger.py`.

## Repository Layout

```
.
├─ ingress_server/
│  ├─ src/
│  │  ├─ main.py                  # Flask backend server
│  │  ├─ endpoints_config.yaml    # List of API endpoints and their schema paths
│  │  └─ schemas/                 # Schema files (define S3 target + variables)
│  ├─ docs/
│  │  └─ swagger.yaml             # Generated OpenAPI spec
│  ├─ tools/
│  │  └─ gen_swagger.py           # Swagger generator script
│  ├─ oci/
│  │  ├─ Dockerfile               # Container build
│  │  └─ README.md                # Docker/OCI usage instructions
│  └─ pyproject.toml              # Project metadata & dependencies
```

---

## Configuration

### 1) Environment (.env)

```ini
# Required
S3_ACCESS_KEY=YOUR_ACCESS_KEY
S3_SECRET_KEY=YOUR_SECRET_KEY
```

The server reads **S3 credentials** from environment, while **S3 endpoint** and **store path** come from the schema.

### 2) Endpoints (src/endpoints_config.yaml)

```yaml
endpoints:
  - name: tree
    endpoint: /api/v1/tree
    schema_path: schemas/tree_schema_v1.yaml

  - name: weather
    endpoint: /api/v1/weather
    schema_path: schemas/weather_schema_v1.yaml

  - name: sensor
    endpoint: /api/v1/sensor
    schema_path: schemas/sensor_schema_v1.yaml
```

Each entry creates two routes:

- `POST {endpoint}` — updates the root (`/`) of the Zarr tree
- `POST {endpoint}/{node_path}` — updates a specific node (e.g. `/api/v1/tree/a/b/c`)

### 3) Schema (example)

```yaml
ATTRS:
  description: Weather schema
  s3_url: https://s3.cl4.du.cesnet.cz
  store_path: test/test.zarr  # bucket + filename (can include subdirs)
```

`store_path` is in the form `bucket_name/path_in_bucket`.

---

## Running

### Local

```bash
# Install latest pip & dependencies
pip install -U pip
pip install .

# Run the server
python src/main.py
```

### Generate Swagger

```bash
python tools/gen_swagger.py
# Output will be in docs/swagger.yaml
```

---

## API

- Base URL: `http://localhost:8000`
- Content-Type: `multipart/form-data`
- Form field: `file` (CSV or JSON)

### Endpoints

- `POST /api/v1/{name}`
- `POST /api/v1/{name}/{node_path}`

Where `{name}` is one of the configured names (`tree`, `weather`, `sensor`) and `{node_path}` is a Zarr node path like `level1/level2`.

---

## Examples

### Upload to **tree** (root)
```bash
curl -X POST http://localhost:8000/api/v1/tree \
  -F "file=@data/example.csv"
```

### Upload to **weather** (specific node)
```bash
curl -X POST http://localhost:8000/api/v1/weather/measurements/daily \
  -F "file=@data/weather_daily.csv"
```

### Upload to **sensor** (specific node)
```bash
curl -X POST http://localhost:8000/api/v1/sensor/stations/bedrichov \
  -F "file=@data/bedrichov.json"
```

---

## Error Responses

- `400 Bad Request` — missing file, unsupported type, schema misconfiguration, or update errors.
- Response contains:
  ```json
  {
    "error": "Error message",
    "trace": "Stack trace"
  }
  ```

---

## Notes

- **Auth:** HTTP authentication not enabled by default; add `Flask-HTTPAuth` if needed.
- **Permissions:** S3 user must have read/write permissions to `store_path`.
- **File Types:** Only CSV and JSON supported by default.
- **Schema Ownership:** Each endpoint points to its own schema.
- **Swagger/OpenAPI:** Run `python tools/gen_swagger.py` after changing `endpoints_config.yaml` to regenerate `docs/swagger.yaml`.
