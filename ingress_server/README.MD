# ZarrFuse Ingress – Backend & Docker/OCI

Backend-only Flask service running in a container with **gunicorn**.
The image contains both the `zarr_fuse` library and the ingress server code.
The service accepts CSV/JSON uploads and merges them into an S3-backed Zarr store using `zarr-fuse`.
There is **no UI**; interaction happens via HTTP endpoints (multipart uploads).

---

## URLS:

- **Production:** https://zarr-fuse-ingress-server.dyn.cloud.e-infra.cz/health
- **Latest (main):** https://zarr-fuse-ingress-server-latest.dyn.cloud.e-infra.cz/health


## Overview

- **Purpose:** Ingest tabular data (CSV/JSON) and merge it into a Zarr dataset on S3.
- **Where to write:** S3 endpoint and path are defined in the *schema* (`ATTRS.s3_url`, `ATTRS.store_path`).
- **How endpoints are defined:** via `endpoints_config.yaml`.
- **Auth:** Basic HTTP authentication (Flask-HTTPAuth) is enabled; credentials via environment variables. S3 credentials also come from environment.
- **Swagger/OpenAPI:** Generated dynamically from `endpoints_config.yaml` using `tools/gen_swagger.py`.

---

## Repository Layout

```
.
├─ ingress_server/
│  ├─ src/
│  │  ├─ main.py                  # Flask backend server
│  │  ├─ endpoints_config.yaml    # List of API endpoints and their schema paths
│  │  └─ schemas/                 # Schema files (prod/ci_test)
│  ├─ docs/
│  │  └─ swagger.yaml             # Generated OpenAPI spec
│  ├─ tools/
│  │  └─ gen_swagger.py           # Swagger generator script
│  ├─ oci/
│  │  ├─ Dockerfile               # Container build
│  └─ pyproject.toml              # Project metadata & dependencies
```

---

## Configuration

### 1) Environment (.env)

```ini
# Required
ZF_S3_ACCESS_KEY=YOUR_ACCESS_KEY
ZF_S3_SECRET_KEY=YOUR_SECRET_KEY
ZF_S3_ENDPOINT_URL=S3_ENDPOINT_URL
ZF_STORE_URL=s3://...

# Queue
QUEUE_DIR=./data/zarr_fuse

# Config Path
CONFIG_PATH=/home/stepanmoc/Projects/zarr_fuse/app/databuk/inputs

# Required for HTTP Basic Auth
BASIC_AUTH_USERS_JSON={"alice":"password"}

# Extra
PORT=8000
LOG_LEVEL=DEBUG

```

The server reads **S3 credentials** from environment, while **S3 endpoint** and **store path** come from the schema.

### 2) Configuration (`src/endpoints_config.yaml`)

#### Passive Scrapper
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

#### Active Scrapper
```yaml
active_scrappers:
  - name: chmi-aladin-1km
    schema_path: schemas/hlavo_surface_schema.yaml
    schema_node: chmi_aladin_10m
    extract_fn: extract_chmi_grib
    fn_module: inputs.extract.chmi

    runs:
      - cron: "*/1 * * * *"
        set:
          time: "00"
      - cron: "52 11 * * *"
        set:
          time: "06"

    request:
      method: GET
      url: "https://opendata.chmi.cz/meteorology/weather/nwp_aladin/CZ_1km/{time}/ALADCZ1K4opendata_{date}{time}_{quantity}.grb.bz2"
      headers:
        - header_name: "User-Agent"
          header_value: "MyWeatherApp/1.0 (your_email@example.com)"

    render:
      - name: date
        source: datetime_utc
        format: "%Y%m%d"

    iterate:
      - name: quantity
        source: schema
        schema_node: "chmi_aladin_10m"
        schema_regex: "VARS.*.df_col"

      - name: dataframe_row
        source: dataframe
        dataframe_path: dataframes/chmi_surface_dataframe.csv
        dataframe_has_header: true
        outputs:
          lat: dflat
          lon: dflon
          station_id: station
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

## Running Locally (without Docker)

```bash
# Install latest pip & dependencies
pip install -U pip
pip install .

# Run the server
cd src
python main.py
```

---

## Build (Docker/OCI)

```bash
docker build -t zarr-fuse-ingress \
  -t zarr-fuse-ingress
  -f oci/Containerfile .
```

---

## Run (Docker, local)

The image runs with **waitress** on port **8000**, as user **ingress** (UID `11233`).

```bash
docker run --rm -p 8000:8000 \
  --env-file src/.env \
  zarr-fuse-ingress:latest
```

### Mounting schemas/config without rebuild

The container already includes `schemas/` and `endpoints_config.yaml` from the build context.
If you want to override them without rebuilding, mount them as read-only volumes:

```bash
-v $(pwd)/src/schemas:/ingress-server/schemas:ro \
-v $(pwd)/src/endpoints_config.yaml:/ingress-server/endpoints_config.yaml:ro
```

---

## Generate Swagger

```bash
python tools/gen_swagger.py
# Output will be in docs/swagger.yaml
```

---

## API

- Base URL: `http://localhost:8000`
- Content-Type: `multipart/form-data`
- Form field: `file` (CSV or JSON)
- Authentication: HTTP Basic Auth (`HTTP_USER` / `HTTP_PASS` from environment)

### Endpoints

- `POST /api/v1/{name}`
- `POST /api/v1/{name}/{node_path}`

Where `{name}` is one of the configured names (`tree`, `weather`, `sensor`)
and `{node_path}` is a Zarr node path like `level1/level2`.

---

## Examples

### Python example:

You can also use python exmplae in `tools/request_test.py`

### Upload to **tree**

```bash
curl -X POST http://localhost:8000/api/v1/tree \
   -H "Content-Type: text/csv" \
   -u test:test \
   --data-binary @src/test_data/tree/default.csv
```

### Upload to **weather**

```bash
curl -X POST http://localhost:8000/api/v1/weather \
   -H "Content-Type: text/csv" \
   -u test:test \
   -d @src/test_data/weather/default.csv
```

### Upload to **sensor**

```bash
curl -X POST http://localhost:8000/api/v1/sensor \
   -H "Content-Type: text/csv" \
   -u test:test \
   -d @src/test_data/sensor/default.csv
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
