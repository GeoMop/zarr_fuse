# ZarrFuse Ingress Server

Flask-based ingress service for collecting, validating, and storing scientific
data into an S3-backed Zarr store using **zarr-fuse**.

The server supports:

- Passive ingestion endpoints for CSV/JSON uploads
- Active scrapers for scheduled data downloads
- Schema-based validation
- S3-backed Zarr storage

Deployment documentation is available in `doc/ingress-server-deployment.md`

---

# Overview

The ingress server provides a backend service that accepts tabular data and
merges it into a Zarr dataset stored in object storage (S3-compatible).

Data can enter the system in two ways:
- **Passive ingestion** – external systems push CSV/JSON data via HTTP endpoints
- **Active scraping** – scheduled jobs periodically download external datasets

---

# Architecture

```text
External Data Source
        │
        │ HTTP / Scheduled Requests
        ▼
Ingress Server (Flask)
        │
        │ Validation + Extraction
        ▼
zarr-fuse
        │
        ▼
S3-backed Zarr Store
```

Key components:

| Component           | Purpose                              |
| ------------------- | ------------------------------------ |
| Flask API           | Receives HTTP ingestion requests     |
| Active scrapers     | Scheduled data downloads             |
| Extractor functions | Convert payloads to dataframe format |
| zarr-fuse           | Merge data into Zarr datasets        |
| S3 object storage   | Persistent dataset storage           |

---

# Repository Layout

```text
  ingress_server/
  ├── charts
  │   └── ingress-server
  │       ├── Chart.yaml
  │       ├── templates
  │       │   ├── deployment.yaml
  │       │   ├── ingress.yaml
  │       │   ├── pvc.yaml
  │       │   ├── secret.yaml
  │       │   └── service.yaml
  │       ├── values
  │       │   └── minimal-required-values.yaml
  │       └── values.yaml
  │
  ├── docs
  │   └── swagger.yaml
  │
  ├── ingress_server
  │   ├── active_scrapper
  │   │   ├── active_scrapper_config_models.py
  │   │   ├── context.py
  │   │   ├── iterate.py
  │   │   ├── planner.py
  │   │   ├── render.py
  │   │   ├── request_builder.py
  │   │   ├── runner.py
  │   │   └── scheduler.py
  │   │
  │   ├── io
  │   │   ├── content_type.py
  │   │   ├── dataframe.py
  │   │   ├── extractor.py
  │   │   ├── files.py
  │   │   ├── process.py
  │   │   └── validate.py
  │   │
  │   ├── auth.py
  │   ├── configs.py
  │   ├── logging_setup.py
  │   ├── main.py
  │   ├── models.py
  │   ├── types.py
  │   └── worker.py
  │
  ├── inputs
  │   ├── dataframes
  │   │   └── hlavo_surface_dataframe.csv
  │   ├── endpoints_config.yaml
  │   ├── extract
  │   │   └── chmi_aladin_1km_extract.py
  │   ├── requirements.txt
  │   └── schemas
  │       └── chmi-aladin-1km-schema.yaml
  │
  ├── oci
  │   ├── Containerfile
  │   └── install_extractor_dependencies.sh
  │
  ├── tools
  │   ├── request_example.py
  │   └── swagger_gen.py
  │
  ├── tests
  │   └── test_worker_smoke.py
  │
  ├── pyproject.toml
  └── README.md
```

---

# Configuration

Application configuration is defined in YAML files located in the
configuration directory.

Main configuration file:
```text
inputs/endpoints_config.yaml
```
This file defines:

- passive ingestion endpoints
- active scraping jobs

---

# Endpoints Configuration

## Passive Scraper

Passive endpoints accept incoming data pushed via HTTP requests.

Example configuration:
```yaml
endpoints:
  # HTTP endpoints that accept incoming data pushed from external systems.
  - name: endpoint-name                         # Logical endpoint name
    endpoint: /api/endpoint-name                # URL path exposed by the ingress server
    schema_path: schemas/schema.yaml            # Path to the schema describing the target dataset
    target_node: chmi_aladin_10m                # Target node in the Zarr store
    extract_fn: extract_function_name           # Extractor function applied to the incoming payload
    fn_module: inputs.extractor.bukov_extractor # Python module containing the extractor function
```

This creates two API routes:
```http
POST /api/{endpoint}
POST /api/{endpoint}/{node_path}
```

Uploaded data is validated and merged into the configured Zarr dataset.

---

## Active Scraper

Active scrapers periodically download data from external sources and ingest
them into the Zarr store.

Example configuration:
```yaml
active_scrappers:
  # Periodic jobs that fetch data from remote sources and push it into the pipeline.
  - name: chmi-aladin-1km                       # Logical scrapper name
    schema_path: schemas/hlavo_surface_schema.yaml  # Path to the schema describing the target dataset
    target_node: chmi_aladin_10m                # Target node in the Zarr store
    extract_fn: extract_chmi_grib               # Extractor function applied to the downloaded payload
    fn_module: inputs.extract.chmi              # Python module containing the extractor function

    runs:
      # Execution schedule definitions.
      # Each item defines one cron-based run and its fixed parameter values.
      - cron: "*/1 * * * *"                     # Run every minute
        set:
          time: "00"                            # Fixed value available during rendering

    request:
      # HTTP request definition used to download the remote payload.
      # Placeholders in {braces} are resolved from rendered or iterated values.
      method: GET                               # HTTP method
      url: "https://opendata.chmi.cz/meteorology/weather/nwp_aladin/CZ_1km/{time}/ALADCZ1K4opendata_{date}{time}_{quantity}.grb.bz2"

      headers:
        # Optional HTTP headers sent with the request.
        - header_name: "User-Agent"
          header_value: "MyWeatherApp/1.0 (your_email@example.com)"

    render:
      # Values rendered before the request is executed.
      # These values can be referenced from the request template.
      - name: date
        source: datetime_utc                    # Value source
        format: "%Y%m%d"

      - name: unknown
        source: const
        value: "unknown"

    iterate:
      # Iteration sources that produce multiple request executions.
      - name: quantity
        source: schema
        target_node: "chmi_aladin_10m"
        schema_regex: "VARS.*.df_col"

      # Iterate over rows from a CSV dataframe.
      - name: dataframe_row
        source: dataframe
        dataframe_path: dataframes/hlavo_surface_dataframe.csv
        dataframe_has_header: true
        outputs:
          lat: dflat
          lon: dflon
          station_id: station
```

---

# Environment Variables

The application uses environment variables for S3 configuration, authentication and runtime settings.

For local development a template file is provided:

```
.env-template
```

Create your local environment file:

```bash
cp .env-template .env
```

Example `.env` configuration:

```env
ZF_S3_ACCESS_KEY=key
ZF_S3_SECRET_KEY=key
ZF_S3_ENDPOINT_URL=http://localhost:9000

ZF_STORE_URL=s3://

QUEUE_DIR=./var/zarr_fuse

BASIC_AUTH_USERS_JSON={"test":"test"}

PORT=8000
LOG_LEVEL=INFO

# Optional overrides
# CONFIG_PATH=inputs
# QUEUE_DIR_PATH=./var/zarr_fuse
# CONFIG_PATH=/path/to/config
```

### Variable Description

| Variable                | Description                     |
| ----------------------- | ------------------------------- |
| `ZF_S3_ACCESS_KEY`      | S3 access key                   |
| `ZF_S3_SECRET_KEY`      | S3 secret key                   |
| `ZF_S3_ENDPOINT_URL`    | S3 endpoint URL                 |
| `ZF_STORE_URL`          | Zarr store location             |
| `QUEUE_DIR`             | Local ingestion queue directory |
| `BASIC_AUTH_USERS_JSON` | Basic auth users in JSON format |
| `PORT`                  | HTTP server port                |
| `LOG_LEVEL`             | Logging level                   |

---

# Running Locally

Install dependencies:
```bash
pip install -U pip
pip install .
```

Run the server:
```bash
python -m ingress_server.main
```

Server will start at: `http://localhost:8000`

---

# Docker / OCI Build

Build container image:
```bash
docker build -f oci/Containerfile -t zarr-fuse-ingress .
```

Run container:
```bash
docker run --rm -p 8000:8000 \
  --env-file .env \
  zarr-fuse-ingress
```

---

# Swagger / OpenAPI

Swagger specification can be generated from the configuration:
```bash
python tools/swagger_gen.py
```

Output:
```text
docs/swagger.yaml
```

---

# Deployment

Deployment to Kubernetes (Helm, CI/CD pipelines, namespaces, URLs)
is documented in:
```text
doc/ingress-server-deployment.md
```
