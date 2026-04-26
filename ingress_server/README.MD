# ZarrFuse Ingress Server

Flask-based ingress service for collecting, validating, and storing scientific
data into an S3-backed Zarr store using **zarr-fuse**.

The server supports:

- Passive ingestion endpoints for CSV/JSON uploads
- Active scrappers for scheduled data downloads
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
| Active scrappers    | Scheduled data downloads             |
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

The `configuration` block at the top of the file controls runtime behaviour and
SMTP notifications. All settings except secrets live here — not in `.env`.

```yaml
configuration:
  base:
    queue_dir_path: ./var/zarr_fuse   # local ingestion queue directory
    log_level: INFO
    port: 8000
    worker_poll_interval: 30          # seconds between worker cycles

  smtp:
    notify_to: ["ops@example.com"]
    host: smtp.gmail.com
    port: 587
    from_email: "zarr.fuse.support@gmail.com"
    username: "zarr.fuse.support@gmail.com"
    password: ""                      # leave empty — set SMTP_PASSWORD in .env
```

The rest of the file defines passive endpoints and active scrappers (see sections below).

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

Active scrappers periodically download data from external sources and ingest
them into the Zarr store.

Example configuration:
```yaml
active_scrappers:
  # Periodic jobs that fetch data from remote sources and push it into the pipeline.
  - name: chmi-aladin-1km                         # Logical scrapper identifier
    schema_path: schemas/hlavo_surface_schema.yaml  # Path to schema describing the target dataset
    target_node: chmi_aladin_10m                  # Target node in the Zarr store
    extract_fn: extract_chmi_grib                 # Extractor function used to process the downloaded payload
    fn_module: inputs.extract.chmi                # Python module containing the extractor

    runs:
      # Execution schedule definitions.
      # Each item defines one cron schedule and optional fixed values.
      - cron: "*/1 * * * *"                       # Run every minute
        set:
          time: "00"                              # Fixed parameter available during rendering

    request:
      # HTTP request definition used to download the remote payload.
      # Placeholders in {braces} are resolved from values produced by
      # render, iterate, or run-set variables.
      method: GET                                 # HTTP method
      url: "https://opendata.chmi.cz/meteorology/weather/nwp_aladin/CZ_1km/{time}/ALADCZ1K4opendata_{date}{time}_{quantity}.grb.bz2"

      headers:
        # Optional HTTP headers included in the request.
        - header_name: "User-Agent"
          header_value: "MyWeatherApp/1.0 (your_email@example.com)"

      query_params:
        # Query parameters appended to the request URL (?name=value).
        # Values are rendered using the same template context as the URL.
        - name: lat
          value: "{dflat}"
        - name: lon
          value: "{dflon}"

    render:
      # Values rendered once before request execution.
      # These values are added to the template context and can be referenced
      # in the request URL, headers, or query parameters.
      - name: date
        source: datetime_utc                      # Source of the value
        format: "%Y%m%d"

      - name: unknown
        source: const
        value: "unknown"

    iterate:
      # Iteration sources produce multiple request executions.
      # Each produced value extends the template context.

      - name: quantity                            # Name of the iteration context
        source: schema                            # Source type
        target_node: "chmi_aladin_10m"            # Target node in the schema
        schema_regex: "VARS.*.df_col"             # Iterate over matching values from the schema

      - name: dataframe_row                                     # Name of the iteration context
        source: dataframe                                       # Source type
        dataframe_path: dataframes/hlavo_surface_dataframe.csv  # Path to CSV file
        dataframe_has_header: true                              # Indicates that the CSV contains a header row
        # Mapping of template variable names to dataframe column names.
        # Example: template variable 'dflat' will receive the value from column 'dflat'.
        outputs:
          dflat: dflat
          dflon: dflon

```

---

# Environment Variables

Only secrets and optional path overrides belong in `.env`. All other settings
(port, log level, SMTP host, recipients, etc.) are configured in
`endpoints_config.yaml`.

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
# S3 CONFIGURATION
ZF_S3_ACCESS_KEY=key
ZF_S3_SECRET_KEY=key
ZF_S3_ENDPOINT_URL=https://s3.cl4.du.cesnet.cz
ZF_STORE_URL=s3://test....

# AUTH
BASIC_AUTH_USERS_JSON={"user":"password"}

# SMTP — password only, all other SMTP settings are in endpoints_config.yaml
SMTP_PASSWORD=your_gmail_app_password

# Optional overrides
#CONFIG_PATH=inputs/endpoints_config.yaml
```

### Variable Description

| Variable                | Description                                                      |
| ----------------------- | ---------------------------------------------------------------- |
| `ZF_S3_ACCESS_KEY`      | S3 access key                                                    |
| `ZF_S3_SECRET_KEY`      | S3 secret key                                                    |
| `ZF_S3_ENDPOINT_URL`    | S3 endpoint URL                                                  |
| `ZF_STORE_URL`          | Zarr store location                                              |
| `BASIC_AUTH_USERS_JSON` | Basic auth users as a JSON object `{"user":"password"}`          |
| `SMTP_PASSWORD`         | SMTP password (for Gmail use App Password, not account password) |
| `CONFIG_PATH`           | Override path to `endpoints_config.yaml` (optional)             |


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
