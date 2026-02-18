# Zarr FUSE - Deployment Documentation

## Overview

Zarr FUSE is a Flask-based ingress server for collecting, validating, and storing scientific data into an S3-backed Zarr store. The application provides both passive data upload endpoints and active data scraping capabilities via scheduled jobs.

## Deployment Environment

The application is deployed on e-infra Rancher Kubernetes with two environments:

### Production Environment
- **Namespace**: `ingress-server`
- **URL**: https://zarr-fuse-ingress-server.dyn.cloud.e-infra.cz/
- **Source**: GitHub tag (ingress-server/*.*.*)
- **Purpose**: Stable production workload

### Latest Environment (Development/Staging)
- **Namespace**: `ingress-server-latest`
- **URL**: https://zarr-fuse-ingress-server-latest.dyn.cloud.e-infra.cz/
- **Source**: GitHub main branch
- **Purpose**: Testing, validation, and latest feature verification

## Repository Structure

```
zarr_fuse/
├── ingress_server/
│   ├── ingress_server/                          # Application source code
│   │   ├── main.py                   # Flask app entry point
│   │   ├── models.py                 # Pydantic data models
│   │   ├── io_utils.py               # I/O and validation utilities
│   │   ├── active_scrapper.py        # Scheduled data scraping jobs
│   │   ├── worker.py                 # Background data processing worker
│   │   └── configs.py                # Configuration loader
│   ├── oci/                          # OCI (Docker) build artifacts
│   ├── charts/                       # Helm chart for Kubernetes deployment
│   │   └── ingress-server/
│   │       ├── Chart.yaml
│   │       ├── values.yaml           # Helm values for configuration
│   │       └── templates/            # Kubernetes resource templates
│   └── pyproject.toml                # Python project configuration
├── .github/workflows/
│   └── ingress-server-reusable-workflow.yaml  # Reusable CI/CD workflow
```

## Helm Deployment

### Chart Information

- **Chart Location**: `ingress_server/charts/ingress-server/`
- **Documentation**: See `values.yaml` for detailed configuration options

### Key Helm Values

The Helm chart manages the following components:

| Component         | Purpose                                             |
| ----------------- | --------------------------------------------------- |
| **Image**         | Container image (registry, repository, tag)         |
| **Deployment**    | Pod replicas, resource limits, security context     |
| **Service**       | Kubernetes service configuration (ClusterIP, ports) |
| **Ingress**       | NGINX ingress with Let's Encrypt TLS certificate    |
| **Configuration** | ConfigMap mounting for application configuration    |
| **Secrets**       | S3 credentials and basic authentication             |
| **Persistence**   | Volumes for data queue directories                  |

## Continuous Integration / Continuous Deployment

### Workflow Architecture

The deployment uses a **reusable workflow pattern** for code reuse and consistency:

```
GitHub Actions Trigger (push/PR)
  ↓
Environment-Specific Workflow
  ↓
Reusable Workflow (ingress-server-reusable-workflow.yaml)
  ├─ Build Docker Image
  ├─ Push to Registry
  └─ Deploy via Helm
```

### Reusable Workflow: `ingress-server-reusable-workflow.yaml`

This workflow handles the complete deployment pipeline:

#### **Inputs** (Configuration)

| Input                    | Type    | Description                            | Example                                        |
| ------------------------ | ------- | -------------------------------------- | ---------------------------------------------- |
| `deploy`                 | boolean | Enable Helm deployment                 | `true`                                         |
| `tag`                    | string  | Docker image tag (`generate` for auto) | `generate`, `v1.2.3`, `latest`                 |
| `namespace`              | string  | Kubernetes namespace                   | `ingress-server`, `ingress-server-latest`      |
| `release-name`           | string  | Helm release name                      | `ingress-server-prod`, `ingress-server-latest` |
| `s3-store-url`           | string  | S3 Zarr store location                 | `s3://bucket/store.zarr`                       |
| `docker-repository`      | string  | Docker registry repository             | `mocstepan/zarr-fuse-ingress-server`           |
| `docker-registry`        | string  | Docker registry URL                    | `docker.io`                                    |
| `configuration-dir-path` | string  | Config files directory (in source)     | `app/databuk/ingress_server/inputs`            |
| `zarr-fuse-ref`          | string  | zarr_fuse branch/tag to checkout       | `main`, `SM-CI-outside-repository`             |
| `extra-helm-args`        | string  | Additional Helm parameters             | `--set image.repository=...`                   |

#### **Secrets** (Authentication & Credentials)

| Secret                  | Description                           | Required      |
| ----------------------- | ------------------------------------- | ------------- |
| `DOCKER_USERNAME`       | Docker registry username              | Yes           |
| `DOCKER_PASSWORD`       | Docker registry token/password        | Yes           |
| `S3_ACCESS_KEY`         | S3 access key for Zarr store          | Yes           |
| `S3_SECRET_KEY`         | S3 secret key for Zarr store          | Yes           |
| `KUBECONFIG`            | Base64-encoded kubeconfig for e-infra | Yes           |
| `BASIC_AUTH_USERS_JSON` | JSON: `{"user": "pass", ...}`         | No (optional) |

#### **Variables** (Configuration)

| Variable          | Description                         | Required |
| ----------------- | ----------------------------------- | -------- |
| `DOCKER_USERNAME` | Docker username (if not in secrets) | No       |
| `S3_ENDPOINT_URL` | S3 endpoint URL                     | Yes      |
| `S3_STORE_URL`    | S3 Zarr store path                  | Yes      |

## Application Configuration

### Configuration Files

Application configuration is managed through YAML files bundled into the Docker image during build. Configuration files are located at the path specified by `configuration-dir-path` input.

#### Directory Structure

```
{configuration-dir-path}/
├── endpoints_config.yaml           # Endpoint and scraper definitions
├── schemas/                        # Data schema definitions
│   ├── example_schema.yaml
│   └── ...
├── dataframes/                     # Sample/reference dataframes
│   ├── example_data.csv
│   └── ...
└── extract/                        # Python extraction modules (optional)
    ├── __init__.py
    └── example_extract.py
```

### Key Configuration Files

#### `endpoints_config.yaml`

Defines HTTP endpoints and active data scrapers:

```yaml
endpoints:
  - name: endpoint-name
    endpoint: /api/endpoint-name
    schema_path: schemas/schema.yaml
    extract_fn: extract_function_name          # optional
    fn_module: module.path.to.extraction       # optional

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

**Key Points:**
- `schema_path`: Relative path to data schema (relative to `CONFIG_PATH`)
- `endpoint`: HTTP path (POST requests accepted here)
- `crons`: Standard cron format (5 fields: minute, hour, day, month, day_of_week)
- `dataframe_path`: CSV file for parametrized scraping (iterate rows for each parameter combination)

#### Schema Files (`schemas/*.yaml`)

Defines Zarr storage structure and variables:

```yaml
# Example: hlavo_surface_schema.yaml
store_url: https://s3.example.com
bucket: my-bucket
path: /store.zarr

coords:
  lat:
    start: 50.84
    end: 50.89
    step: 0.001
  lon:
    start: 14.85
    end: 14.96
    step: 0.001
  time:
    # time dimension handled by application

vars:
  temperature:
    dimensions: [lat, lon, time]
    dtype: float32
    units: °C
  precipitation:
    dimensions: [lat, lon, time]
    dtype: float32
    units: mm
```

#### Dataframe Files (`dataframes/*.csv`)

CSV files for parametrized scraping (referenced by `dataframe_path`):

```csv
site_id,latitude,longitude,name
SITE001,50.855,14.905,Main Weather Station
SITE002,50.865,14.915,Secondary Station
```

Columns are mapped to URL parameters via `query_params.column_name` in config.

## Environment Variables

### Required Environment Variables

| Variable             | Description        | Default |
| -------------------- | ------------------ | ------- |
| `ZF_S3_ACCESS_KEY`   | S3 access key      | -       |
| `ZF_S3_SECRET_KEY`   | S3 secret key      | -       |
| `ZF_S3_ENDPOINT_URL` | S3 endpoint URL    | -       |
| `ZF_STORE_URL`       | Store url for zarr | -       |

### Optional Environment Variables

| Variable      | Description                                 | Default           |
| ------------- | ------------------------------------------- | ----------------- |
| `CONFIG_PATH` | Configuration directory path                | `inputs`          |
| `QUEUE_DIR`   | Directory for data queue                    | `./var/zarr_fuse` |
| `LOG_LEVEL`   | Logging level (DEBUG, INFO, WARNING, ERROR) | `INFO`            |
| `PORT`        | Flask server port                           | `8000`            |



## Application Endpoints

### Health Check

```bash
GET /health
```
Returns JSON status for Kubernetes liveness/readiness probes.

### Data Upload (Passive Endpoints)

Configured via `endpoints_config.yaml`:

```bash
POST /api/{endpoint-name}
POST /api/{endpoint-name}/{node_path}
```

Accepts CSV or JSON payload with basic auth.

**Example:**
```bash
curl -u user:pass -X POST \
  -H "Content-Type: application/json" \
  https://zarr-fuse-ingress-server.dyn.cloud.e-infra.cz/api/my-endpoint \
  -d '{"temperature": 25.5, "humidity": 65}'
```

## Troubleshooting

### Viewing Logs

```bash
# Production
kubectl logs -n ingress-server -l app=ingress-server --tail=100 -f

# Latest
kubectl logs -n ingress-server-latest -l app=ingress-server-latest --tail=100 -f

# Specific pod
kubectl logs -n ingress-server pod/ingress-server-prod-xxxxx -f
```

### Exec into Pod

```bash
kubectl exec -it -n ingress-server deployment/ingress-server-prod -- /bin/bash
```

### Manual Health Check

```bash
curl https://zarr-fuse-ingress-server.dyn.cloud.e-infra.cz/health

# Expected response:
# {"status": "ok"}
```
