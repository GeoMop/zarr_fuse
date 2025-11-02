# Ingress System

The **Ingress System** provides a backend pipeline for collecting, queuing, and processing data into **S3-backed Zarr datasets**.

It is composed of two deployable services:

1. 🌀 **Ingress Service** – A FastAPI-based data collector
   - Accepts **passive uploads** via HTTP endpoints
   - Runs **active scrappers** to periodically fetch remote data
   - Stores data + metadata into an **S3 queue**

2. ⚙️ **Extractor Service** – An Apache Airflow DAG for data processing
   - Reads queued data and metadata from S3
   - Merges them into Zarr datasets using [`zarr-fuse`](https://github.com/GeoMop/zarr_fuse)
   - Moves processed or failed items into `queue/processed/` or `queue/failed/`

Together, these components form a complete ingestion-to-processing workflow.

---

## Architecture

       ┌──────────────────────────┐
       │      External Sources    │
       └─────────────┬────────────┘
                     │  HTTP POST / Scheduled Scrappers
                     ▼
        ┌────────────────────────┐
        │   Ingress Service      │
        │ (FastAPI + APScheduler)│
        ├────────────────────────┤
        │ Endpoints + Auth       │
        │ Upload                 │
        └───────┬────────────────┘
                │  S3 Put (data + metadata)
                ▼
        ┌────────────────────────┐
        │        S3 Queue        │
        │ queue/accepted/...     │
        └────────┬───────────────┘
                 │  Airflow reads + writes
                 ▼
        ┌────────────────────────┐
        │   Extractor Service    │
        │ (Airflow DAG + Zarr)   │
        ├────────────────────────┤
        │ Data merge + move      │
        └────────────────────────┘

---

## Repository Layout

```
.
├── packages/                         # Common shared Python package
│   ├── common/
│   │   ├── configuration.py          # Loads S3 and endpoint/scrapper configs
│   │   ├── s3io.py                   # Uploads payloads and metadata to S3
│   │   ├── validation.py             # Validates JSON/CSV payloads
│   │   ├── logging_setup.py          # UTC logging
│   │   └── models/
│   │       ├── configuration_model.py
│   │       └── metadata_model.py
│   └── pyproject.toml

├── services/
│   ├── ingress_service/              # FastAPI ingress backend
│   │   ├── main.py                   # Entry point, registers endpoints/scrappers
│   │   ├── web/
│   │   │   ├── api.py                # Upload endpoints
│   │   │   └── auth.py               # HTTP Basic Auth
│   │   ├── scrapper/
│   │   │   ├── scheduler.py          # APScheduler setup
│   │   │   └── jobs.py               # Scrapper job execution
│   │   └── pyproject.toml
│   │
│   ├── extractor_service/            # Airflow DAG processor
│       ├── dags/dag.py               # Airflow DAG: processes queue -> Zarr
│       └── pyproject.toml
│
├── inputs/
│   ├── configuration.yaml            # Endpoints and scrappers configuration
│   └── schemas/
│       └── bukov_schema_v1.yaml      # Zarr-fuse schema definitions
│
├── charts/zarr-fuse-ingress/         # Helm chart for Kubernetes deployment
├── oci/                              # Container build definitions
│   ├── ingress.Containerfile         # Containerfile for ingress service
│   └── extractor.Containerfile       # Containerfile for extractor service
├── docker-compose.yaml               # Local stack for development
├── tools/request_example.py          # Example HTTP request sender
└── pyproject.toml                    # Root project metadata
```

---

---

## 1. Ingress Service

### Purpose
Handles all data ingestion, both **passive (via API)** and **active (via scrappers)**, and writes payloads into S3 for later processing.

### Configuration

`.env example:`
```ini
S3_ACCESS_KEY=your_access_key
S3_SECRET_KEY=your_secret_key
S3_ENDPOINT_URL=https://s3.endpoint
S3_STORE_URL=s3://bucket/path/to/store.zarr
LOG_LEVEL=INFO
BASIC_AUTH_USERS_JSON={"user":"password"}
```

`inputs/configuration.yaml:`
```yaml
endpoints:
  - name: weather
    endpoint: /api/v1/weather
    schema_name: bukov_schema_v1.yaml

active_scrappers:
  - name: meteo
    url: https://example.com/data.csv
    cron: "*/15 * * * *"
    schema_name: bukov_schema_v1.yaml
    method: GET
```

Each entry creates two routes:

- `POST {endpoint}` — updates the root (`/`) of the Zarr tree
- `POST {endpoint}/{node_path}` — updates a specific node (e.g. `/api/v1/tree/a/b/c`)

### Running locally

```bash
pip install -e .
python -m services.ingress_service.main
```

### Running with Docker

```bash
docker build -t ingress-service -f oci/ingress.Containerfile .
docker run --rm -p 8000:8000 --env-file .env ingress-service
```


## 2. Extractor Service

### Purpose

Processes data previously uploaded to S3 by the Ingress Service and merges it into Zarr datasets.

### Features

- Periodically scans queue/accepted/
- Validates data and metadata
- Merges records into Zarr datasets
- Moves processed/failed items


### Running with Airflow

```bash
docker build -t extractor-service -f oci/extractor.Containerfile .
docker compose up
```


## Mounting schemas/config without rebuild

The container already includes `schemas/` and `config.yaml` from the build context.
If you want to override them without rebuilding, mount them as read-only volumes:

```bash
-v $(pwd)/src/schemas:/ingress-server/schemas:ro \
-v $(pwd)/src/config.yaml:/ingress-server/config.yaml:ro
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

| Code | Meaning                | Description                |
| ---- | ---------------------- | -------------------------- |
| 202  | Accepted               | Upload stored to S3        |
| 400  | Bad Request            | Invalid request or schema  |
| 401  | Unauthorized           | Wrong credentials          |
| 406  | Not Acceptable         | Invalid CSV/JSON format    |
| 415  | Unsupported Media Type | Content-Type not supported |
