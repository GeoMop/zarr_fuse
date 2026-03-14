# Zarr FUSE – Deployment Documentation

## Overview

Zarr FUSE Ingress Server is a Flask-based service for collecting, validating and storing scientific data into an S3-backed Zarr store.

The application supports:

- Passive ingestion via HTTP endpoints
- Active data scraping via scheduled jobs
- Schema-based validation and transformation
- Storage in S3-backed Zarr datasets

This document describes how the service is deployed and operated in Kubernetes.

---

# Deployment Environments

The service is deployed on **e-infra Rancher Kubernetes** with two environments.

## Production

- **Namespace:** `ingress-server`
- **URL:** https://zarr-fuse-ingress-server.dyn.cloud.e-infra.cz/
- **Source:** GitHub release tag (`ingress-server/*.*.*`)
- **Purpose:** Stable production workload

## Latest (Development)

- **Namespace:** `ingress-server-latest`
- **URL:** https://zarr-fuse-ingress-server-latest.dyn.cloud.e-infra.cz/
- **Source:** GitHub `main` branch
- **Purpose:** Testing and validation of latest changes

---

# Kubernetes Architecture

```text
GitHub
   │
   ▼
CI Pipeline
   │
   ▼
Docker Image
   │
   ▼
Helm Deployment
   │
   ▼
Ingress Server Pod
   │
   ▼
S3-backed Zarr Store
```

Main Kubernetes components:

| Component  | Purpose                        |
| ---------- | ------------------------------ |
| Deployment | Runs the ingress server pods   |
| Service    | Internal Kubernetes service    |
| Ingress    | Public HTTP access with TLS    |
| ConfigMap  | Application configuration      |
| Secret     | Credentials and authentication |
| PVC        | Persistent queue storage       |

---

# Helm Deployment

The application is deployed using a Helm chart located in:

```text
ingress_server/charts/ingress-server
```

The Helm chart manages:

- container image configuration
- pod resources
- ingress configuration
- secrets and credentials
- persistent queue storage
- application configuration

Important Helm configuration areas:

| Section     | Description                            |
| ----------- | -------------------------------------- |
| image       | Container registry, repository and tag |
| deployment  | Pod replicas and resources             |
| ingress     | External access and TLS                |
| secrets     | S3 credentials and authentication      |
| persistence | Queue directory storage                |

Configuration options are documented directly in:

```text
charts/ingress-server/values.yaml
```

---

# Continuous Integration / Continuous Deployment

Deployment is handled via **GitHub Actions workflows**.

Pipeline stages:

1. Build OCI container image
2. Push image to Docker registry
3. Deploy Helm chart to Kubernetes

Reusable workflow:

```text
.github/workflows/ingress-server-reusable-workflow.yaml
```

This workflow handles:

- container build
- registry authentication
- Helm deployment
- Kubernetes authentication

---

# Application Configuration

Application configuration is provided through YAML files bundled into the container image during build.

Default configuration directory:

```text
inputs/
```

Typical structure:

```text
inputs/
├── endpoints_config.yaml
├── schemas/
├── dataframes/
└── extract/
```

## endpoints_config.yaml

Defines:

- passive ingestion endpoints
- active scrapers
- request rendering and iteration logic

See detailed examples in:

```text
ingress_server/README.md
```

---

# Environment Variables

## Required

| Variable           | Description         |
| ------------------ | ------------------- |
| ZF_S3_ACCESS_KEY   | S3 access key       |
| ZF_S3_SECRET_KEY   | S3 secret key       |
| ZF_S3_ENDPOINT_URL | S3 endpoint         |
| ZF_STORE_URL       | Zarr store location |

## Optional

| Variable    | Description                   | Default           |
| ----------- | ----------------------------- | ----------------- |
| CONFIG_PATH | Configuration directory       | `inputs`          |
| QUEUE_DIR   | Queue directory for ingestion | `./var/zarr_fuse` |
| LOG_LEVEL   | Logging level                 | `INFO`            |
| PORT        | Server port                   | `8000`            |

---

# Application Endpoints

## Health Check

```text
GET /health
```

Used for Kubernetes readiness and liveness probes.

Expected response:

```json
{"status": "ok"}
```

---

## Data Upload (Passive Endpoints)

Endpoints are defined in:

```text
inputs/endpoints_config.yaml
```

Available routes:

```http
POST /api/{endpoint}
POST /api/{endpoint}/{node_path}
```

Example request:

```bash
curl -u user:pass -X POST \
  -H "Content-Type: application/json" \
  https://zarr-fuse-ingress-server.dyn.cloud.e-infra.cz/api/my-endpoint \
  -d '{"temperature": 25.5}'
```

---

# Troubleshooting

## View Logs

Production environment:

```bash
kubectl logs -n ingress-server -l app=ingress-server --tail=100 -f
```

Latest environment:

```bash
kubectl logs -n ingress-server-latest -l app=ingress-server-latest --tail=100 -f
```

Specific pod:

```bash
kubectl logs -n ingress-server pod/<pod-name> -f
```

---

## Exec into Pod

```bash
kubectl exec -it -n ingress-server pod/<pod-name> -- /bin/sh
```

---

## Manual Health Check

```bash
curl https://zarr-fuse-ingress-server.dyn.cloud.e-infra.cz/health
```

Expected response:

```json
{"status":"ok"}
```
