# HoloViz Dashboard Helm Deployment

This chart deploys the HoloViz dashboard backend and frontend to Kubernetes using Helm.

## Prerequisites

- Helm 3.x
- kubectl configured for your cluster
- Access to the target namespace
- S3 credentials for the backend

## Chart Location

- Chart: dashboard/charts/holoviz

## Quick Deploy

```bash
# set namespace and release name
NAMESPACE=zarr-fuse-dashboard-development
RELEASE=holoviz-development

# create namespace if needed
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

# deploy
helm upgrade "$RELEASE" dashboard/charts/holoviz \
  --install --atomic --timeout 10m --namespace "$NAMESPACE" \
  --set backend.image.tag=<IMAGE_TAG> \
  --set frontend.image.tag=<IMAGE_TAG> \
  --set backend.s3.secrets.accessKey=<S3_ACCESS_KEY> \
  --set backend.s3.secrets.secretKey=<S3_SECRET_KEY>
```

## Values

Key values in values.yaml:

- backend.image.name, backend.image.tag
- frontend.image.name, frontend.image.tag
- backend.resources / frontend.resources
- ingress.className and ingress.annotations

To override values without editing the chart:

```bash
helm upgrade "$RELEASE" dashboard/charts/holoviz \
  --install --atomic --timeout 10m --namespace "$NAMESPACE" \
  --set backend.resources.requests.memory=256Mi \
  --set backend.resources.limits.memory=512Mi \
  --set frontend.resources.requests.memory=256Mi \
  --set frontend.resources.limits.memory=512Mi
```

## Troubleshooting

- If you see "another operation is in progress", a previous Helm release is stuck. Roll back or uninstall it:

```bash
helm -n "$NAMESPACE" history "$RELEASE"
helm -n "$NAMESPACE" rollback "$RELEASE" <REVISION>
# or
helm -n "$NAMESPACE" uninstall "$RELEASE"
```

- If pods fail with quota errors, reduce memory limits/requests or ask the cluster admin to raise the namespace quota.

## GitHub Actions

The workflow that deploys this chart is:

- .github/workflows/holoviz-dashboard-pull-request.yaml

It runs the same Helm upgrade command with the tag and S3 secrets injected.
