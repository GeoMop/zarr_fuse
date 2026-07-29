# Dashboard Helm Deployment

This chart deploys a dashboard frontend to Kubernetes using Helm.

## Prerequisites

- Helm 3.x
- kubectl configured for your cluster
- Access to the target namespace
- S3 credentials for the frontend

## Chart Location

- Chart: dashboard/charts/dashboard

## Quick Deploy

```bash
# set namespace and release name
NAMESPACE=dashboard-development
RELEASE=dashboard

# create namespace if needed
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

# deploy
helm upgrade "$RELEASE" dashboard/charts/dashboard \
  --install --atomic --timeout 10m --namespace "$NAMESPACE" \
  --set frontend.image.tag=<IMAGE_TAG> \
  --set frontend.s3.secrets.accessKey=<S3_ACCESS_KEY> \
  --set frontend.s3.secrets.secretKey=<S3_SECRET_KEY>
```

## Values

Key values in values.yaml:

- frontend.image.name, frontend.image.tag
- frontend.resources
- ingress.host (required for BOKEH_ALLOW_WS_ORIGIN and TLS)
- ingress.className and ingress.annotations
- ingress.tlsSecretName

To override values without editing the chart:

```bash
helm upgrade "$RELEASE" dashboard/charts/dashboard \
  --install --atomic --timeout 10m --namespace "$NAMESPACE" \
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
