# HoloViz Dashboard Helm Chart

Deploys the HoloViz dashboard frontend (`dashboard/`) to Kubernetes. Chart path: `dashboard/charts/holoviz`.

Every resource is named from the release (`<release>-holoviz`, or just `<release>` when the release name
already contains `holoviz`) and carries the standard `app.kubernetes.io/*` and `helm.sh/chart` labels, so
several releases can share one namespace.

## Prerequisites

- Helm 3.x and `kubectl` with access to the target namespace.
- A Secret with the S3 credentials, created once per namespace and never through CI:

```bash
NAMESPACE=zarr-fuse-dashboard-development
kubectl create secret generic zarr-fuse-s3 --namespace "$NAMESPACE" \
  --from-literal=ZF_S3_ACCESS_KEY=<S3_ACCESS_KEY> \
  --from-literal=ZF_S3_SECRET_KEY=<S3_SECRET_KEY> \
  --from-literal=ZF_S3_ENDPOINT_URL=https://s3.cl4.du.cesnet.cz
```

## Deploy

1. Stage the view configuration into the chart. Files under `config/` (git-ignored) are rendered into a
   release-scoped ConfigMap and mounted at `/opt/dashboard/config`; `ZF_VIEW_PATH` and `SCHEMAS_PATH`
   point there. Stage only `zf_view.yaml` and the schema files it references.

```bash
CHART=dashboard/charts/holoviz
mkdir -p "$CHART/config"
cp app/databuk/config/zf_view.yaml "$CHART/config/"
cp app/databuk/config/schemas/*.yaml "$CHART/config/"
```

2. Install or upgrade the release:

```bash
NAMESPACE=zarr-fuse-dashboard-development
RELEASE=holoviz-development

helm upgrade "$RELEASE" "$CHART" \
  --install --atomic --timeout 10m --namespace "$NAMESPACE" \
  --set frontend.image.tag=<IMAGE_TAG> \
  --set frontend.s3.existingSecret=zarr-fuse-s3 \
  --set frontend.config.enabled=true
```

The dashboard is served at `https://zarr-fuse-<release>.dyn.cloud.e-infra.cz` unless `ingress.host` is set.

## Values

| Key | Default | Purpose |
|-----|---------|---------|
| `nameOverride`, `fullnameOverride` | `""` | Override the chart name or the full resource name |
| `frontend.image.name`, `frontend.image.tag` | `jbrezmorf/...`, `latest` | Frontend image |
| `frontend.viewName` | `bukov_endpoint` | View from `zf_view.yaml` to serve (`HV_DASHBOARD_VIEW`) |
| `frontend.s3.existingSecret` | `""` | Existing Secret with the `ZF_S3_*` keys (recommended) |
| `frontend.s3.endpointUrl`, `frontend.s3.secrets.*` | | Fallback: the chart renders the Secret itself |
| `frontend.config.enabled` | `false` | Render and mount the ConfigMap from `config/` |
| `frontend.config.mountPath` | `/opt/dashboard/config` | Mount path, also `SCHEMAS_PATH` |
| `frontend.extraEnv`, `frontend.extraVolumes`, `frontend.extraVolumeMounts` | `[]` | Extra pod settings |
| `frontend.resources`, `frontend.securityContext` | | Resources and security contexts |
| `ingress.enabled`, `ingress.className`, `ingress.annotations` | `true`, `nginx` | Ingress settings |
| `ingress.host` | `""` | Public host; empty means `zarr-fuse-<release>.dyn.cloud.e-infra.cz` |
| `ingress.tls.enabled`, `ingress.tls.secretName` | `true`, `""` | TLS; an empty name is derived from the host |

`values/minimal-required-values.yaml` holds placeholder credentials for `helm lint` and `helm template`.

## Several releases in one namespace

Release names give distinct resources, hosts and ConfigMaps; the S3 Secret can be shared:

```bash
helm upgrade holoviz-bukov "$CHART" --install --namespace "$NAMESPACE" \
  --set frontend.viewName=bukov_endpoint --set frontend.s3.existingSecret=zarr-fuse-s3 \
  --set frontend.config.enabled=true
helm upgrade holoviz-other "$CHART" --install --namespace "$NAMESPACE" \
  --set frontend.viewName=other_view --set frontend.s3.existingSecret=zarr-fuse-s3 \
  --set frontend.config.enabled=true
```

## Upgrading from chart 0.2.x

Resource names changed from the fixed `holoviz-frontend` / `holoviz-ingress` to release-scoped names, so
the first `helm upgrade` replaces the Deployment, Service and Ingress (short downtime). The ingress host
and the TLS secret name are unchanged. The ConfigMap the old workflow created with `kubectl`
(`dashboard-config` or `holoviz-dashboard-config`) is no longer used and can be deleted.

## Troubleshooting

- "another operation is in progress": a previous Helm operation is stuck. Roll back or uninstall:

```bash
helm -n "$NAMESPACE" history "$RELEASE"
helm -n "$NAMESPACE" rollback "$RELEASE" <REVISION>
# or
helm -n "$NAMESPACE" uninstall "$RELEASE"
```

- Pods stuck in `CreateContainerConfigError`: the Secret named by `frontend.s3.existingSecret` is missing
  or lacks one of the `ZF_S3_*` keys.
- Quota errors: reduce `frontend.resources` or ask the cluster admin to raise the namespace quota.

## GitHub Actions

- `.github/workflows/holoviz-dashboard-pull-request.yaml` deploys every PR touching `dashboard/**` or
  `app/databuk/config/**` to the `holoviz-development` release.
- `.github/workflows/dashboard-push-main.yaml` deploys the `dashboard-main` branch to the `dashboard`
  release.

Both call `dashboard-reusable-workflow.yaml` (same layout as `ingress-server-reusable-workflow.yaml`):
it builds the image, hands `zf_view.yaml` and the schemas over as an artifact that lands under `config/`
in the chart, lints the chart and runs the same `helm upgrade` as above with the Secret named by
`s3-secret-name` (default `zarr-fuse-s3`). If that Secret is missing the pods never become ready and
`--atomic` rolls the release back.
