# HoloViz Dashboard Helm Chart

Deploys the HoloViz dashboard frontend (`dashboard/`) to Kubernetes.

Resources are named after the release (`<release>-holoviz`, or just `<release>` when the release name contains
`holoviz`) and carry the standard `app.kubernetes.io/*` labels, so several releases can share a namespace.

## Prerequisites

- Helm 3 and `kubectl` access to the target namespace.
- A Secret with the S3 credentials, created once per namespace:

```bash
kubectl create secret generic zarr-fuse-s3 --namespace "$NAMESPACE" \
  --from-literal=ZF_S3_ACCESS_KEY=<S3_ACCESS_KEY> \
  --from-literal=ZF_S3_SECRET_KEY=<S3_SECRET_KEY> \
  --from-literal=ZF_S3_ENDPOINT_URL=https://s3.cl2.du.cesnet.cz
```

## Deploy

Copy `zf_view.yaml` and the schemas into the chart's `config/` directory (git-ignored); they are mounted at
`/opt/dashboard/config`.

```bash
CHART=dashboard/charts/holoviz
mkdir -p "$CHART/config"
cp app/databuk/config/zf_view.yaml app/databuk/config/schemas/*.yaml "$CHART/config/"

helm upgrade "$RELEASE" "$CHART" \
  --install --atomic --timeout 10m --namespace "$NAMESPACE" \
  --set frontend.image.tag=<IMAGE_TAG> \
  --set frontend.s3.existingSecret=zarr-fuse-s3 \
  --set frontend.config.enabled=true
```

The dashboard is served at `https://zarr-fuse-<release>.dyn.cloud.e-infra.cz`.

## Values

| Key | Default | Purpose |
|-----|---------|---------|
| `frontend.image.name` | `jbrezmorf/zarr-fuse-holoviz-frontend` | Frontend image |
| `frontend.image.tag` | `latest` | Image tag, set by CI |
| `frontend.viewName` | `bukov_endpoint` | View from `zf_view.yaml` to serve |
| `frontend.s3.existingSecret` | `""` | Secret with the `ZF_S3_*` keys |
| `frontend.s3.endpointUrl`, `frontend.s3.secrets.*` | | Used to create the Secret when `existingSecret` is empty |
| `frontend.config.enabled` | `false` | Mount the files from `config/` |
| `frontend.extraEnv`, `frontend.extraVolumes`, `frontend.extraVolumeMounts` | `[]` | Extra pod settings |
| `ingress.className`, `ingress.annotations` | `nginx`, cert-manager | Ingress settings |
| `nameOverride`, `fullnameOverride` | `""` | Override the resource names |

`values/minimal-required-values.yaml` holds placeholder credentials for `helm lint` and `helm template`.

## Upgrading from chart 0.2.x

Chart 0.2.x used fixed names (`holoviz-frontend`, `holoviz-ingress`). The new Ingress claims the same host, and the
ingress-nginx admission webhook rejects a second Ingress for a host, so delete the old one right before the first
upgrade:

```bash
kubectl -n "$NAMESPACE" delete ingress holoviz-ingress
```

The old Deployment, Service and Secret are removed by the upgrade itself. The ConfigMap the old workflow created with
`kubectl` (`dashboard-config` or `holoviz-dashboard-config`) is no longer used and can be deleted.

## Troubleshooting

- "another operation is in progress": a previous Helm operation is stuck. Roll back or uninstall:

```bash
helm -n "$NAMESPACE" history "$RELEASE"
helm -n "$NAMESPACE" rollback "$RELEASE" <REVISION>
```

- Pods stuck in `CreateContainerConfigError`: the Secret named by `frontend.s3.existingSecret` is missing or lacks a
  `ZF_S3_*` key.
- Quota errors: reduce `frontend.resources` or ask the cluster admin to raise the namespace quota.

## GitHub Actions

`.github/workflows/dashboard-reusable-workflow.yaml` builds the image, copies the view configuration into `config/`
and runs the `helm upgrade` above. It is called by `holoviz-dashboard-pull-request.yaml` (release
`holoviz-development`) and `dashboard-push-main.yaml` (release `dashboard`).
