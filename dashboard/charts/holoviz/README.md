# HoloViz Dashboard Helm Chart

Deploys the HoloViz dashboard frontend (`dashboard/`) to Kubernetes.

Resources are named after the release (`<release>-holoviz`, or just `<release>` when the release name contains
`holoviz`) and carry the standard `app.kubernetes.io/*` labels, so several releases can share a namespace.

## Prerequisites

- Helm 3 and `kubectl` access to the target namespace.
- S3 credentials. The chart stores them in its own Secret (`<name>-s3`).

## Deploy

Copy `zf_view.yaml` and the schemas into the chart's `config/` directory (git-ignored); they are mounted at
`/opt/dashboard/config`.

```bash
CHART=dashboard/charts/holoviz
VIEW_CONFIG=dashboard/test_deployment/databuk/config
mkdir -p "$CHART/config"
cp "$VIEW_CONFIG/zf_view.yaml" "$VIEW_CONFIG"/schemas/*.yaml "$CHART/config/"

helm upgrade "$RELEASE" "$CHART" \
  --install --atomic --timeout 10m --namespace "$NAMESPACE" \
  --set frontend.image.tag=<IMAGE_TAG> \
  --set frontend.s3.secrets.accessKey=<S3_ACCESS_KEY> \
  --set frontend.s3.secrets.secretKey=<S3_SECRET_KEY> \
  --set frontend.config.enabled=true
```

The dashboard is served at `https://zarr-fuse-<release>.dyn.cloud.e-infra.cz`.

## Values

| Key | Default | Purpose |
|-----|---------|---------|
| `frontend.image.name` | `jbrezmorf/zarr-fuse-holoviz-frontend` | Frontend image |
| `frontend.image.tag` | `latest` | Image tag, set by CI |
| `frontend.viewName` | `bukov_endpoint` | View from `zf_view.yaml` to serve |
| `frontend.s3.endpointUrl` | `https://s3.cl2.du.cesnet.cz` | S3 endpoint |
| `frontend.s3.secrets.accessKey`, `frontend.s3.secrets.secretKey` | `""` | S3 credentials for the chart's Secret |
| `frontend.s3.existingSecret` | `""` | Optional Secret with the `ZF_S3_*` keys, used instead of the chart's Secret |
| `frontend.config.enabled` | `false` | Mount the files from `config/` |
| `frontend.extraEnv`, `frontend.extraVolumes`, `frontend.extraVolumeMounts` | `[]` | Extra pod settings |
| `ingress.className`, `ingress.annotations` | `nginx`, cert-manager | Ingress settings |
| `nameOverride`, `fullnameOverride` | `""` | Override the resource names |

`values/minimal-required-values.yaml` holds placeholder credentials for `helm lint` and `helm template`.

## Upgrading from chart 0.2.x

Chart 0.2.x used fixed names (`holoviz-frontend`, `holoviz-ingress`, `holoviz-frontend-secrets`). The new Ingress
claims the same host, and the ingress-nginx admission webhook rejects a second Ingress for a host, so delete the old one
right before the first upgrade. The GitHub workflow does this step itself.

```bash
kubectl -n "$NAMESPACE" delete ingress holoviz-ingress
```

The old Deployment, Service and Secret are removed by the upgrade itself. A failed first upgrade cannot be rolled back
(`no Secret with the name "holoviz-frontend-secrets" found`); fix the cause and upgrade again. The ConfigMap the old
workflow created with `kubectl` (`dashboard-config` or `holoviz-dashboard-config`) is no longer used and can be
deleted.

## Troubleshooting

- "another operation is in progress": a previous Helm operation is stuck. Roll back or uninstall:

```bash
helm -n "$NAMESPACE" history "$RELEASE"
helm -n "$NAMESPACE" rollback "$RELEASE" <REVISION>
```

- `frontend.s3.secrets.accessKey is required`: the S3 credentials were not passed. In GitHub Actions, pass the
  `S3_ACCESS_KEY` and `S3_SECRET_KEY` secrets.
- Pods stuck in `CreateContainerConfigError`: the Secret named by `frontend.s3.existingSecret` is missing or lacks a
  `ZF_S3_*` key.
- Quota errors: reduce `frontend.resources` or ask the cluster admin to raise the namespace quota.

## GitHub Actions

`.github/workflows/dashboard-reusable-workflow.yaml` builds the image, copies the view configuration from the caller
repository into `config/` and runs the `helm upgrade` above. It is called by `holoviz-dashboard-pull-request.yaml`
(release `holoviz-development`), `dashboard-push-main.yaml` (release `dashboard`) and other repositories:

```yaml
jobs:
  dashboard:
    uses: GeoMop/zarr_fuse/.github/workflows/dashboard-reusable-workflow.yaml@<ref>
    with:
      deploy: true
      namespace: <namespace>
      release-name: <release>
      views-path: <path to zf_view.yaml>
      schemas-path: <schemas directory>
      zarr-fuse-ref: <ref>
    secrets:
      DOCKER_PASSWORD: ${{ secrets.DOCKER_PASSWORD }}
      KUBECONFIG: ${{ secrets.KUBECONFIG }}
      S3_ACCESS_KEY: ${{ secrets.S3_ACCESS_KEY }}
      S3_SECRET_KEY: ${{ secrets.S3_SECRET_KEY }}
```

The caller repository also needs the variable `DOCKER_USERNAME` for pushing the image. `KUBECONFIG` is base64-encoded.
