# ingressService

![Version: 2.0.0](https://img.shields.io/badge/Version-2.0.0-informational?style=flat-square)

Ingress Service component of the Zarr Fuse Ingress Server

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| component | string | `""` |  |
| configuration.fastapi.secrets.usersJson | string | `"{}"` |  |
| extraAnnotations | object | `{}` |  |
| extraEnv | list | `[]` |  |
| extraLabels | object | `{}` |  |
| global.runId | int | `0` |  |
| global.s3.endpointUrl | string | `""` |  |
| global.s3.secrets.accessKey | string | `""` |  |
| global.s3.secrets.secretKey | string | `""` |  |
| global.s3.storeUrl | string | `""` |  |
| image.host | string | `""` |  |
| image.name | string | `""` |  |
| image.tag | string | `""` |  |
| imagePullPolicy | string | `"Always"` |  |
| ingress.annotations | object | `{}` |  |
| ingress.className | string | `""` |  |
| ingress.hosts | string | `nil` |  |
| ingress.labels | object | `{}` |  |
| name | string | `""` |  |
| partOf | string | `""` |  |
| replicaCount | int | `1` |  |
| resources.limits | object | `{}` |  |
| resources.requests | object | `{}` |  |
| restartPolicy | string | `"Always"` |  |
| securityContext.container | object | `{}` |  |
| securityContext.pod | object | `{}` |  |
| service.annotations | object | `{}` |  |
| service.externalPort.name | string | `"http"` |  |
| service.externalPort.number | int | `80` |  |
| service.internalPort.name | string | `"http"` |  |
| service.internalPort.number | int | `8000` |  |
| service.labels | object | `{}` |  |
| service.name | string | `""` |  |
| service.type | string | `"ClusterIP"` |  |
