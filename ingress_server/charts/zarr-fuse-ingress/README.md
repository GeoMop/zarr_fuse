# zarr-fuse-ingress

![Version: 2.0.0](https://img.shields.io/badge/Version-2.0.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 2.0.0](https://img.shields.io/badge/AppVersion-2.0.0-informational?style=flat-square)

A Helm chart for ingress service and Apache Airflow extractor service

**Homepage:** <https://github.com/geomop/zarr_fuse>

## Maintainers

| Name | Email | Url |
| ---- | ------ | --- |
| Stepan Moc | <stepan.mocik@gmail.com> |  |

## Source Code

* <https://github.com/geomop/zarr_fuse>

## Requirements

| Repository | Name | Version |
|------------|------|---------|
| https://airflow.apache.org | airflow | 1.18.0 |

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| apache-airflow.enabled | bool | `true` |  |
| ingressService.component | string | `"ingress-service"` |  |
| ingressService.configuration.fastapi.security.usersJson | string | `""` |  |
| ingressService.configuration.s3.endpointUrl | string | `""` |  |
| ingressService.configuration.s3.secrets.accessKey | string | `""` |  |
| ingressService.configuration.s3.secrets.secretKey | string | `""` |  |
| ingressService.configuration.s3.storeUrl | string | `""` |  |
| ingressService.extraEnv[0].name | string | `"LOG_LEVEL"` |  |
| ingressService.extraEnv[0].value | string | `"DEBUG"` |  |
| ingressService.extraEnv[1].name | string | `"PORT"` |  |
| ingressService.extraEnv[1].value | int | `8000` |  |
| ingressService.image.name | string | `"jbrezmorf/ingress-service"` |  |
| ingressService.image.tag | string | `"latest"` |  |
| ingressService.imagePullPolicy | string | `"Always"` |  |
| ingressService.ingress.annotations."cert-manager.io/cluster-issuer" | string | `"letsencrypt-prod"` |  |
| ingressService.ingress.annotations."kubernetes.io/tls-acme" | string | `"true"` |  |
| ingressService.ingress.className | string | `"nginx"` |  |
| ingressService.ingress.labels | object | `{}` |  |
| ingressService.name | string | `"ingress-service"` |  |
| ingressService.partOf | string | `""` |  |
| ingressService.replicaCount | int | `1` |  |
| ingressService.resources.limits.cpu | string | `"250m"` |  |
| ingressService.resources.limits.memory | string | `"256Mi"` |  |
| ingressService.resources.requests.cpu | string | `"125m"` |  |
| ingressService.resources.requests.memory | string | `"128Mi"` |  |
| ingressService.restartPolicy | string | `"Always"` |  |
| ingressService.securityContext.container.allowPrivilegeEscalation | bool | `false` |  |
| ingressService.securityContext.container.capabilities.drop[0] | string | `"ALL"` |  |
| ingressService.securityContext.container.privileged | bool | `false` |  |
| ingressService.securityContext.container.readOnlyRootFilesystem | bool | `true` |  |
| ingressService.securityContext.container.runAsGroup | int | `1000` |  |
| ingressService.securityContext.container.runAsNonRoot | bool | `true` |  |
| ingressService.securityContext.container.runAsUser | int | `1000` |  |
| ingressService.securityContext.pod.fsGroup | int | `1000` |  |
| ingressService.securityContext.pod.runAsGroup | int | `1000` |  |
| ingressService.securityContext.pod.runAsNonRoot | bool | `true` |  |
| ingressService.securityContext.pod.runAsUser | int | `1000` |  |
| ingressService.securityContext.pod.seccompProfile.type | string | `"RuntimeDefault"` |  |
| ingressService.service.annotations | object | `{}` |  |
| ingressService.service.externalPort.name | string | `"http"` |  |
| ingressService.service.externalPort.number | int | `80` |  |
| ingressService.service.internalPort.name | string | `"http"` |  |
| ingressService.service.internalPort.number | int | `8000` |  |
| ingressService.service.labels | object | `{}` |  |
| ingressService.service.name | string | `"ingress_service"` |  |
| ingressService.service.type | string | `"ClusterIP"` |  |
| registry.host | string | `"docker.io"` |  |
| runId | int | `0` |  |
