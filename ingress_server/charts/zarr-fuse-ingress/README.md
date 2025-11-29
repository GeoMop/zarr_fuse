# zarr-fuse-ingress

![Version: 2.0.0](https://img.shields.io/badge/Version-2.0.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 2.0.0](https://img.shields.io/badge/AppVersion-2.0.0-informational?style=flat-square)

A Helm chart for ingress service and Apache Airflow extractor service

**Homepage:** <https://github.com/geomop/zarr_fuse>

## Maintainers

| Name | Email | Url |
| ---- | ------ | --- |
| Stepan Moc | <stepan.mocik@gmail.com> |  |
| Jan Brezmor | <jan.brezina@tul.cz> |  |

## Source Code

* <https://github.com/geomop/zarr_fuse>

## Requirements

| Repository | Name | Version |
|------------|------|---------|
| file://charts/extractor-service | extractor-service | 2.0.0 |
| file://charts/ingress-service | ingress-service | 2.0.0 |

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| extractor-service.airflow.executor | string | `"LocalExecutor"` |  |
| extractor-service.airflow.image.host | string | `"docker.io"` |  |
| extractor-service.airflow.image.name | string | `"jbrezmorf/zarr-fuse-extractor-service"` |  |
| extractor-service.airflow.image.tag | string | `"ci-7aab59a"` |  |
| extractor-service.airflow.imagePullPolicy | string | `"IfNotPresent"` |  |
| extractor-service.airflow.initJob.imagePullPolicy | string | `"IfNotPresent"` |  |
| extractor-service.airflow.initJob.name | string | `"airflow-init-job"` |  |
| extractor-service.airflow.initJob.restartPolicy | string | `"OnFailure"` |  |
| extractor-service.airflow.initJob.securityContext.container.allowPrivilegeEscalation | bool | `false` |  |
| extractor-service.airflow.initJob.securityContext.container.capabilities.drop[0] | string | `"ALL"` |  |
| extractor-service.airflow.initJob.securityContext.container.privileged | bool | `false` |  |
| extractor-service.airflow.initJob.securityContext.container.runAsGroup | int | `50000` |  |
| extractor-service.airflow.initJob.securityContext.container.runAsNonRoot | bool | `true` |  |
| extractor-service.airflow.initJob.securityContext.container.runAsUser | int | `50000` |  |
| extractor-service.airflow.initJob.securityContext.pod.fsGroup | int | `50000` |  |
| extractor-service.airflow.initJob.securityContext.pod.runAsGroup | int | `50000` |  |
| extractor-service.airflow.initJob.securityContext.pod.runAsNonRoot | bool | `true` |  |
| extractor-service.airflow.initJob.securityContext.pod.runAsUser | int | `50000` |  |
| extractor-service.airflow.initJob.securityContext.pod.seccompProfile.type | string | `"RuntimeDefault"` |  |
| extractor-service.airflow.scheduler.component | string | `"scheduler"` |  |
| extractor-service.airflow.scheduler.extraAnnotations | object | `{}` |  |
| extractor-service.airflow.scheduler.extraEnv | list | `[]` |  |
| extractor-service.airflow.scheduler.extraLabels | object | `{}` |  |
| extractor-service.airflow.scheduler.imagePullPolicy | string | `"IfNotPresent"` |  |
| extractor-service.airflow.scheduler.name | string | `"airflow-scheduler"` |  |
| extractor-service.airflow.scheduler.partOf | string | `"extractor-service"` |  |
| extractor-service.airflow.scheduler.replicaCount | int | `1` |  |
| extractor-service.airflow.scheduler.resources.limits.cpu | string | `"500m"` |  |
| extractor-service.airflow.scheduler.resources.limits.memory | string | `"1024Mi"` |  |
| extractor-service.airflow.scheduler.resources.requests.cpu | string | `"100m"` |  |
| extractor-service.airflow.scheduler.resources.requests.memory | string | `"256Mi"` |  |
| extractor-service.airflow.scheduler.restartPolicy | string | `"Always"` |  |
| extractor-service.airflow.scheduler.securityContext.container.allowPrivilegeEscalation | bool | `false` |  |
| extractor-service.airflow.scheduler.securityContext.container.capabilities.drop[0] | string | `"ALL"` |  |
| extractor-service.airflow.scheduler.securityContext.container.privileged | bool | `false` |  |
| extractor-service.airflow.scheduler.securityContext.container.runAsGroup | int | `50000` |  |
| extractor-service.airflow.scheduler.securityContext.container.runAsNonRoot | bool | `true` |  |
| extractor-service.airflow.scheduler.securityContext.container.runAsUser | int | `50000` |  |
| extractor-service.airflow.scheduler.securityContext.pod.fsGroup | int | `50000` |  |
| extractor-service.airflow.scheduler.securityContext.pod.runAsGroup | int | `50000` |  |
| extractor-service.airflow.scheduler.securityContext.pod.runAsNonRoot | bool | `true` |  |
| extractor-service.airflow.scheduler.securityContext.pod.runAsUser | int | `50000` |  |
| extractor-service.airflow.scheduler.securityContext.pod.seccompProfile.type | string | `"RuntimeDefault"` |  |
| extractor-service.airflow.secrets.fernetKey | string | `""` |  |
| extractor-service.airflow.webServer.component | string | `"scheduler"` |  |
| extractor-service.airflow.webServer.extraAnnotations | object | `{}` |  |
| extractor-service.airflow.webServer.extraEnv | list | `[]` |  |
| extractor-service.airflow.webServer.extraLabels | object | `{}` |  |
| extractor-service.airflow.webServer.imagePullPolicy | string | `"IfNotPresent"` |  |
| extractor-service.airflow.webServer.name | string | `"airflow-webserver"` |  |
| extractor-service.airflow.webServer.partOf | string | `"extractor-service"` |  |
| extractor-service.airflow.webServer.replicaCount | int | `1` |  |
| extractor-service.airflow.webServer.resources.limits.cpu | string | `"500m"` |  |
| extractor-service.airflow.webServer.resources.limits.memory | string | `"1024Mi"` |  |
| extractor-service.airflow.webServer.resources.requests.cpu | string | `"100m"` |  |
| extractor-service.airflow.webServer.resources.requests.memory | string | `"256Mi"` |  |
| extractor-service.airflow.webServer.restartPolicy | string | `"Always"` |  |
| extractor-service.airflow.webServer.secrets.webServerSecretKey | string | `""` |  |
| extractor-service.airflow.webServer.securityContext.container.allowPrivilegeEscalation | bool | `false` |  |
| extractor-service.airflow.webServer.securityContext.container.capabilities.drop[0] | string | `"ALL"` |  |
| extractor-service.airflow.webServer.securityContext.container.privileged | bool | `false` |  |
| extractor-service.airflow.webServer.securityContext.container.runAsGroup | int | `50000` |  |
| extractor-service.airflow.webServer.securityContext.container.runAsNonRoot | bool | `true` |  |
| extractor-service.airflow.webServer.securityContext.container.runAsUser | int | `50000` |  |
| extractor-service.airflow.webServer.securityContext.pod.fsGroup | int | `50000` |  |
| extractor-service.airflow.webServer.securityContext.pod.runAsGroup | int | `50000` |  |
| extractor-service.airflow.webServer.securityContext.pod.runAsNonRoot | bool | `true` |  |
| extractor-service.airflow.webServer.securityContext.pod.runAsUser | int | `50000` |  |
| extractor-service.airflow.webServer.securityContext.pod.seccompProfile.type | string | `"RuntimeDefault"` |  |
| extractor-service.airflow.webServer.service | object | ~ | Service configuration |
| extractor-service.airflow.webServer.service.annotations | dict | `{}` | Service annotations |
| extractor-service.airflow.webServer.service.externalPort | object | ~ | External port configuration |
| extractor-service.airflow.webServer.service.externalPort.name | string | `"http"` | Port name |
| extractor-service.airflow.webServer.service.externalPort.number | int | `8080` | Port number |
| extractor-service.airflow.webServer.service.internalPort | object | ~ | Internal port configuration |
| extractor-service.airflow.webServer.service.internalPort.name | string | `"http"` | Port name |
| extractor-service.airflow.webServer.service.internalPort.number | int | `80` | Port number |
| extractor-service.airflow.webServer.service.labels | dict | `{}` | Service labels |
| extractor-service.airflow.webServer.service.name | string | `"airflow-webserver-service"` | Service name |
| extractor-service.airflow.webServer.service.type | string | `"ClusterIP"` | Service type |
| extractor-service.postgres.component | string | `"postgresql"` |  |
| extractor-service.postgres.extraAnnotations | object | `{}` |  |
| extractor-service.postgres.extraEnv | list | `[]` |  |
| extractor-service.postgres.extraLabels | object | `{}` |  |
| extractor-service.postgres.image.host | string | `"docker.io"` |  |
| extractor-service.postgres.image.name | string | `"postgres"` |  |
| extractor-service.postgres.image.tag | int | `16` |  |
| extractor-service.postgres.imagePullPolicy | string | `"IfNotPresent"` |  |
| extractor-service.postgres.name | string | `"postgres"` |  |
| extractor-service.postgres.partOf | string | `"extractor-service"` |  |
| extractor-service.postgres.persistence.accessModes[0] | string | `"ReadWriteOnce"` |  |
| extractor-service.postgres.persistence.name | string | `"postgres-data"` |  |
| extractor-service.postgres.persistence.size | string | `"10Gi"` |  |
| extractor-service.postgres.persistence.storageClass | string | `"csi-ceph-rbd-du"` |  |
| extractor-service.postgres.persistence.volumeMode | string | `"Filesystem"` |  |
| extractor-service.postgres.replicaCount | int | `1` |  |
| extractor-service.postgres.resources.limits.cpu | string | `"500m"` |  |
| extractor-service.postgres.resources.limits.memory | string | `"500Mi"` |  |
| extractor-service.postgres.resources.requests.cpu | string | `"100m"` |  |
| extractor-service.postgres.resources.requests.memory | string | `"100Mi"` |  |
| extractor-service.postgres.restartPolicy | string | `"Always"` |  |
| extractor-service.postgres.secrets.database | string | `""` |  |
| extractor-service.postgres.secrets.password | string | `""` |  |
| extractor-service.postgres.secrets.user | string | `""` |  |
| extractor-service.postgres.securityContext.container.allowPrivilegeEscalation | bool | `false` |  |
| extractor-service.postgres.securityContext.container.capabilities.drop[0] | string | `"ALL"` |  |
| extractor-service.postgres.securityContext.container.privileged | bool | `false` |  |
| extractor-service.postgres.securityContext.container.runAsGroup | int | `999` |  |
| extractor-service.postgres.securityContext.container.runAsNonRoot | bool | `true` |  |
| extractor-service.postgres.securityContext.container.runAsUser | int | `999` |  |
| extractor-service.postgres.securityContext.pod.fsGroup | int | `999` |  |
| extractor-service.postgres.securityContext.pod.runAsGroup | int | `999` |  |
| extractor-service.postgres.securityContext.pod.runAsNonRoot | bool | `true` |  |
| extractor-service.postgres.securityContext.pod.runAsUser | int | `999` |  |
| extractor-service.postgres.securityContext.pod.seccompProfile.type | string | `"RuntimeDefault"` |  |
| extractor-service.postgres.service | object | ~ | Service configuration |
| extractor-service.postgres.service.annotations | dict | `{}` | Service annotations |
| extractor-service.postgres.service.externalPort | object | ~ | External port configuration |
| extractor-service.postgres.service.externalPort.name | string | `"http"` | Port name |
| extractor-service.postgres.service.externalPort.number | int | `5432` | Port number |
| extractor-service.postgres.service.internalPort | object | ~ | Internal port configuration |
| extractor-service.postgres.service.internalPort.name | string | `"http"` | Port name |
| extractor-service.postgres.service.internalPort.number | int | `5432` | Port number |
| extractor-service.postgres.service.labels | dict | `{}` | Service labels |
| extractor-service.postgres.service.name | string | `"postgres-service"` | Service name |
| extractor-service.postgres.service.type | string | `"ClusterIP"` | Service type |
| global.runId | int | `0` | Unique run identifier |
| global.s3.endpointUrl | string | `"endpointUrl"` | S3 endpoint URL |
| global.s3.secrets | object | ~ | S3 secrets |
| global.s3.secrets.accessKey | string | `"accessKey"` | S3 access key |
| global.s3.secrets.secretKey | string | `"secretKey"` | S3 secret key |
| global.s3.storeUrl | string | `"storeUrl"` | S3 store URL |
| ingress | object | ~ | Ingress configuration |
| ingress-service | object | ~ | Ingress Service configuration |
| ingress-service.component | string | .Chart.Name | Component name for label 'app.kubernetes.io/component' |
| ingress-service.configuration | object | ~ | Service configuration |
| ingress-service.configuration.fastapi | object | ~ | FastAPI configuration |
| ingress-service.configuration.fastapi.secrets | object | ~ | FastAPI secrets |
| ingress-service.configuration.fastapi.secrets.usersJson | string | "{}" | Must be provided as a JSON string. |
| ingress-service.extraAnnotations | dict | `{}` | Extra annotations to add to all resources |
| ingress-service.extraEnv | list | `[]` | Each item is an object with {name, value}. |
| ingress-service.extraLabels | dict | `{}` | Extra labels to add to all resources |
| ingress-service.image | object | ~ | Image details |
| ingress-service.image.host | string | `"docker.io"` | Hostname of the container registry |
| ingress-service.image.name | string | `"jbrezmorf/zarr-fuse-ingress-service"` | Image repository |
| ingress-service.image.tag | string | `"ci-7aab59a"` | Image tag |
| ingress-service.imagePullPolicy | string | `"Always"` | Image pull policy |
| ingress-service.name | string | `"ingress-service"` | Application name |
| ingress-service.partOf | string | .Chart.Name | Part of label 'app.kubernetes.io/part-of' |
| ingress-service.replicaCount | int | `1` | Number of desired pods |
| ingress-service.resources | object | ~ | Resource requests and limits |
| ingress-service.resources.limits | object | {} | Resource limits for the container |
| ingress-service.resources.requests | object | {} | Resource requests for the container |
| ingress-service.restartPolicy | string | `"Always"` | Restart policy for the pod |
| ingress-service.securityContext | object | ~ | Security context settings |
| ingress-service.service | object | ~ | Service configuration |
| ingress-service.service.annotations | dict | `{}` | Service annotations |
| ingress-service.service.externalPort | object | ~ | External port configuration |
| ingress-service.service.externalPort.name | string | `"http"` | Port name |
| ingress-service.service.externalPort.number | int | `80` | Port number |
| ingress-service.service.internalPort | object | ~ | Internal port configuration |
| ingress-service.service.internalPort.name | string | `"http"` | Port name |
| ingress-service.service.internalPort.number | int | `8000` | Port number |
| ingress-service.service.labels | dict | `{}` | Service labels |
| ingress-service.service.name | string | `"ingress-service"` | Service name |
| ingress-service.service.type | string | `"ClusterIP"` | Service type |
| ingress.annotations | dict | `{}` | Ingress annotations |
| ingress.className | string | `"nginx"` | Ingress class name |
| ingress.hosts | object | ~ | Hosts configuration |
| ingress.labels | dict | {} | Ingress labels |
