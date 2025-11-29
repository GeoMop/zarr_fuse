# extractorService

![Version: 2.0.0](https://img.shields.io/badge/Version-2.0.0-informational?style=flat-square)

Extractor Service component of the Zarr Fuse Ingress Server

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| airflow.executor | string | `""` |  |
| airflow.image.host | string | `""` |  |
| airflow.image.name | string | `""` |  |
| airflow.image.tag | string | `""` |  |
| airflow.imagePullPolicy | string | `"IfNotPresent"` |  |
| airflow.scheduler.component | string | `""` |  |
| airflow.scheduler.extraAnnotations | object | `{}` |  |
| airflow.scheduler.extraEnv | list | `[]` |  |
| airflow.scheduler.extraLabels | object | `{}` |  |
| airflow.scheduler.imagePullPolicy | string | `"IfNotPresent"` |  |
| airflow.scheduler.name | string | `""` |  |
| airflow.scheduler.partOf | string | `""` |  |
| airflow.scheduler.replicaCount | int | `1` |  |
| airflow.scheduler.resources.limits | object | `{}` |  |
| airflow.scheduler.resources.requests | object | `{}` |  |
| airflow.scheduler.restartPolicy | string | `"Always"` |  |
| airflow.scheduler.securityContext.container | object | `{}` |  |
| airflow.scheduler.securityContext.pod | object | `{}` |  |
| airflow.secrets.fernetKey | string | `""` |  |
| airflow.webServer.component | string | `""` |  |
| airflow.webServer.extraAnnotations | object | `{}` |  |
| airflow.webServer.extraEnv | list | `[]` |  |
| airflow.webServer.extraLabels | object | `{}` |  |
| airflow.webServer.imagePullPolicy | string | `"IfNotPresent"` |  |
| airflow.webServer.name | string | `""` |  |
| airflow.webServer.partOf | string | `""` |  |
| airflow.webServer.replicaCount | int | `1` |  |
| airflow.webServer.resources.limits | object | `{}` |  |
| airflow.webServer.resources.requests | object | `{}` |  |
| airflow.webServer.restartPolicy | string | `"Always"` |  |
| airflow.webServer.secrets.webServerSecretKey | string | `""` |  |
| airflow.webServer.securityContext.container | object | `{}` |  |
| airflow.webServer.securityContext.pod | object | `{}` |  |
| airflow.webServer.service.annotations | object | `{}` |  |
| airflow.webServer.service.externalPort.name | string | `"http"` |  |
| airflow.webServer.service.externalPort.number | int | `8080` |  |
| airflow.webServer.service.internalPort.name | string | `"http"` |  |
| airflow.webServer.service.internalPort.number | int | `80` |  |
| airflow.webServer.service.labels | object | `{}` |  |
| airflow.webServer.service.name | string | `""` |  |
| airflow.webServer.service.type | string | `"ClusterIP"` |  |
| global.runId | int | `0` |  |
| global.s3.endpointUrl | string | `""` |  |
| global.s3.secrets.accessKey | string | `""` |  |
| global.s3.secrets.secretKey | string | `""` |  |
| global.s3.storeUrl | string | `""` |  |
| postgres.component | string | `""` |  |
| postgres.extraAnnotations | object | `{}` |  |
| postgres.extraEnv | list | `[]` |  |
| postgres.extraLabels | object | `{}` |  |
| postgres.image.host | string | `""` |  |
| postgres.image.name | string | `""` |  |
| postgres.image.tag | int | `16` |  |
| postgres.imagePullPolicy | string | `"IfNotPresent"` |  |
| postgres.name | string | `""` |  |
| postgres.partOf | string | `""` |  |
| postgres.persistence.accessModes[0] | string | `"ReadWriteOnce"` |  |
| postgres.persistence.name | string | `""` |  |
| postgres.persistence.size | string | `"10Gi"` |  |
| postgres.persistence.storageClass | string | `""` |  |
| postgres.persistence.volumeMode | string | `"Filesystem"` |  |
| postgres.replicaCount | int | `1` |  |
| postgres.resources.limits | object | `{}` |  |
| postgres.resources.requests | object | `{}` |  |
| postgres.restartPolicy | string | `"Always"` |  |
| postgres.secrets.database | string | `""` |  |
| postgres.secrets.password | string | `""` |  |
| postgres.secrets.user | string | `""` |  |
| postgres.securityContext.container | object | `{}` |  |
| postgres.securityContext.pod | object | `{}` |  |
| postgres.service.annotations | object | `{}` |  |
| postgres.service.externalPort.name | string | `"http"` |  |
| postgres.service.externalPort.number | int | `5432` |  |
| postgres.service.internalPort.name | string | `"http"` |  |
| postgres.service.internalPort.number | int | `5432` |  |
| postgres.service.labels | object | `{}` |  |
| postgres.service.name | string | `""` |  |
| postgres.service.type | string | `"ClusterIP"` |  |
