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
| file://charts/extractor-service | extractorService | 2.0.0 |
| file://charts/ingress-service | ingressService | 2.0.0 |

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| extractorService | object | ~ | Extractor Service configuration |
| extractorService.airflow | object | ~ | Airflow configuration |
| extractorService.airflow.executor | string | `"LocalExecutor"` | Airflow executor type |
| extractorService.airflow.image | object | ~ | Image details |
| extractorService.airflow.image.host | string | `"docker.io"` | Hostname of the container registry |
| extractorService.airflow.image.name | string | `"jbrezmorf/zarr-fuse-extractor-service"` | Image repository |
| extractorService.airflow.image.tag | string | `""` | Image tag |
| extractorService.airflow.imagePullPolicy | string | `"IfNotPresent"` | Image pull policy |
| extractorService.airflow.initJob | object | ~ | Init Job configuration |
| extractorService.airflow.initJob.component | string | `"init-job"` | Component name for label 'app.kubernetes.io/component' |
| extractorService.airflow.initJob.extraAnnotations | dict | `{}` | Extra annotations to add to all resources |
| extractorService.airflow.initJob.extraEnv | array | `[]` | Extra environment variables injected into the application container. |
| extractorService.airflow.initJob.extraLabels | array | `{}` | Extra environment variables injected into the application container. |
| extractorService.airflow.initJob.imagePullPolicy | string | `"IfNotPresent"` | Image pull policy |
| extractorService.airflow.initJob.name | string | `"airflow-init-job"` | Name of the init job |
| extractorService.airflow.initJob.partOf | string | `"extractor-service"` | Part of label 'app.kubernetes.io/part-of' |
| extractorService.airflow.initJob.resources | object | ~ | Resource requests and limits |
| extractorService.airflow.initJob.resources.container | object | ~ | Container requests and limits |
| extractorService.airflow.initJob.resources.container.limits | object | {} | Resource limits for the container |
| extractorService.airflow.initJob.resources.container.requests | object | {} | Resource requests for the container |
| extractorService.airflow.initJob.resources.initContainer | object | ~ | Init Container requests and limits |
| extractorService.airflow.initJob.resources.initContainer.limits | object | {} | Resource limits for the container |
| extractorService.airflow.initJob.resources.initContainer.requests | object | {} | Resource requests for the container |
| extractorService.airflow.initJob.restartPolicy | string | `"OnFailure"` | Restart policy for the pod |
| extractorService.airflow.initJob.secrets | object | ~ | Init Job secrets |
| extractorService.airflow.initJob.secrets.adminUser | object | ~ | Admin user credentials |
| extractorService.airflow.initJob.secrets.adminUser.email | string | `"admin@gmail.com"` | Admin email |
| extractorService.airflow.initJob.secrets.adminUser.firstname | string | `"admin"` | Admin first name |
| extractorService.airflow.initJob.secrets.adminUser.lastname | string | `"admin"` | Admin last name |
| extractorService.airflow.initJob.secrets.adminUser.password | string | `"admin"` | Admin password |
| extractorService.airflow.initJob.secrets.adminUser.username | string | `"admin"` | Admin username |
| extractorService.airflow.initJob.securityContext | object | ~ | Security context settings |
| extractorService.airflow.initJob.securityContext.container | object | ~ | Container-level security context |
| extractorService.airflow.initJob.securityContext.initContainer | object | ~ | Init Container-level security context |
| extractorService.airflow.initJob.securityContext.pod | object | ~ | Pod-level security context |
| extractorService.airflow.initJob.ttlSecondsAfterFinished | int | `3600` | TTL seconds after finished |
| extractorService.airflow.scheduler | object | ~ | Scheduler configuration |
| extractorService.airflow.scheduler.component | string | `"scheduler"` | Component name for label 'app.kubernetes.io/component' |
| extractorService.airflow.scheduler.extraAnnotations | dict | `{}` | Extra annotations to add to all resources |
| extractorService.airflow.scheduler.extraEnv | array | `[]` | Extra environment variables injected into the application container. |
| extractorService.airflow.scheduler.extraLabels | array | `{}` | Extra environment variables injected into the application container. |
| extractorService.airflow.scheduler.imagePullPolicy | string | `"IfNotPresent"` | Image pull policy |
| extractorService.airflow.scheduler.name | string | `"airflow-scheduler"` | Name of the scheduler deployment |
| extractorService.airflow.scheduler.partOf | string | `"extractor-service"` | Part of label 'app.kubernetes.io/part-of' |
| extractorService.airflow.scheduler.replicaCount | int | `1` | Number of desired pods |
| extractorService.airflow.scheduler.resources | object | ~ | Resource requests and limits |
| extractorService.airflow.scheduler.resources.limits | object | {} | Resource limits for the container |
| extractorService.airflow.scheduler.resources.requests | object | {} | Resource requests for the container |
| extractorService.airflow.scheduler.restartPolicy | string | `"Always"` | Restart policy for the pod |
| extractorService.airflow.scheduler.securityContext | object | ~ | Security context settings |
| extractorService.airflow.scheduler.securityContext.container | object | ~ | Container-level security context |
| extractorService.airflow.scheduler.securityContext.pod | object | ~ | Pod-level security context |
| extractorService.airflow.secrets | object | ~ | Airflow secrets |
| extractorService.airflow.secrets.fernetKey | string | `""` | Fernet key for encrypting sensitive data |
| extractorService.airflow.webServer | object | ~ | Web Server configuration |
| extractorService.airflow.webServer.component | string | `"scheduler"` | Component name for label 'app.kubernetes.io/component' |
| extractorService.airflow.webServer.extraAnnotations | dict | `{}` | Extra annotations to add to all resources |
| extractorService.airflow.webServer.extraEnv | array | `[]` | Extra environment variables injected into the application container. |
| extractorService.airflow.webServer.extraLabels | array | `{}` | Extra environment variables injected into the application container. |
| extractorService.airflow.webServer.imagePullPolicy | string | `"IfNotPresent"` | Image pull policy |
| extractorService.airflow.webServer.name | string | `"airflow-webserver"` | Name of the web server deployment |
| extractorService.airflow.webServer.partOf | string | `"extractor-service"` | Part of label 'app.kubernetes.io/part-of' |
| extractorService.airflow.webServer.replicaCount | int | `1` | Number of desired pods |
| extractorService.airflow.webServer.resources | object | ~ | Resource requests and limits |
| extractorService.airflow.webServer.resources.limits | object | {} | Resource limits for the container |
| extractorService.airflow.webServer.resources.requests | object | {} | Resource requests for the container |
| extractorService.airflow.webServer.restartPolicy | string | `"Always"` | Restart policy for the pod |
| extractorService.airflow.webServer.secrets | object | ~ | Web Server secrets |
| extractorService.airflow.webServer.secrets.webServerSecretKey | string | `""` | Secret key for the web server |
| extractorService.airflow.webServer.securityContext | object | ~ | Security context settings |
| extractorService.airflow.webServer.securityContext.container | object | ~ | Container-level security context |
| extractorService.airflow.webServer.securityContext.pod | object | ~ | Pod-level security context |
| extractorService.airflow.webServer.service | object | ~ | Service configuration |
| extractorService.airflow.webServer.service.annotations | dict | `{}` | Service annotations |
| extractorService.airflow.webServer.service.externalPort | object | ~ | External port configuration |
| extractorService.airflow.webServer.service.externalPort.name | string | `"http"` | Port name |
| extractorService.airflow.webServer.service.externalPort.number | int | `8080` | Port number |
| extractorService.airflow.webServer.service.internalPort | object | ~ | Internal port configuration |
| extractorService.airflow.webServer.service.internalPort.name | string | `"http"` | Port name |
| extractorService.airflow.webServer.service.internalPort.number | int | `8080` | Port number |
| extractorService.airflow.webServer.service.labels | dict | `{}` | Service labels |
| extractorService.airflow.webServer.service.name | string | `"airflow-webserver-service"` | Service name |
| extractorService.airflow.webServer.service.type | string | `"ClusterIP"` | Service type |
| extractorService.postgres | object | ~ | Postgres database configuration |
| extractorService.postgres.component | string | .Chart.Name | Component name for label 'app.kubernetes.io/component' |
| extractorService.postgres.extraAnnotations | dict | `{}` | Extra annotations to add to all resources |
| extractorService.postgres.extraEnv | list | `[]` | Each item is an object with {name, value}. |
| extractorService.postgres.extraLabels | dict | `{}` | Extra labels to add to all resources |
| extractorService.postgres.image | object | ~ | Image details |
| extractorService.postgres.image.host | string | `"docker.io"` | Hostname of the container registry |
| extractorService.postgres.image.name | string | `"postgres"` | Image repository |
| extractorService.postgres.image.tag | string | `16` | Image tag |
| extractorService.postgres.imagePullPolicy | string | `"IfNotPresent"` | Image pull policy |
| extractorService.postgres.name | string | `"postgres"` | Application name |
| extractorService.postgres.partOf | string | .Chart.Name | Part of label 'app.kubernetes.io/part-of' |
| extractorService.postgres.persistence | object | ~ | Persistence configuration |
| extractorService.postgres.persistence.accessModes | array | `["ReadWriteOnce"]` | Access modes for the persistent volume |
| extractorService.postgres.persistence.name | string | `"postgres-data"` | Name of the PersistentVolumeClaim |
| extractorService.postgres.persistence.size | string | `"10Gi"` | Size of the persistent volume |
| extractorService.postgres.persistence.storageClass | string | `"csi-ceph-rbd-du"` | Storage class name |
| extractorService.postgres.persistence.volumeMode | string | `"Filesystem"` | Volume mode |
| extractorService.postgres.replicaCount | int | `1` | Number of desired pods |
| extractorService.postgres.resources | object | ~ | Resource requests and limits |
| extractorService.postgres.resources.limits | object | {} | Resource limits for the container |
| extractorService.postgres.resources.requests | object | {} | Resource requests for the container |
| extractorService.postgres.restartPolicy | string | `"Always"` | Restart policy for the pod |
| extractorService.postgres.secrets | object | ~ | Database secrets |
| extractorService.postgres.secrets.database | string | `""` | Database name |
| extractorService.postgres.secrets.password | string | `""` | Database password |
| extractorService.postgres.secrets.user | string | `""` | Database user |
| extractorService.postgres.securityContext | object | ~ | Security context settings |
| extractorService.postgres.securityContext.container | object | ~ | Container-level security context |
| extractorService.postgres.securityContext.pod | object | ~ | Pod-level security context |
| extractorService.postgres.service | object | ~ | Service configuration |
| extractorService.postgres.service.annotations | dict | `{}` | Service annotations |
| extractorService.postgres.service.externalPort | object | ~ | External port configuration |
| extractorService.postgres.service.externalPort.name | string | `"postgres"` | Port name |
| extractorService.postgres.service.externalPort.number | int | `5432` | Port number |
| extractorService.postgres.service.internalPort | object | ~ | Internal port configuration |
| extractorService.postgres.service.internalPort.name | string | `"postgres"` | Port name |
| extractorService.postgres.service.internalPort.number | int | `5432` | Port number |
| extractorService.postgres.service.labels | dict | `{}` | Service labels |
| extractorService.postgres.service.name | string | `"postgres-service"` | Service name |
| extractorService.postgres.service.type | string | `"ClusterIP"` | Service type |
| global.runId | int | `0` | Unique run identifier |
| global.s3 | object | ~ | S3 configuration |
| global.s3.endpointUrl | string | `"endpointUrl"` | S3 endpoint URL |
| global.s3.secrets | object | ~ | S3 secrets |
| global.s3.secrets.accessKey | string | `"accessKey"` | S3 access key |
| global.s3.secrets.secretKey | string | `"secretKey"` | S3 secret key |
| global.s3.storeUrl | string | `"storeUrl"` | S3 store URL |
| ingress | object | ~ | Ingress configuration |
| ingress.annotations | dict | `{}` | Ingress annotations |
| ingress.className | string | `"nginx"` | Ingress class name |
| ingress.hosts | object | ~ | Hosts configuration |
| ingress.hosts.extractorService | list | `[]` | Each item is an object with {name, tls} |
| ingress.hosts.ingressService | list | `[]` | Each item is an object with {name, tls} |
| ingress.labels | dict | {} | Ingress labels |
| ingressService | object | ~ | Ingress Service configuration |
| ingressService.component | string | .Chart.Name | Component name for label 'app.kubernetes.io/component' |
| ingressService.configuration | object | ~ | Service configuration |
| ingressService.configuration.fastapi | object | ~ | FastAPI configuration |
| ingressService.configuration.fastapi.secrets | object | ~ | FastAPI secrets |
| ingressService.configuration.fastapi.secrets.usersJson | string | "{}" | Must be provided as a JSON string. |
| ingressService.extraAnnotations | dict | `{}` | Extra annotations to add to all resources |
| ingressService.extraEnv | list | `[]` | Each item is an object with {name, value}. |
| ingressService.extraLabels | dict | `{}` | Extra labels to add to all resources |
| ingressService.image | object | ~ | Image details |
| ingressService.image.host | string | `"docker.io"` | Hostname of the container registry |
| ingressService.image.name | string | `"jbrezmorf/zarr-fuse-ingress-service"` | Image repository |
| ingressService.image.tag | string | `""` | Image tag |
| ingressService.imagePullPolicy | string | `"Always"` | Image pull policy |
| ingressService.name | string | `"ingress-service"` | Application name |
| ingressService.partOf | string | .Chart.Name | Part of label 'app.kubernetes.io/part-of' |
| ingressService.replicaCount | int | `1` | Number of desired pods |
| ingressService.resources | object | ~ | Resource requests and limits |
| ingressService.resources.limits | object | {} | Resource limits for the container |
| ingressService.resources.requests | object | {} | Resource requests for the container |
| ingressService.restartPolicy | string | `"Always"` | Restart policy for the pod |
| ingressService.securityContext | object | ~ | Security context settings |
| ingressService.securityContext.container | object | ~ | Container-level security context |
| ingressService.securityContext.pod | object | ~ | Pod-level security context |
| ingressService.service | object | ~ | Service configuration |
| ingressService.service.annotations | dict | `{}` | Service annotations |
| ingressService.service.externalPort | object | ~ | External port configuration |
| ingressService.service.externalPort.name | string | `"http"` | Port name |
| ingressService.service.externalPort.number | int | `80` | Port number |
| ingressService.service.internalPort | object | ~ | Internal port configuration |
| ingressService.service.internalPort.name | string | `"http"` | Port name |
| ingressService.service.internalPort.number | int | `8000` | Port number |
| ingressService.service.labels | dict | `{}` | Service labels |
| ingressService.service.name | string | `"ingress-service"` | Service name |
| ingressService.service.type | string | `"ClusterIP"` | Service type |
