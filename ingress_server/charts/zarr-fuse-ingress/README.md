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
| https://airflow.apache.org | airflow | 1.18.0 |

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| airflow | object | ~ | Apache Airflow configuration Documentation: https://artifacthub.io/packages/helm/apache-airflow/airflow |
| ingressService | object | ~ | Ingress Service configuration |
| ingressService.component | string | .Chart.Name | Component name for label 'app.kubernetes.io/component' |
| ingressService.configuration | object | ~ | Service configuration |
| ingressService.configuration.fastapi | object | ~ | FastAPI configuration |
| ingressService.configuration.fastapi.secrets | object | ~ | FastAPI secrets |
| ingressService.configuration.fastapi.secrets.usersJson | string | "{}" | Must be provided as a JSON string. |
| ingressService.configuration.s3 | object | ~ | S3 configuration |
| ingressService.configuration.s3.endpointUrl | string | `"endpointUrl"` | S3 endpoint URL |
| ingressService.configuration.s3.secrets | object | ~ | S3 secrets |
| ingressService.configuration.s3.secrets.accessKey | string | `"accessKey"` | S3 access key |
| ingressService.configuration.s3.secrets.secretKey | string | `"secretKey"` | S3 secret key |
| ingressService.configuration.s3.storeUrl | string | `"storeUrl"` | S3 store URL |
| ingressService.extraAnnotations | dict | `{}` | Extra annotations to add to all resources |
| ingressService.extraEnv | list | `[]` | Each item is an object with {name, value}. |
| ingressService.extraLabels | dict | `{}` | Extra labels to add to all resources |
| ingressService.image | object | ~ | Image details |
| ingressService.image.name | string | `"jbrezmorf/ingress-service"` | Image repository |
| ingressService.image.tag | string | `"latest"` | Image tag |
| ingressService.imagePullPolicy | string | `"Always"` | Image pull policy |
| ingressService.ingress | object | ~ | Ingress configuration |
| ingressService.ingress.annotations | dict | `{}` | Ingress annotations |
| ingressService.ingress.className | string | `"nginx"` | Ingress class name |
| ingressService.ingress.hosts | object | ~ | Hosts configuration |
| ingressService.ingress.labels | dict | {} | Ingress labels |
| ingressService.name | string | `"ingress-service"` | Application name |
| ingressService.partOf | string | .Chart.Name | Part of label 'app.kubernetes.io/part-of' |
| ingressService.replicaCount | int | `1` | Number of desired pods |
| ingressService.resources | object | ~ | Resource requests and limits |
| ingressService.resources.limits | object | {} | Resource limits for the container |
| ingressService.resources.requests | object | {} | Resource requests for the container |
| ingressService.restartPolicy | string | `"Always"` | Restart policy for the pod |
| ingressService.securityContext | object | ~ | Security context settings |
| ingressService.securityContext.container | object | {} | Container security context settings |
| ingressService.securityContext.pod | object | {} | Pod security context settings |
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
| registry | object | ~ | Registry definition |
| registry.host | string | `"docker.io"` | Hostname of the container registry |
| runId | int | `0` | Unique run identifier |
