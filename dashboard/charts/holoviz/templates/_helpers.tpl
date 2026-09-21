{{/*
Expand the name of the chart.
*/}}
{{- define "holoviz.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Fully qualified app name, release-scoped so several releases can share a namespace.
Truncated at 63 chars (DNS label limit). If the release name already contains the chart
name it is used as-is, e.g. release "holoviz-development" -> "holoviz-development",
release "dashboard" -> "dashboard-holoviz".
*/}}
{{- define "holoviz.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Chart name and version as used by the helm.sh/chart label.
*/}}
{{- define "holoviz.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Selector labels. They are immutable on a Deployment, so keep this set stable.
*/}}
{{- define "holoviz.selectorLabels" -}}
app.kubernetes.io/name: {{ include "holoviz.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: frontend
{{- end }}

{{/*
Common labels, see
https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/
*/}}
{{- define "holoviz.labels" -}}
helm.sh/chart: {{ include "holoviz.chart" . }}
{{ include "holoviz.selectorLabels" . }}
app.kubernetes.io/version: {{ default .Chart.AppVersion .Values.frontend.image.tag | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: zarr-fuse
{{- end }}

{{/*
Public hostname of the dashboard: ingress.host, or the e-infra dynamic DNS name of the release.
Also used as the allowed Bokeh websocket origin.
*/}}
{{- define "holoviz.host" -}}
{{- default (printf "zarr-fuse-%s.dyn.cloud.e-infra.cz" .Release.Name) .Values.ingress.host }}
{{- end }}

{{/*
Name of the TLS secret: ingress.tls.secretName, or derived from the host.
*/}}
{{- define "holoviz.tlsSecretName" -}}
{{- default (printf "%s-tls" (include "holoviz.host" . | replace "." "-")) .Values.ingress.tls.secretName }}
{{- end }}

{{/*
Name of the Secret that provides the ZF_S3_* variables: a pre-created one
(frontend.s3.existingSecret) or the one rendered by this chart.
*/}}
{{- define "holoviz.s3SecretName" -}}
{{- default (printf "%s-s3" (include "holoviz.fullname" .)) .Values.frontend.s3.existingSecret }}
{{- end }}

{{/*
Name of the ConfigMap holding the staged view configuration.
*/}}
{{- define "holoviz.configMapName" -}}
{{- printf "%s-config" (include "holoviz.fullname" .) }}
{{- end }}
