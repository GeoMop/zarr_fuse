{{/*
Expand the name of the chart.
*/}}
{{- define "holoviz.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
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
Create chart name and version as used by the chart label.
*/}}
{{- define "holoviz.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "holoviz.labels" -}}
helm.sh/chart: {{ include "holoviz.chart" . }}
{{ include "holoviz.selectorLabels" . }}
app.kubernetes.io/version: {{ default .Chart.AppVersion .Values.frontend.image.tag | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: zarr-fuse
{{- end }}

{{/*
Selector labels
*/}}
{{- define "holoviz.selectorLabels" -}}
app.kubernetes.io/name: {{ include "holoviz.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: frontend
{{- end }}

{{- define "holoviz.host" -}}
zarr-fuse-{{ .Release.Name }}.dyn.cloud.e-infra.cz
{{- end }}

{{- define "holoviz.s3SecretName" -}}
{{- default (printf "%s-s3" (include "holoviz.fullname" .)) .Values.frontend.s3.existingSecret }}
{{- end }}
