{{/*
Expand the name of the chart.
*/}}
{{- define "strata.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "strata.fullname" -}}
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
Create chart label.
*/}}
{{- define "strata.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "strata.labels" -}}
helm.sh/chart: {{ include "strata.chart" . }}
{{ include "strata.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels.
*/}}
{{- define "strata.selectorLabels" -}}
app.kubernetes.io/name: {{ include "strata.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
ServiceAccount name.
*/}}
{{- define "strata.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "strata.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Image reference.
*/}}
{{- define "strata.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag }}
{{- printf "%s:%s" .Values.image.repository $tag }}
{{- end }}

{{/*
Headless service name (used for peer DNS resolution).
*/}}
{{- define "strata.headlessServiceName" -}}
{{- printf "%s-headless" (include "strata.fullname" .) }}
{{- end }}

{{/*
Name of the Secret that holds S3 credentials (either existing or created).
Returns empty string when IRSA is used.
*/}}
{{- define "strata.s3SecretName" -}}
{{- if .Values.s3.useIRSA }}
{{- "" }}
{{- else if .Values.s3.existingSecret }}
{{- .Values.s3.existingSecret }}
{{- else if and .Values.s3.credentials.accessKeyId .Values.s3.credentials.secretAccessKey }}
{{- printf "%s-s3" (include "strata.fullname" .) }}
{{- end }}
{{- end }}

{{/*
Build the --advertise-peer flag value.
Uses .Values.advertiseAddr when set, otherwise constructs the stable DNS name
from the StatefulSet pod ordinal via the headless service.

Because Helm templates don't have access to pod ordinals at render time, the
value is passed as the MY_POD_NAME env var from the downward API and the
advertise address is assembled via shell in the container args.
This template produces the domain suffix only:
  <pod>.<headless-svc>.<namespace>.svc.cluster.local:<peerPort>
The pod-name prefix is prepended at runtime (see statefulset.yaml args).
*/}}
{{- define "strata.peerDomainSuffix" -}}
{{- if .Values.advertiseAddr }}
{{- .Values.advertiseAddr }}
{{- else }}
{{- printf ".%s.%s.svc.cluster.local:%d" (include "strata.headlessServiceName" .) .Release.Namespace (int .Values.service.peerPort) }}
{{- end }}
{{- end }}

{{/*
MinIO sub-deployment full name.
*/}}
{{- define "strata.minio.fullname" -}}
{{- printf "%s-minio" (include "strata.fullname" .) }}
{{- end }}

{{/*
Name of the Secret that holds MinIO / strata S3 credentials when minio.enabled.
*/}}
{{- define "strata.minio.secretName" -}}
{{- printf "%s-minio" (include "strata.fullname" .) }}
{{- end }}

{{/*
Internal S3 endpoint for the MinIO sub-deployment.
*/}}
{{- define "strata.minio.endpoint" -}}
{{- printf "http://%s:%d" (include "strata.minio.fullname" .) (int .Values.minio.service.port) }}
{{- end }}

{{/*
Effective S3 bucket: explicit s3.bucket takes priority, then minio.bucket.
*/}}
{{- define "strata.effectiveS3Bucket" -}}
{{- if .Values.s3.bucket }}
{{- .Values.s3.bucket }}
{{- else if .Values.minio.enabled }}
{{- .Values.minio.bucket }}
{{- end }}
{{- end }}

{{/*
Effective S3 endpoint: explicit s3.endpoint takes priority, then MinIO service.
*/}}
{{- define "strata.effectiveS3Endpoint" -}}
{{- if .Values.s3.endpoint }}
{{- .Values.s3.endpoint }}
{{- else if .Values.minio.enabled }}
{{- include "strata.minio.endpoint" . }}
{{- end }}
{{- end }}

{{/*
Name of the Secret that holds AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY for
the strata pods.  When minio.enabled, use the MinIO secret; otherwise fall
back to the standard s3SecretName helper.
*/}}
{{- define "strata.effectiveS3SecretName" -}}
{{- if .Values.minio.enabled }}
{{- include "strata.minio.secretName" . }}
{{- else }}
{{- include "strata.s3SecretName" . }}
{{- end }}
{{- end }}
