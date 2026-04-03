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
