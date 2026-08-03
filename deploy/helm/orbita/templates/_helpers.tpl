{{/*
Naming and labels, in one place so that a rename cannot half happen.
*/}}

{{- define "orbita.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "orbita.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "orbita.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "orbita.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: orbita
{{- end -}}

{{- define "orbita.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "orbita.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "orbita.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}

{{/*
The initial leader group membership, as stable DNS names from the headless
Service. Every node gets the same list, which is what the bootstrap rule needs:
the node with the lowest address forms the initial Raft configuration and the
rest wait to hear from it, with no coordination and no race.
*/}}
{{- define "orbita.leaderPeers" -}}
{{- $full := include "orbita.fullname" . -}}
{{- $svc := printf "%s-leader" $full -}}
{{- $peers := list -}}
{{- range $i := until (int .Values.leader.replicas) -}}
{{- $peers = append $peers (printf "%s-%d.%s.%s.svc.cluster.local:%d" $svc $i $svc $.Release.Namespace (int $.Values.service.port)) -}}
{{- end -}}
{{- join "," $peers -}}
{{- end -}}

{{- define "orbita.objectStoreSecretName" -}}
{{- default (printf "%s-object-store" (include "orbita.fullname" .)) .Values.objectStore.existingSecret -}}
{{- end -}}
