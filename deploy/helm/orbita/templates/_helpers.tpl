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
The leader group, as stable DNS names from the headless Service, on the peer
port. Every node gets the same list, which is what the bootstrap rule needs:
the node with the lowest address forms the initial Raft configuration and the
rest wait to hear from it, with no coordination and no race. Workers use the
same list to find a leader to register with.

These are peer addresses, not client ones. A leader group listed on the client
port would look almost right and never form a quorum.
*/}}
{{- define "orbita.leaderPeers" -}}
{{- $full := include "orbita.fullname" . -}}
{{- $svc := printf "%s-leader" $full -}}
{{- $peers := list -}}
{{- range $i := until (int .Values.leader.replicas) -}}
{{- $peers = append $peers (printf "%s-%d.%s.%s.svc.cluster.local:%d" $svc $i $svc $.Release.Namespace (int $.Values.service.peerPort)) -}}
{{- end -}}
{{- join "," $peers -}}
{{- end -}}

{{/*
What a probe runs: the binary asking its own client port whether it is serving.

It is `cluster ping` rather than `cluster describe` on purpose. A probe asks
whether this node is up. Whether the cluster is well is a different question,
and answering it in a liveness probe would kill healthy pods during an incident
somebody else is already handling, which is the failure mode ADR 0005 calls out
by name.

One definition, used by all three probes, because the day liveness and
readiness drift apart is the day one of them is wrong and nobody notices.
*/}}
{{- define "orbita.probeCommand" -}}
- /usr/local/bin/orbita
- --endpoint
- http://127.0.0.1:{{ .Values.service.port }}
- cluster
- ping
{{- end -}}

{{/*
The three probes, rendered together so that a StatefulSet cannot pick up two of
them and forget the third.

Takes a dict of "root" and "startupFailureThreshold", because the startup
budget is the one thing that differs between a leader and a worker.

Readiness is weaker than ADR 0005 requires. It should mean registered,
recovered, and caught up, and today it means the process answers on the client
port, because the server reports nothing better. That gap is written down in
values.yaml under `probes` and in docs/UPGRADES.md rather than hidden here.
*/}}
{{- define "orbita.probes" -}}
{{- $root := .root -}}
{{- $probes := $root.Values.probes -}}
{{- if $probes.startup.enabled }}
startupProbe:
  exec:
    command:
      {{- include "orbita.probeCommand" $root | nindent 6 }}
  periodSeconds: {{ $probes.startup.periodSeconds }}
  failureThreshold: {{ .startupFailureThreshold }}
{{- end }}
{{- if $probes.readiness.enabled }}
readinessProbe:
  exec:
    command:
      {{- include "orbita.probeCommand" $root | nindent 6 }}
  periodSeconds: {{ $probes.readiness.periodSeconds }}
  failureThreshold: {{ $probes.readiness.failureThreshold }}
{{- end }}
{{- if $probes.liveness.enabled }}
livenessProbe:
  exec:
    command:
      {{- include "orbita.probeCommand" $root | nindent 6 }}
  periodSeconds: {{ $probes.liveness.periodSeconds }}
  failureThreshold: {{ $probes.liveness.failureThreshold }}
{{- end }}
{{- end -}}

{{- define "orbita.objectStoreSecretName" -}}
{{- default (printf "%s-object-store" (include "orbita.fullname" .)) .Values.objectStore.existingSecret -}}
{{- end -}}
