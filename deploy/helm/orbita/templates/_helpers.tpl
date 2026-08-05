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
{{- $peers = append $peers (printf "%d=%s-%d.%s.%s.svc.cluster.local:%d" (add1 $i) $svc $i $svc $.Release.Namespace (int $.Values.service.peerPort)) -}}
{{- end -}}
{{- join "," $peers -}}
{{- end -}}

{{/*
What the probes run: the binary asking its own client port.

Two questions, two commands, on purpose. `cluster ready` asks whether this
node may take traffic: registered with the leader group, write-ahead log
recovered, partitions open and caught up. That is what readiness and startup
gate on, and it is what makes a rolling upgrade wait for a node instead of
outrunning it (ADR 0005).

`cluster ping` asks only whether the process answers, and that is all liveness
may ever ask. Readiness depends on the leader group, and a liveness probe that
did would kill healthy pods during an incident somebody else is already
handling, which is the failure mode ADR 0005 calls out by name.
*/}}
{{- define "orbita.livenessCommand" -}}
- /usr/local/bin/orbita
- --endpoint
- http://127.0.0.1:{{ .Values.service.port }}
- cluster
- ping
{{- end -}}

{{- define "orbita.readinessCommand" -}}
- /usr/local/bin/orbita
- --endpoint
- http://127.0.0.1:{{ .Values.service.port }}
- cluster
- ready
{{- end -}}

{{/*
The three probes, rendered together so that a StatefulSet cannot pick up two of
them and forget the third.

Takes a dict of "root" and "startupFailureThreshold", because the startup
budget is the one thing that differs between a leader and a worker.

Startup uses the readiness command with its own generous budget: recovering a
large write-ahead log or waiting for the leader group legitimately takes time,
and that is the startup probe's problem rather than the liveness probe's.
*/}}
{{- define "orbita.probes" -}}
{{- $root := .root -}}
{{- $probes := $root.Values.probes -}}
{{- if $probes.startup.enabled }}
startupProbe:
  exec:
    command:
      {{- include "orbita.readinessCommand" $root | nindent 6 }}
  periodSeconds: {{ $probes.startup.periodSeconds }}
  failureThreshold: {{ .startupFailureThreshold }}
{{- end }}
{{- if $probes.readiness.enabled }}
readinessProbe:
  exec:
    command:
      {{- include "orbita.readinessCommand" $root | nindent 6 }}
  periodSeconds: {{ $probes.readiness.periodSeconds }}
  failureThreshold: {{ $probes.readiness.failureThreshold }}
{{- end }}
{{- if $probes.liveness.enabled }}
livenessProbe:
  exec:
    command:
      {{- include "orbita.livenessCommand" $root | nindent 6 }}
  periodSeconds: {{ $probes.liveness.periodSeconds }}
  failureThreshold: {{ $probes.liveness.failureThreshold }}
{{- end }}
{{- end -}}

{{- define "orbita.objectStoreSecretName" -}}
{{- default (printf "%s-object-store" (include "orbita.fullname" .)) .Values.objectStore.existingSecret -}}
{{- end -}}

{{/*
Whether a Secret is in play at all.

The keyless path — an instance profile, with or without a role to assume — has
no Secret, mounts nothing, and sets no credential environment variables. This
is the one predicate that decides that, so a StatefulSet cannot disagree with
the Secret template about whether one exists.
*/}}
{{- define "orbita.usesObjectStoreSecret" -}}
{{- if or .Values.objectStore.existingSecret .Values.objectStore.accessKeyId .Values.objectStore.externalId -}}
true
{{- end -}}
{{- end -}}

{{/*
The credential source written into the config file.

Empty in values means "follow the keys", which matches what the binary does
with an unset object_store.credential_source. It is resolved here rather than
left out so that the rendered ConfigMap says which one this release picked;
an operator reading it should not have to know the defaulting rule.
*/}}
{{- define "orbita.credentialSource" -}}
{{- if .Values.objectStore.credentialSource -}}
{{- .Values.objectStore.credentialSource -}}
{{- else if or .Values.objectStore.existingSecret .Values.objectStore.accessKeyId -}}
static
{{- else -}}
instance-profile
{{- end -}}
{{- end -}}

{{/*
Fails a release whose credentials cannot possibly work, at template time,
because a rendered manifest that deploys and then 403s on every write is the
most expensive way to learn this.
*/}}
{{- define "orbita.validateObjectStore" -}}
{{- if .Values.objectStore.endpoint -}}
{{- $source := include "orbita.credentialSource" . -}}
{{- if and (eq $source "static") (not (or .Values.objectStore.existingSecret .Values.objectStore.accessKeyId)) -}}
{{- fail "objectStore.credentialSource is static but neither objectStore.accessKeyId nor objectStore.existingSecret is set" -}}
{{- end -}}
{{- if and (eq $source "instance-profile") .Values.objectStore.accessKeyId -}}
{{- fail "objectStore.credentialSource is instance-profile but objectStore.accessKeyId is also set; remove one, because a node that silently ignores a credential is a node nobody can audit" -}}
{{- end -}}
{{- if and .Values.objectStore.externalId (not .Values.objectStore.roleArn) -}}
{{- fail "objectStore.externalId is set but objectStore.roleArn is not; an external id only means anything to a role being assumed" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
The credential environment variables for a node container.

Keys come from the Secret when there is one and are simply absent when there is
not, which is what makes the instance-profile path a deployment with no Secret
rather than a deployment with an empty one.
*/}}
{{- define "orbita.objectStoreEnv" -}}
{{- if include "orbita.usesObjectStoreSecret" . }}
{{- if or .Values.objectStore.existingSecret .Values.objectStore.accessKeyId }}
- name: ORBITA_OBJECT_STORE_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: {{ include "orbita.objectStoreSecretName" . }}
      key: access_key_id
- name: ORBITA_OBJECT_STORE_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "orbita.objectStoreSecretName" . }}
      key: secret_access_key
{{- end }}
{{- if .Values.objectStore.roleArn }}
- name: ORBITA_OBJECT_STORE_ROLE_EXTERNAL_ID
  valueFrom:
    secretKeyRef:
      name: {{ include "orbita.objectStoreSecretName" . }}
      key: external_id
      # Optional so that a role whose trust policy does not demand an external
      # id can share a Secret with one that does.
      optional: true
{{- end }}
{{- end }}
{{- end -}}
