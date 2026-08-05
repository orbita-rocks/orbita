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
The credential source written into the config file.

Empty in values means "follow the keys": keys mean static, and no keys means
"default", which is what an unset object_store.credential_source means to the
binary — resolve at startup in the AWS chain's order and log the result.

Empty deliberately does NOT mean instance-profile. On EKS the instance profile
is the node role and IRSA is the workload role, so rendering instance-profile
for every keyless release would take existing IRSA deployments and re-point
them at a different principal, which succeeds rather than failing wherever node
IMDS is reachable.

It is resolved here rather than left out so that the rendered ConfigMap says
which one this release picked; an operator reading it should not have to know
the defaulting rule.
*/}}
{{- define "orbita.credentialSource" -}}
{{- if .Values.objectStore.credentialSource -}}
{{- .Values.objectStore.credentialSource -}}
{{- else if or .Values.objectStore.existingSecret .Values.objectStore.accessKeyId -}}
static
{{- else -}}
default
{{- end -}}
{{- end -}}

{{/*
Whether this release authenticates with an access key pair.

One predicate, used by validation, by the Secret, and by the container
environment alike, so those three cannot disagree about whether a key is in
play — which is exactly how a release ends up rendering access-key environment
variables next to a keyless credential source.
*/}}
{{- define "orbita.usesStaticKeys" -}}
{{- if eq (include "orbita.credentialSource" .) "static" -}}
true
{{- end -}}
{{- end -}}

{{/*
Whether a Secret is in play at all.

Three things can put one there: a static key pair, an inline role external id,
and an existingSecret the operator brought themselves. A keyless release with
none of them has no Secret, mounts nothing, and sets no credential environment
variables, which is the whole point of it.

Note this is broader than orbita.usesStaticKeys on purpose: a keyless release
that assumes a cross-account role still needs a Secret for the external id, and
it must read *only* that key out of it.
*/}}
{{- define "orbita.usesObjectStoreSecret" -}}
{{- if or (include "orbita.usesStaticKeys" .) .Values.objectStore.externalId .Values.objectStore.existingSecret -}}
true
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
{{- $known := list "default" "static" "environment" "web-identity" "container" "instance-profile" -}}
{{- if not (has $source $known) -}}
{{- fail (printf "objectStore.credentialSource must be one of %s, got %q" (join ", " $known) $source) -}}
{{- end -}}
{{- if eq $source "static" -}}
{{- if not (or .Values.objectStore.existingSecret .Values.objectStore.accessKeyId) -}}
{{- fail "objectStore.credentialSource is static but neither objectStore.accessKeyId nor objectStore.existingSecret is set" -}}
{{- end -}}
{{- else -}}
{{/*
A keyless source with an inline key, or with an existingSecret that is not
carrying an external id, is the contradiction the binary refuses at startup:
it would render mandatory access-key environment variables from a Secret whose
keys this release has no reason to expect. Rejecting it here turns a
CrashLoopBackOff (or a CreateContainerConfigError, if the Secret holds only an
external id) into a sentence at `helm template` time.
*/}}
{{- if .Values.objectStore.accessKeyId -}}
{{- fail (printf "objectStore.credentialSource is %s but objectStore.accessKeyId is also set; remove one, because a node that silently ignores a credential is a node nobody can audit" $source) -}}
{{- end -}}
{{- if and .Values.objectStore.existingSecret (not .Values.objectStore.roleArn) -}}
{{- fail (printf "objectStore.credentialSource is %s but objectStore.existingSecret is set with no objectStore.roleArn; nothing would read that Secret. Set credentialSource to static to use its access keys, or drop existingSecret." $source) -}}
{{- end -}}
{{- end -}}
{{- if and .Values.objectStore.externalId (not .Values.objectStore.roleArn) -}}
{{- fail "objectStore.externalId is set but objectStore.roleArn is not; an external id only means anything to a role being assumed" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
The credential environment variables for a node container.

Access keys are mounted only when the source is static. Anything else gets at
most the role external id, and a keyless release with no role gets nothing at
all — which is what makes the keyless path a deployment with no Secret rather
than a deployment with an empty one.
*/}}
{{- define "orbita.objectStoreEnv" -}}
{{- if include "orbita.usesStaticKeys" . }}
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
{{- if and .Values.objectStore.roleArn (include "orbita.usesObjectStoreSecret" .) }}
- name: ORBITA_OBJECT_STORE_ROLE_EXTERNAL_ID
  valueFrom:
    secretKeyRef:
      name: {{ include "orbita.objectStoreSecretName" . }}
      key: external_id
      # Optional so that a role whose trust policy does not demand an external
      # id can share a Secret with one that does.
      optional: true
{{- end }}
{{- end -}}
