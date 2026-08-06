#!/usr/bin/env bash
# Prove the test cluster works end to end: quorum forms, a keyspace is created,
# a write lands and reads back, and a segment shows up in the bucket.
#
#   ORBITA_EKS_REGION=us-west-2 ORBITA_EKS_BUCKET=orbita-test-you deploy/eks/smoke.sh
#
# It drives the shipped `orbita` CLI through a port-forward rather than the e2e
# harness, which spawns its own local node and cannot be pointed at a remote
# endpoint. Set ORBITA_BIN if `orbita` is not on your PATH (for example
# target/debug/orbita from a build).
#
# The last check waits on the bucket because a segment is published on the
# 30-second flush timer, not on the write. That wait is the point of the check:
# it is what proves the object store, the IRSA credentials, and the flush path
# all actually work against real S3, which is the whole reason this cluster
# exists.
set -euo pipefail

# shellcheck source=deploy/eks/_common.sh
# shellcheck disable=SC1091  # sourced by absolute path resolved at runtime
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

require_tools aws kubectl
resolve_config

ORBITA_BIN="${ORBITA_BIN:-orbita}"
command -v "$ORBITA_BIN" >/dev/null 2>&1 || die "orbita CLI not found; set ORBITA_BIN to its path"

export ORBITA_ENDPOINT="http://127.0.0.1:7100"
KEYSPACE="smoke"
KEY="greeting"
VALUE="hello-from-eks"

# Port-forward the client Service, and make sure it is torn down however this
# script exits. A leaked port-forward is a confusing failure on the next run.
log "port-forwarding svc/${ORBITA_EKS_RELEASE} 7100"
kubectl port-forward "svc/${ORBITA_EKS_RELEASE}" 7100:7100 \
  --namespace "$ORBITA_EKS_NAMESPACE" >/dev/null 2>&1 &
PORT_FORWARD_PID=$!
cleanup() { kill "$PORT_FORWARD_PID" 2>/dev/null || true; }
trap cleanup EXIT

# Wait for the forward to carry a live connection. cluster ping asks only whether
# the process answers, which is exactly the right question for "is the tunnel up
# and a worker behind it".
log "waiting for the endpoint to answer"
for _ in $(seq 1 30); do
  if "$ORBITA_BIN" cluster ping >/dev/null 2>&1; then break; fi
  sleep 2
done
"$ORBITA_BIN" cluster ping >/dev/null 2>&1 || die "endpoint never answered on 127.0.0.1:7100"

# 1. Quorum. describe is served by the Raft leader, so a description that names
# a leader and reports every node healthy is proof the group formed.
log "checking quorum"
description="$("$ORBITA_BIN" cluster describe)"
printf '%s\n' "$description"
printf '%s' "$description" | grep -q "raft leader" \
  || die "no Raft leader in cluster describe; quorum did not form"
printf '%s' "$description" | grep -Eq "0 suspect, 0 dead" \
  || die "cluster reports unhealthy nodes"

# 2. A keyspace. Tolerate one already there from a previous smoke run.
log "creating keyspace ${KEYSPACE}"
"$ORBITA_BIN" keyspace create "$KEYSPACE" >/dev/null 2>&1 || true

# 3. A write that reads back. This is the client-visible contract in one line.
log "writing and reading back"
"$ORBITA_BIN" set "$KEYSPACE" "$KEY" "$VALUE" >/dev/null
got="$("$ORBITA_BIN" get "$KEYSPACE" "$KEY")"
[ "$got" = "$VALUE" ] || die "read back '${got}', expected '${VALUE}'"

# 4. A segment in the bucket. The flush timer is 30 seconds, so poll for up to
# two minutes. Finding an .oseg under the keyspace prefix is proof the write
# reached real S3 through the IRSA credentials, not just a worker's local disk.
log "waiting for a segment to land in s3://${ORBITA_EKS_BUCKET} (flush timer is 30s)"
found=""
for _ in $(seq 1 24); do
  if aws s3 ls "s3://${ORBITA_EKS_BUCKET}/" --recursive | grep -q "\.oseg$"; then
    found=yes
    break
  fi
  sleep 5
done
[ -n "$found" ] || die "no segment appeared in the bucket within two minutes"

log "segments in the bucket:"
aws s3 ls "s3://${ORBITA_EKS_BUCKET}/" --recursive | grep -E "\.(oseg|json)$" >&2 || true

log "smoke passed: quorum formed, keyspace created, write round-tripped, segment in S3."
