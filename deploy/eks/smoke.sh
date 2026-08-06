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

# The .oseg object keys currently in the bucket. The segment check below
# requires a NEW one, so it has to know what was there before this run wrote
# anything. A leftover segment from an earlier run proves nothing about this
# write or this run's IRSA access.
list_segments() {
  aws s3 ls "s3://${ORBITA_EKS_BUCKET}/" --recursive | awk '{print $4}' | grep '\.oseg$' || true
}
segments_before="$(list_segments)"

# 2. A keyspace. Tolerate one already there from a previous smoke run.
log "creating keyspace ${KEYSPACE}"
"$ORBITA_BIN" keyspace create "$KEYSPACE" >/dev/null 2>&1 || true

# 3. A write that reads back. Retry the write: keyspace creation returns before
# the workers necessarily open the new partition, so an immediate write can come
# back Unavailable on a perfectly healthy cluster. That is a propagation delay,
# not a failure, so retry within a bound rather than let it fail the smoke.
log "writing and reading back"
wrote=""
for _ in $(seq 1 15); do
  if "$ORBITA_BIN" set "$KEYSPACE" "$KEY" "$VALUE" >/dev/null 2>&1; then
    wrote=yes
    break
  fi
  sleep 2
done
[ -n "$wrote" ] || die "write never succeeded; the partition may not have opened"
got="$("$ORBITA_BIN" get "$KEYSPACE" "$KEY")"
[ "$got" = "$VALUE" ] || die "read back '${got}', expected '${VALUE}'"

# 4. A NEW segment in the bucket. The flush timer is 30 seconds, so poll for up
# to two minutes for an .oseg that was not there before this run wrote. Requiring
# a new one is what makes this prove the current write reached real S3 through
# the IRSA credentials; accepting any historical segment would pass on a broken
# cluster the moment a previous run had ever succeeded.
log "waiting for a new segment in s3://${ORBITA_EKS_BUCKET} (flush timer is 30s)"
new_segment=""
for _ in $(seq 1 24); do
  new_segment="$(comm -13 <(printf '%s\n' "$segments_before" | sort -u) <(list_segments | sort -u) | grep -m1 '\.oseg$' || true)"
  [ -n "$new_segment" ] && break
  sleep 5
done
[ -n "$new_segment" ] || die "no new segment appeared in the bucket within two minutes"

log "new segment from this run: ${new_segment}"
log "smoke passed: quorum formed, keyspace created, write round-tripped, fresh segment in S3."
