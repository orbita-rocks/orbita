#!/usr/bin/env bash
# Stand up the disposable Orbita test cluster: an S3 bucket, an EKS cluster with
# OIDC and an IRSA role scoped to that bucket, a gp3 StorageClass, and the chart.
#
# One command. When it returns, `deploy/eks/smoke.sh` should pass.
#
#   ORBITA_EKS_REGION=us-west-2 ORBITA_EKS_BUCKET=orbita-test-you deploy/eks/up.sh
#
# Every step is idempotent enough to re-run after a partial failure: the bucket
# create tolerates an existing bucket, and `eksctl create cluster` refuses a
# duplicate rather than damaging one. Tear it all down with down.sh, which is
# the half of this that pays the bill if you forget it.
set -euo pipefail

# shellcheck source=deploy/eks/_common.sh
# shellcheck disable=SC1091  # sourced by absolute path resolved at runtime
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

require_tools aws eksctl kubectl helm envsubst
resolve_config

REPO_ROOT="$(cd "$EKS_DIR/../.." && pwd)"
RENDERED_CLUSTER="$EKS_DIR/cluster.rendered.yaml"
RENDERED_VALUES="$EKS_DIR/values-eks.rendered.yaml"

log "account ${ORBITA_EKS_ACCOUNT_ID}, region ${ORBITA_EKS_REGION}, bucket ${ORBITA_EKS_BUCKET}, cluster ${ORBITA_EKS_CLUSTER}"

# 1. The bucket. It has to exist before the IRSA policy can name it, and before
# the cluster writes a single segment. us-east-1 is the one region whose API
# rejects a LocationConstraint, so it takes a different call.
if aws s3api head-bucket --bucket "$ORBITA_EKS_BUCKET" 2>/dev/null; then
  log "bucket ${ORBITA_EKS_BUCKET} already exists, reusing it"
else
  log "creating bucket ${ORBITA_EKS_BUCKET}"
  if [ "$ORBITA_EKS_REGION" = "us-east-1" ]; then
    aws s3api create-bucket --bucket "$ORBITA_EKS_BUCKET" --region us-east-1
  else
    aws s3api create-bucket --bucket "$ORBITA_EKS_BUCKET" --region "$ORBITA_EKS_REGION" \
      --create-bucket-configuration "LocationConstraint=$ORBITA_EKS_REGION"
  fi
  # A safety net, not the teardown. down.sh empties and deletes the bucket; this
  # only bounds the damage of a cluster left running past its welcome, so a
  # forgotten test does not accrue storage forever.
  aws s3api put-bucket-lifecycle-configuration --bucket "$ORBITA_EKS_BUCKET" \
    --lifecycle-configuration '{"Rules":[{"ID":"expire-test-data","Status":"Enabled","Filter":{"Prefix":""},"Expiration":{"Days":7}}]}'
fi

# 2. The cluster, its OIDC provider, the IRSA role, and the EBS CSI addon, in one
# eksctl pass. envsubst fills only the two tokens cluster.yaml carries; the
# rendered file is gitignored because it is a build artifact, not source.
log "rendering ${RENDERED_CLUSTER}"
# shellcheck disable=SC2016  # envsubst wants the literal ${VAR} names, not their values
envsubst '${ORBITA_EKS_REGION} ${ORBITA_EKS_BUCKET}' \
  < "$EKS_DIR/cluster.yaml" > "$RENDERED_CLUSTER"

if eksctl get cluster --name "$ORBITA_EKS_CLUSTER" --region "$ORBITA_EKS_REGION" >/dev/null 2>&1; then
  log "cluster ${ORBITA_EKS_CLUSTER} already exists, skipping create"
else
  log "creating cluster ${ORBITA_EKS_CLUSTER} (this takes ~15 minutes)"
  eksctl create cluster -f "$RENDERED_CLUSTER"
fi

# kubectl should already point here after eksctl create, but an existing-cluster
# run needs the kubeconfig written explicitly.
aws eks update-kubeconfig --name "$ORBITA_EKS_CLUSTER" --region "$ORBITA_EKS_REGION" >/dev/null

# 3. The gp3 StorageClass the overlay names. The addon provisions it; this
# object is what a PersistentVolumeClaim asks for by name.
log "applying the gp3 StorageClass"
kubectl apply -f "$EKS_DIR/storageclass-gp3.yaml"

# 4. The chart, with the AWS overlay. envsubst fills the account id, region, and
# bucket the overlay carries.
log "rendering ${RENDERED_VALUES}"
# shellcheck disable=SC2016  # envsubst wants the literal ${VAR} names, not their values
envsubst '${ORBITA_EKS_ACCOUNT_ID} ${ORBITA_EKS_REGION} ${ORBITA_EKS_BUCKET}' \
  < "$REPO_ROOT/deploy/helm/orbita/values-eks.yaml" > "$RENDERED_VALUES"

log "installing the orbita chart"
helm upgrade --install "$ORBITA_EKS_RELEASE" "$REPO_ROOT/deploy/helm/orbita" \
  --namespace "$ORBITA_EKS_NAMESPACE" --create-namespace \
  --values "$RENDERED_VALUES" \
  --wait --timeout 10m

log "up. run deploy/eks/smoke.sh to prove it works, and deploy/eks/down.sh when you are finished."
