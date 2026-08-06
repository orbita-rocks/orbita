#!/usr/bin/env bash
# Stand up the disposable Orbita test cluster: with Terraform, a VPC, an EKS
# cluster with OIDC and an IRSA role scoped to one bucket, and the bucket; then a
# gp3 StorageClass and the chart on top.
#
# One command. When it returns, `deploy/eks/smoke.sh` should pass.
#
#   ORBITA_EKS_REGION=us-west-2 ORBITA_EKS_BUCKET=orbita-test-you deploy/eks/up.sh
#
# Terraform owns the AWS layer and is re-runnable: a repeat apply after a partial
# failure converges rather than duplicating. The Kubernetes steps after it are
# idempotent too. Tear it all down with down.sh, which is the half of this that
# pays the bill if you forget it.
set -euo pipefail

# shellcheck source=deploy/eks/_common.sh
# shellcheck disable=SC1091  # sourced by absolute path resolved at runtime
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

require_tools aws kubectl helm envsubst
TF="$(tf_bin)"
resolve_config

REPO_ROOT="$(cd "$EKS_DIR/../.." && pwd)"
RENDERED_VALUES="$EKS_DIR/values-eks.rendered.yaml"

log "account ${ORBITA_EKS_ACCOUNT_ID}, region ${ORBITA_EKS_REGION}, bucket ${ORBITA_EKS_BUCKET}, cluster ${ORBITA_EKS_CLUSTER}"

# 1. The AWS layer, in one Terraform pass: the VPC, the cluster, its OIDC
# provider, both IRSA roles, the EBS CSI addon, and the bucket. Terraform reads
# AWS credentials from the environment the same way the aws CLI does.
log "applying Terraform (this takes ~15 minutes on a fresh cluster)"
"$TF" -chdir="$TF_DIR" init -input=false
# The namespace is threaded through because it is half of the IRSA trust subject
# (<namespace>:orbita). If the operator overrides ORBITA_EKS_NAMESPACE, the role
# has to trust that namespace or every pod fails to assume it. The service
# account name is fixed at orbita by the overlay, so it stays Terraform's
# default and is not passed here.
"$TF" -chdir="$TF_DIR" apply -input=false -auto-approve \
  -var "region=$ORBITA_EKS_REGION" \
  -var "bucket_name=$ORBITA_EKS_BUCKET" \
  -var "cluster_name=$ORBITA_EKS_CLUSTER" \
  -var "namespace=$ORBITA_EKS_NAMESPACE"

# Point kubectl at the new cluster. Terraform created it; this writes the
# kubeconfig entry the Kubernetes steps below need.
log "writing kubeconfig for ${ORBITA_EKS_CLUSTER}"
aws eks update-kubeconfig --name "$ORBITA_EKS_CLUSTER" --region "$ORBITA_EKS_REGION" >/dev/null

# 2. The gp3 StorageClass the overlay names. The addon Terraform installed
# provisions it; this object is what a PersistentVolumeClaim asks for by name.
log "applying the gp3 StorageClass"
kubectl apply -f "$EKS_DIR/storageclass-gp3.yaml"

# 3. The chart, with the AWS overlay. envsubst fills the account id, region, and
# bucket the overlay carries; the role name in its annotation is fixed and
# matches the role Terraform just created.
log "rendering ${RENDERED_VALUES}"
# shellcheck disable=SC2016  # envsubst wants the literal ${VAR} names, not their values
envsubst '${ORBITA_EKS_ACCOUNT_ID} ${ORBITA_EKS_REGION} ${ORBITA_EKS_BUCKET}' \
  < "$REPO_ROOT/deploy/helm/orbita/values-eks.yaml" > "$RENDERED_VALUES"

# Sanity check: the role the overlay actually names must be the role Terraform
# made. Read it straight out of the rendered overlay rather than reconstructing
# it, so this catches a wrong account or a renamed role however it crept in. A
# mismatch here is far cheaper to find now than as every pod 403ing on write.
tf_role_arn="$("$TF" -chdir="$TF_DIR" output -raw irsa_role_arn)"
overlay_role_arn="$(grep -o 'arn:aws:iam::[0-9]*:role/[^"[:space:]]*' "$RENDERED_VALUES" | head -1)"
[ "$tf_role_arn" = "$overlay_role_arn" ] \
  || die "role ARN mismatch: Terraform made ${tf_role_arn}, overlay names ${overlay_role_arn}"

log "installing the orbita chart"
helm upgrade --install "$ORBITA_EKS_RELEASE" "$REPO_ROOT/deploy/helm/orbita" \
  --namespace "$ORBITA_EKS_NAMESPACE" --create-namespace \
  --values "$RENDERED_VALUES" \
  --wait --timeout 10m

log "up. run deploy/eks/smoke.sh to prove it works, and deploy/eks/down.sh when you are finished."
