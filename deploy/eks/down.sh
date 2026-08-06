#!/usr/bin/env bash
# Tear the test cluster down and leave nothing behind to bill for.
#
# One command, and it is the important half. A lingering EKS cluster is roughly
# a dollar an hour before it holds any data, so teardown is part of the
# procedure, not a chore for later.
#
#   ORBITA_EKS_REGION=us-west-2 ORBITA_EKS_BUCKET=orbita-test-you deploy/eks/down.sh
#
# It removes, in order: the Helm release, the persistent volumes it left behind,
# and then everything Terraform owns -- the cluster, its VPC and OIDC provider,
# both IRSA roles, and the bucket with everything in it. Each step tolerates the
# thing already being gone, so a teardown after a half-finished bring-up still
# ends with nothing.
set -euo pipefail

# shellcheck source=deploy/eks/_common.sh
# shellcheck disable=SC1091  # sourced by absolute path resolved at runtime
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

require_tools aws kubectl helm
TF="$(tf_bin)"
resolve_config

log "tearing down cluster ${ORBITA_EKS_CLUSTER}, region ${ORBITA_EKS_REGION}, bucket ${ORBITA_EKS_BUCKET}"

# 1. The release and its volumes. A StatefulSet's PersistentVolumeClaims outlive
# a helm uninstall on purpose, and each one is backed by an EBS volume Terraform
# does not know about, so deleting the namespace's PVCs is what lets the CSI
# driver release those volumes before the cluster that runs the driver is gone.
#
# Point kubectl at THIS cluster first, explicitly, rather than trusting whatever
# context happens to be current. An operator who switched contexts after
# bring-up would otherwise have this uninstall a release and delete PVCs on an
# unrelated cluster. update-kubeconfig failing means the cluster is already
# gone, which is the one case where skipping the Kubernetes cleanup is correct.
if aws eks update-kubeconfig --name "$ORBITA_EKS_CLUSTER" --region "$ORBITA_EKS_REGION" >/dev/null 2>&1; then
  if helm status "$ORBITA_EKS_RELEASE" --namespace "$ORBITA_EKS_NAMESPACE" >/dev/null 2>&1; then
    log "uninstalling the orbita release"
    # Tolerated: a release that is already gone is not a reason to stop.
    helm uninstall "$ORBITA_EKS_RELEASE" --namespace "$ORBITA_EKS_NAMESPACE" --wait || true
  fi
  # NOT tolerated. --ignore-not-found already makes an absent PVC a success, so
  # anything that reaches here is a real failure to release EBS volumes, and
  # destroying the cluster and its CSI driver on top of that orphans those
  # volumes to bill silently. Let it stop teardown so an operator deals with it
  # while the driver that can still delete the volumes is alive.
  log "deleting persistent volume claims so their EBS volumes are released"
  kubectl delete pvc --all --namespace "$ORBITA_EKS_NAMESPACE" --ignore-not-found --wait
else
  log "cluster ${ORBITA_EKS_CLUSTER} not reachable, skipping release and volume cleanup"
fi

# 2. Everything AWS, in one Terraform pass. destroy removes the cluster, the node
# group, the VPC, the OIDC provider, both IRSA roles, and the bucket. The bucket
# has force_destroy set, so it is emptied and deleted rather than blocking the
# destroy the way a non-empty bucket would. --auto-approve because this script
# is the confirmation.
log "destroying Terraform-managed infrastructure (this takes several minutes)"
"$TF" -chdir="$TF_DIR" destroy -input=false -auto-approve \
  -var "namespace=$ORBITA_EKS_NAMESPACE" \
  -var "region=$ORBITA_EKS_REGION" \
  -var "bucket_name=$ORBITA_EKS_BUCKET" \
  -var "cluster_name=$ORBITA_EKS_CLUSTER"

log "down. nothing left running."
