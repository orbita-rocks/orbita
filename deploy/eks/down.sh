#!/usr/bin/env bash
# Tear the test cluster down and leave nothing behind to bill for.
#
# One command, and it is the important half. A lingering EKS cluster is roughly
# a dollar an hour before it holds any data, so teardown is part of the
# procedure, not a chore for later.
#
#   ORBITA_EKS_REGION=us-west-2 ORBITA_EKS_BUCKET=orbita-test-you deploy/eks/down.sh
#
# It removes, in order: the Helm release, the EKS cluster (which unwinds the
# node group, the OIDC provider, and the IRSA role with it), and the S3 bucket
# and everything in it. Each step tolerates the thing already being gone, so a
# teardown after a half-finished bring-up still ends with nothing.
set -euo pipefail

# shellcheck source=deploy/eks/_common.sh
# shellcheck disable=SC1091  # sourced by absolute path resolved at runtime
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

require_tools aws eksctl kubectl helm
resolve_config

log "tearing down cluster ${ORBITA_EKS_CLUSTER}, region ${ORBITA_EKS_REGION}, bucket ${ORBITA_EKS_BUCKET}"

# 1. The release. Not strictly necessary since deleting the cluster takes it too,
# but it releases the load balancer and the volumes cleanly first, which avoids
# orphaned AWS resources that outlive the CloudFormation stack.
if helm status "$ORBITA_EKS_RELEASE" --namespace "$ORBITA_EKS_NAMESPACE" >/dev/null 2>&1; then
  log "uninstalling the orbita release"
  helm uninstall "$ORBITA_EKS_RELEASE" --namespace "$ORBITA_EKS_NAMESPACE" --wait || true
else
  log "no orbita release found, skipping"
fi

# 2. The cluster. eksctl deletes the CloudFormation stacks it created, which is
# the node group, the OIDC provider, the IRSA role, and the VPC. --wait so this
# command does not return before the bill actually stops.
if eksctl get cluster --name "$ORBITA_EKS_CLUSTER" --region "$ORBITA_EKS_REGION" >/dev/null 2>&1; then
  log "deleting cluster ${ORBITA_EKS_CLUSTER} (this takes several minutes)"
  eksctl delete cluster --name "$ORBITA_EKS_CLUSTER" --region "$ORBITA_EKS_REGION" --wait
else
  log "no cluster ${ORBITA_EKS_CLUSTER} found, skipping"
fi

# 3. The bucket. eksctl never owned it, so it has to go separately, and it will
# not delete while it holds objects. Emptying first is what makes teardown mean
# teardown rather than teardown-except-the-storage-bill.
if aws s3api head-bucket --bucket "$ORBITA_EKS_BUCKET" 2>/dev/null; then
  log "emptying and deleting bucket ${ORBITA_EKS_BUCKET}"
  aws s3 rm "s3://$ORBITA_EKS_BUCKET" --recursive || true
  aws s3api delete-bucket --bucket "$ORBITA_EKS_BUCKET" --region "$ORBITA_EKS_REGION"
else
  log "no bucket ${ORBITA_EKS_BUCKET} found, skipping"
fi

log "down. nothing left running."
