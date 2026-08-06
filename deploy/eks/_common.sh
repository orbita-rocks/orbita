#!/usr/bin/env bash
# Shared configuration and helpers for the EKS test-cluster scripts.
#
# Sourced by up.sh, down.sh, and smoke.sh so the three cannot disagree about
# which cluster, region, bucket, or namespace they are operating on. It sets no
# strict-mode flags of its own; each script owns `set -euo pipefail`.
#
# Configuration comes from the environment, with the same three values the docs
# and the overlay use. Region and bucket are required; the rest have defaults so
# a normal run needs only two exports.
#
#   ORBITA_EKS_REGION     (required) region for the cluster and the bucket
#   ORBITA_EKS_BUCKET     (required) S3 bucket name, created by up.sh
#   ORBITA_EKS_CLUSTER    (default: orbita-test) must match cluster.yaml
#   ORBITA_EKS_NAMESPACE  (default: orbita) Kubernetes namespace for the release
#   ORBITA_EKS_RELEASE    (default: orbita) Helm release name

# The directory this library lives in, so a script can find its siblings
# regardless of the caller's working directory.
EKS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export EKS_DIR

log() { printf '\033[1;34m==>\033[0m %s\n' "$*" >&2; }
die() { printf '\033[1;31merror:\033[0m %s\n' "$*" >&2; exit 1; }

require_tools() {
  local missing=()
  local tool
  for tool in "$@"; do
    command -v "$tool" >/dev/null 2>&1 || missing+=("$tool")
  done
  if [ ${#missing[@]} -gt 0 ]; then
    die "missing required tools: ${missing[*]}"
  fi
}

resolve_config() {
  : "${ORBITA_EKS_REGION:?set ORBITA_EKS_REGION to the region for the cluster and bucket}"
  : "${ORBITA_EKS_BUCKET:?set ORBITA_EKS_BUCKET to the S3 bucket name}"
  export ORBITA_EKS_CLUSTER="${ORBITA_EKS_CLUSTER:-orbita-test}"
  export ORBITA_EKS_NAMESPACE="${ORBITA_EKS_NAMESPACE:-orbita}"
  export ORBITA_EKS_RELEASE="${ORBITA_EKS_RELEASE:-orbita}"

  # Derived, not asked for: the account id is whatever the current AWS
  # credentials belong to. Asking for it invites a mismatch between the id in
  # the role ARN and the account eksctl actually builds in.
  if [ -z "${ORBITA_EKS_ACCOUNT_ID:-}" ]; then
    ORBITA_EKS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)" \
      || die "could not read the AWS account id; is 'aws' configured?"
    export ORBITA_EKS_ACCOUNT_ID
  fi
}
