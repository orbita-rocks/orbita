# Provider and Terraform version pins for the live-object-store test
# infrastructure.
#
# This is the first Terraform in the repository, so it also sets the pattern:
# small, single-purpose modules that provision exactly what a workflow needs and
# say why in comments, the same culture the rest of this repo's config follows.

terraform {
  # A floor, not a pin. Anything from 1.6 up understands the syntax here. The CI
  # that would enforce an exact version does not exist yet, and pinning one
  # before it does would just break an operator on a slightly newer release for
  # no gain.
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source = "hashicorp/aws"
      # 5.x is the current major. The S3 lifecycle and IAM resources used here
      # have been stable across the whole line, so a major-version ceiling is
      # enough and a tighter pin would only add upgrade churn.
      version = "~> 5.0"
    }
    cloudflare = {
      source = "cloudflare/cloudflare"
      # R2 buckets and their lifecycle rules are the reason this is v5: the
      # `cloudflare_r2_bucket_lifecycle` resource does not exist in v4.
      version = "~> 5.0"
    }
  }

  # State is deliberately local for now.
  #
  # This module provisions CI test infrastructure for one repository: one S3
  # bucket, one IAM role, one R2 bucket. The state is small, it changes rarely,
  # and exactly one operator applies it at a time, so a local state file is
  # honest about the current scale and avoids bootstrapping a remote backend
  # before there is anything to put in it.
  #
  # The intended remote, once a second operator needs to apply this, is an S3
  # backend with a DynamoDB lock table. The S3 bucket this module already
  # creates is a fine home for that state. Moving there is a `backend "s3"`
  # block here plus a `terraform init -migrate-state`; nothing about the
  # resources below has to change.
}
