# Provider and Terraform version pins for the disposable EKS test cluster.
#
# This mirrors the pattern set by terraform/ (the live-object-store role): pin
# the major, floor the Terraform version, and say why in comments. Works under
# either `terraform` or `tofu`; the bring-up script picks whichever is present.

terraform {
  # A floor, not a pin, matching terraform/versions.tf. The EKS and VPC modules
  # below carry their own tighter constraints, so pinning an exact CLI version
  # here would only break an operator on a newer release for no gain.
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source = "hashicorp/aws"
      # 5.x is the current major and what terraform/ already uses. The EKS and
      # VPC modules are built against it.
      version = "~> 5.0"
    }
  }

  # State is deliberately local, the same call terraform/versions.tf makes and
  # for the same reasons: one operator applies a disposable cluster at a time,
  # the state is small and short-lived, and a cluster whose whole purpose is to
  # be destroyed does not earn a remote backend. `terraform destroy` is the
  # normal end of this state's life, not a migration.
}

# The region every resource lives in. Read from the same ORBITA_EKS_REGION the
# scripts and the Helm overlay use, passed through as a variable.
provider "aws" {
  region = var.region

  default_tags {
    tags = local.tags
  }
}
