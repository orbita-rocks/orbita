# AWS side of the live-object-store test infrastructure: the S3 test bucket, its
# expiry rule, and the OIDC-assumable IAM role that replaces the long-lived
# access key the workflow used to carry.

provider "aws" {
  region = var.aws_region
}

# GitHub's OIDC provider, through which the workflow proves it is this
# repository without a stored secret.
#
# It is account-global: an AWS account can hold exactly one provider per URL. If
# the account already trusts GitHub Actions for something else, set
# create_github_oidc_provider = false and this module reads the existing one
# instead of failing to create a duplicate.
resource "aws_iam_openid_connect_provider" "github" {
  count = var.create_github_oidc_provider ? 1 : 0

  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]

  # AWS now validates the GitHub OIDC endpoint against its own trust store and
  # ignores this list, but the argument is still required. This is GitHub's
  # published intermediate-CA thumbprint, kept so the resource is correct on an
  # older provider or a partition that still checks it.
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]
}

data "aws_iam_openid_connect_provider" "github" {
  count = var.create_github_oidc_provider ? 0 : 1

  url = "https://token.actions.githubusercontent.com"
}

locals {
  github_oidc_provider_arn = (
    var.create_github_oidc_provider
    ? aws_iam_openid_connect_provider.github[0].arn
    : data.aws_iam_openid_connect_provider.github[0].arn
  )
  github_oidc_subject_prefix = coalesce(
    var.github_oidc_subject_prefix,
    "repo:${var.github_owner}/${var.github_repo}",
  )
}

resource "aws_s3_bucket" "live_test" {
  bucket = var.s3_bucket_name
}

# Objects left under the test prefix expire on their own after a day. The tests
# clean up on the happy path; the run that fails is the run that leaves an
# object behind, and also the one you least want to be doing bucket housekeeping
# during.
resource "aws_s3_bucket_lifecycle_configuration" "live_test" {
  bucket = aws_s3_bucket.live_test.id

  rule {
    id     = "expire-test-prefix"
    status = "Enabled"

    filter {
      prefix = var.test_prefix
    }

    expiration {
      days = var.expire_after_days
    }
  }
}

# The trust policy is where the security of the whole OIDC arrangement lives:
# anyone who can make GitHub mint a token matching these conditions can assume
# the role. So it is scoped twice.
data "aws_iam_policy_document" "github_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [local.github_oidc_provider_arn]
    }

    # The audience AWS itself expects for configure-aws-credentials.
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    # The subject pins the principal to this repository, on any ref.
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["${local.github_oidc_subject_prefix}:*"]
    }

    # job_workflow_ref pins it further to this one workflow file, so a different
    # workflow added to the same repository cannot assume this role by accident.
    # The @* leaves the ref open on purpose: the schedule runs on the default
    # branch and a manual dispatch could run on any, and neither is a weaker
    # control than the workflow-path pin above it.
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:job_workflow_ref"
      values   = ["${var.github_owner}/${var.github_repo}/${var.workflow_ref_path}@*"]
    }
  }
}

resource "aws_iam_role" "live_object_store" {
  name               = var.iam_role_name
  description        = "Assumed by the live-object-store GitHub Actions workflow via OIDC to run conditional-write tests against the S3 test bucket."
  assume_role_policy = data.aws_iam_policy_document.github_trust.json
}

# The least-privilege policy the PR documented, expressed as Terraform. There is
# no s3:* here and there should not be: this role's blast radius is one prefix
# in one bucket.
data "aws_iam_policy_document" "s3_test_access" {
  # ListBucket is a bucket-level action, so it cannot be resource-scoped to a
  # prefix directly; the s3:prefix condition is how it is confined to the test
  # keys.
  statement {
    sid       = "ListOnlyTheTestPrefix"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.live_test.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${var.test_prefix}*"]
    }
  }

  # GetObject covers HeadObject and ranged reads; conditional PUT needs nothing
  # beyond PutObject.
  statement {
    sid    = "ObjectsUnderTheTestPrefix"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = ["${aws_s3_bucket.live_test.arn}/${var.test_prefix}*"]
  }
}

resource "aws_iam_role_policy" "s3_test_access" {
  name   = "s3-test-prefix"
  role   = aws_iam_role.live_object_store.id
  policy = data.aws_iam_policy_document.s3_test_access.json
}
