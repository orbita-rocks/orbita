# Outputs an operator copies into repository secrets and variables after an
# apply. The workflow reads these names; keeping the mapping in one place is why
# they are outputs and not something to fish out of the AWS console.

output "aws_role_arn" {
  description = "ARN of the IAM role the workflow assumes. Store as the LIVE_S3_ROLE_ARN repository secret."
  value       = aws_iam_role.live_object_store.arn
}

output "aws_s3_bucket" {
  description = "Name of the S3 test bucket. Store as the LIVE_S3_BUCKET repository secret."
  value       = aws_s3_bucket.live_test.bucket
}

output "aws_region" {
  description = "Region the S3 bucket lives in. Store as the LIVE_S3_REGION repository variable."
  value       = var.aws_region
}

output "github_oidc_provider_arn" {
  description = "ARN of the GitHub OIDC provider the role trusts. Informational; nothing in the workflow reads it."
  value       = local.github_oidc_provider_arn
}

output "r2_bucket" {
  description = "Name of the R2 test bucket. Store as the LIVE_R2_BUCKET repository secret."
  value       = cloudflare_r2_bucket.live_test.name
}
