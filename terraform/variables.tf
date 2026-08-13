# Inputs. Everything an operator has to decide is here; the resource files read
# these and nothing else, so a second environment is a second tfvars file rather
# than an edit to the module.

variable "aws_region" {
  description = "Region the AWS S3 test bucket lives in. This must match the LIVE_S3_REGION repository variable the workflow reads, because the endpoint is derived from it on both sides."
  type        = string
}

variable "s3_bucket_name" {
  description = "Name of the AWS S3 test bucket. Dedicate it to this and nothing else; the workflow writes only under the test prefix, but the bucket's blast radius should still be one purpose."
  type        = string
}

variable "iam_role_name" {
  description = "Name of the IAM role the workflow assumes via OIDC."
  type        = string
  default     = "orbita-live-object-store"
}

variable "github_owner" {
  description = "GitHub org that owns the repository. Half of the OIDC subject claim the role's trust policy is scoped to."
  type        = string
  default     = "orbita-rocks"
}

variable "github_repo" {
  description = "Repository name. The other half of the OIDC subject claim."
  type        = string
  default     = "orbita"
}

variable "github_oidc_subject_prefix" {
  description = "GitHub OIDC subject before the ref or environment suffix. Leave null for repo:<owner>/<repo>; set it when GitHub reports an immutable subject containing owner and repository ids."
  type        = string
  default     = null
}

variable "workflow_ref_path" {
  description = "Path to the workflow allowed to assume the role, matched against the OIDC job_workflow_ref claim. Pinning this means a new workflow added to the same repository cannot assume the role by accident."
  type        = string
  default     = ".github/workflows/live-object-store.yml"
}

variable "create_github_oidc_provider" {
  description = "Whether to create the account's GitHub OIDC provider. An AWS account can hold only one provider per URL, so set this to false if the account already trusts token.actions.githubusercontent.com for something else, and the module will look the existing one up instead of colliding with it."
  type        = bool
  default     = true
}

variable "test_prefix" {
  description = "Key prefix every test object is written under. The IAM policy and both lifecycle rules are scoped to it. It must match the prefix the tests use in crates/orbita-objectstore/tests/minio.rs, which is orbita-it/."
  type        = string
  default     = "orbita-it/"
}

variable "expire_after_days" {
  description = "How long an object under the test prefix survives before the lifecycle rule deletes it. The tests clean up after themselves on the happy path; this is the backstop for the run that fails and leaves an object behind."
  type        = number
  default     = 1
}

variable "cloudflare_account_id" {
  description = "Cloudflare account id that owns the R2 bucket. It is also in the R2 endpoint hostname, which is why the workflow keeps it as a secret."
  type        = string
}

variable "cloudflare_api_token" {
  description = "Cloudflare API token used to create the R2 bucket and its lifecycle rule. It needs account-level Workers R2 Storage: Edit. This is a bootstrap credential for Terraform, not the credential the workflow uses; see README.md."
  type        = string
  sensitive   = true
}

variable "r2_bucket_name" {
  description = "Name of the Cloudflare R2 test bucket. Like the S3 one, dedicate it to this."
  type        = string
}

variable "r2_bucket_location" {
  description = "Location hint for the R2 bucket. Honored only at creation and best-effort even then. Null lets Cloudflare choose. Valid values: apac, eeur, enam, weur, wnam, oc."
  type        = string
  default     = null
}
