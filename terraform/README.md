# Terraform: live object store test infrastructure

This module provisions everything the `.github/workflows/live-object-store.yml`
workflow needs to run its weekly conditional-write tests against real backends:
an AWS S3 test bucket, the IAM the workflow assumes through GitHub OIDC, and a
Cloudflare R2 test bucket. Without it, standing this verification up is a pile of
console clicks nobody can review, and the IAM policy that keeps a leaked
credential to one bucket prefix exists only in a doc. Here it is code, so it is
reviewable and repeatable.

This is the first Terraform in the repository. It sets the pattern on purpose:
one small module, state that is honest about its scale, and comments that
explain why rather than what.

The operator runbook lives in
[`docs/CI-LIVE-TESTS.md`](../docs/CI-LIVE-TESTS.md). Use it for first setup,
state recovery, credential rotation, failure response, and retirement. This
README explains the Terraform root itself.

## What it creates

- An S3 bucket, with a lifecycle rule that deletes objects under `orbita-it/`
  after a day. The tests clean up after themselves on the happy path; the rule
  is the backstop for the run that fails and leaves an object behind.
- A GitHub Actions OIDC provider in the AWS account, and an IAM role the
  workflow assumes. The role trusts only this repository and, tighter, only this
  one workflow file. Its policy reaches no further than `s3:GetObject`,
  `s3:PutObject`, `s3:DeleteObject`, and a prefix-scoped `s3:ListBucket` on the
  `orbita-it/` prefix. There is no long-lived AWS key anywhere.
- An R2 bucket, with the same one-day expiry on the same prefix.

## The one decision worth reading: OIDC for AWS, tokens for R2

AWS is on GitHub OIDC. The workflow assumes an IAM role at run time and gets
short-lived credentials, so there is no standing AWS secret to leak or rotate.
This replaces the old `LIVE_S3_ACCESS_KEY_ID` and `LIVE_S3_SECRET_ACCESS_KEY`
secrets outright.

R2 is not, because it cannot be. I looked for an AWS-style federated role
Cloudflare would let a GitHub token assume, and R2 has no such thing: its
S3-compatible API authenticates with an Access Key ID and Secret Access Key
derived from an R2 API token, and there is no OIDC path to mint those at run
time. So R2 stays on a scoped, long-lived API token kept as a repository secret.
Keeping it honest matters more than making both backends look uniform: pretending
R2 has OIDC would mean inventing a flow that does not exist.

That split also shows up in what Terraform manages. It creates the R2 bucket and
its lifecycle rule, which the provider supports cleanly. It does not create the
R2 credentials, because the provider exposes no resource that returns the
S3-compatible key pair R2's endpoint wants. That token is a manual step, below.

## Before you can apply

Terraform needs credentials for both providers to exist first. This is the
bootstrapping the issue asked to document, and it is unavoidable: something has
to create the first credentials before Terraform can create the rest.

- **AWS.** An account, and credentials for the initial apply with permission to
  create S3 buckets, IAM roles and policies, and the OIDC provider. The usual
  AWS provider chain supplies them: `AWS_PROFILE`, environment variables, or an
  already-assumed role. These are your operator credentials, used once at apply
  time. They are not what the workflow uses; the workflow uses the role this
  creates.
- **Cloudflare.** An account, its account id, and an API token with account-level
  `Workers R2 Storage: Edit`. Pass it as `TF_VAR_cloudflare_api_token` in the
  environment rather than writing it to a file. This token is for Terraform, not
  the workflow.

## Applying it

```sh
cd terraform
cp terraform.tfvars.example terraform.tfvars   # then edit
export TF_VAR_cloudflare_api_token=...          # rather than putting it in the file

terraform init
terraform plan
terraform apply
```

State is a local file to start. That is a deliberate choice for a module this
small with one operator; `versions.tf` explains it and names the S3 backend to
move to when a second operator needs in. Commit the generated
`.terraform.lock.hcl` after the first `init` so provider versions are pinned for
everyone; do not commit `terraform.tfstate` or `terraform.tfvars`, which the
`.gitignore` here already blocks.

## After you apply: repository secrets and variables

`terraform output` gives you most of what the workflow reads. The mapping:

| Terraform output / value | Repository setting | Kind |
| --- | --- | --- |
| `aws_role_arn` | `LIVE_S3_ROLE_ARN` | secret |
| `aws_s3_bucket` | `LIVE_S3_BUCKET` | secret |
| `aws_region` | `LIVE_S3_REGION` | variable |
| `r2_bucket` | `LIVE_R2_BUCKET` | secret |

The region is a variable, not a secret, because GitHub masks secret values in
logs and masking a string as common as `us-east-1` would redact unrelated output
and make a failure harder to read. The rest are secrets because they name or
authenticate to an account.

The R2 credentials are not outputs, because Terraform does not create them. Set
these four by hand:

| Repository secret | Where it comes from |
| --- | --- |
| `LIVE_R2_ACCOUNT_ID` | Your Cloudflare account id. Also in the R2 endpoint hostname, which is why it is a secret. |
| `LIVE_R2_BUCKET` | The `r2_bucket` output, or the name you set. |
| `LIVE_R2_ACCESS_KEY_ID` | From the R2 API token below. |
| `LIVE_R2_SECRET_ACCESS_KEY` | From the same token. |

### Cloudflare R2 credentials

Create these in the Cloudflare dashboard under **R2 > Manage R2 API Tokens >
Create API token**. Scope the token to **Object Read & Write** on the one test
bucket, nothing broader. Creating it yields an Access Key ID and a Secret Access
Key; those are `LIVE_R2_ACCESS_KEY_ID` and `LIVE_R2_SECRET_ACCESS_KEY`. The
secret is shown once, so capture it then.

This is the manual step OIDC removed on the AWS side and could not remove here.
If Cloudflare later ships federated credentials for R2, this is the piece to
revisit.

## What secrets remain after this

OIDC removed the two standing AWS keys. What is left:

- Removed: `LIVE_S3_ACCESS_KEY_ID`, `LIVE_S3_SECRET_ACCESS_KEY`.
- Added: `LIVE_S3_ROLE_ARN` (the role the workflow assumes).
- Unchanged: `LIVE_S3_REGION`, `LIVE_S3_BUCKET`, and all four R2 settings, which
  stay because R2 has no OIDC to move them to.
