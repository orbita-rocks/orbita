# CI live object store tests

The `Live object store` workflow checks that AWS S3 and Cloudflare R2 still
enforce the conditional writes Orbita uses to publish partition manifests. A
backend that accepts a stale conditional write can let an old owner replace a
newer manifest, so this is a correctness check against the vendors themselves,
not a compatibility smoke test.

The workflow runs every Monday at 08:00 UTC and on demand. MinIO covers the
same test surface on every pull request; this environment exists for behavior a
local container cannot prove.

## Ownership

The `ci-live` environment is persistent. It is not part of the disposable EKS
test environment and must not be destroyed with it.

The Terraform root in [`terraform/`](../terraform/) owns:

- the AWS S3 test bucket and its lifecycle rule;
- the GitHub Actions IAM role and its prefix-scoped S3 policy;
- the AWS GitHub OIDC provider, when the account does not already have one;
- the Cloudflare R2 test bucket and its lifecycle rule.

GitHub repository settings hold the Terraform outputs and the R2 credentials.
Terraform does not create the R2 credentials because the Cloudflare provider
does not expose the S3-compatible key pair.

Every test object lives under `orbita-it/`. The IAM policy and both lifecycle
rules depend on that prefix, so changing it is a contract change across the
Terraform, workflow, and Rust test.

## Access needed for setup

The first apply needs operator credentials for both providers.

AWS credentials must be able to create an S3 bucket, IAM role and policy, and,
when needed, the account's GitHub OIDC provider. Use AWS SSO, `AWS_PROFILE`, or
an already-assumed administrative role. These credentials are not stored in
GitHub and are not used by the test workflow.

Cloudflare needs an account API token with account-level `Workers R2 Storage:
Edit`. Export it for the apply rather than writing it into a variable file:

```sh
export TF_VAR_cloudflare_api_token=...
```

The examples below use `terraform`. OpenTofu 1.6 or newer is equivalent.

## Bootstrap AWS OIDC

An AWS account can have only one OIDC provider for
`https://token.actions.githubusercontent.com`. Check whether the account already
has one before the first apply.

If the provider does not exist, leave `create_github_oidc_provider` at its
default of `true`. Terraform creates it and records ownership in this state.

If the provider already exists, set this in `terraform.tfvars`:

```hcl
create_github_oidc_provider = false
```

Terraform then reads the shared provider instead of trying to create a
duplicate. This distinction matters at teardown: an OIDC provider shared by
other repositories is account infrastructure, not an Orbita test resource.

AWS OIDC removes standing AWS keys from GitHub. The workflow receives a
short-lived session by assuming the repository-scoped role at run time.

## Create the infrastructure

Create the local variable file from the checked-in example and choose dedicated
bucket names. Neither bucket should contain data for another purpose.

```sh
cd terraform
cp terraform.tfvars.example terraform.tfvars
$EDITOR terraform.tfvars

terraform init
terraform plan -out=ci-live.tfplan
terraform apply ci-live.tfplan
```

Review the plan for one S3 bucket, one workflow role, one R2 bucket, their
lifecycle rules, and the expected OIDC create-or-read behavior. An unexpected
replacement of a bucket, role, or OIDC provider is a reason to stop.

The state is local today because this environment has one operator and changes
rarely. Treat `terraform.tfstate` as the ownership record:

- keep it encrypted and backed up;
- never commit it;
- never run a fresh apply after losing it;
- import the existing resources before making another change if the state is
  unavailable.

Commit the generated `.terraform.lock.hcl`. It pins the provider selections
that produced the reviewed plan. Do not commit `terraform.tfvars` or a saved
plan.

## Configure the repository

Copy the Terraform outputs into the repository settings:

| Source | GitHub setting | Kind |
| --- | --- | --- |
| `aws_role_arn` | `LIVE_S3_ROLE_ARN` | secret |
| `aws_s3_bucket` | `LIVE_S3_BUCKET` | secret |
| `aws_region` | `LIVE_S3_REGION` | variable |
| `r2_bucket` | `LIVE_R2_BUCKET` | secret |

The GitHub CLI keeps this step reproducible:

```sh
gh secret set LIVE_S3_ROLE_ARN --body "$(terraform output -raw aws_role_arn)"
gh secret set LIVE_S3_BUCKET --body "$(terraform output -raw aws_s3_bucket)"
gh variable set LIVE_S3_REGION --body "$(terraform output -raw aws_region)"
gh secret set LIVE_R2_BUCKET --body "$(terraform output -raw r2_bucket)"
```

Run these commands from a checkout whose `origin` is
`orbita-rocks/orbita`, or pass `--repo orbita-rocks/orbita` explicitly.

## Create the R2 workflow credential

In Cloudflare, open `R2`, then `Manage R2 API Tokens`, then create an API token.
Grant `Object Read & Write` on the one `ci-live` bucket and nothing else.

Capture the Access Key ID and Secret Access Key when Cloudflare shows them. Set
the four R2 repository secrets:

```sh
gh secret set LIVE_R2_ACCOUNT_ID
gh secret set LIVE_R2_BUCKET --body "$(terraform output -raw r2_bucket)"
gh secret set LIVE_R2_ACCESS_KEY_ID
gh secret set LIVE_R2_SECRET_ACCESS_KEY
```

The prompts avoid putting secret values in shell history. The Cloudflare API
token used by Terraform and the R2 API token used by the workflow are separate
credentials with separate lifetimes.

## Prove the setup

Dispatch the workflow after the first setup and after any infrastructure,
credential, signing, or conditional-write change:

```sh
gh workflow run live-object-store.yml --ref develop
gh run list --workflow live-object-store.yml --limit 1
```

Both matrix rows must run and pass. A skipped backend is not success. The
workflow deliberately fails when a required repository setting is absent,
because a green run with no live verification is worse than a visible setup
failure.

Check that both buckets have the one-day lifecycle rule on `orbita-it/`. The
tests delete their own objects on success; the lifecycle rule exists for the
failed run that cannot clean up.

## Routine changes

Use this sequence for Terraform changes:

```sh
cd terraform
terraform init
terraform fmt -check
terraform validate
terraform plan -out=ci-live.tfplan
terraform apply ci-live.tfplan
```

Update repository settings only when the corresponding output changes. Run the
workflow manually before considering the maintenance complete.

Provider upgrades should be explicit. Update the lock file, review the provider
changelog, produce a plan with no unexplained replacements, and commit the lock
change with the configuration that needs it.

## Credential rotation

AWS has no scheduled workflow credential to rotate. GitHub mints short-lived
credentials for each run. Rotate the operator's AWS access through the normal
account process; it is independent of `ci-live`.

The Cloudflare Terraform token is needed only when planning or applying the R2
resources. Replace it in the operator's secret manager when it rotates. No
GitHub setting uses it.

Rotate the R2 workflow credential without creating a blind interval:

1. Create a new bucket-scoped `Object Read & Write` R2 token.
2. Replace `LIVE_R2_ACCESS_KEY_ID` and `LIVE_R2_SECRET_ACCESS_KEY` together.
3. Dispatch `live-object-store.yml` and wait for the R2 row to pass.
4. Revoke the old token.

If the validation run fails, restore the old pair before revoking it. Do not
leave an access key from one token paired with the secret from another.

## Failure response

A scheduled failure opens or updates the issue named `Live object store
verification is failing`. Manual runs do not file an issue because the operator
who dispatched one is already present.

Start with the failed matrix row:

| Failure | First check |
| --- | --- |
| Missing repository setting | Compare repository settings with the table above and current Terraform outputs. |
| AWS `AccessDenied` during role assumption | Check the OIDC provider, `id-token: write`, and the role trust conditions for this repository and workflow path. |
| AWS S3 authorization failure | Check the bucket output, `orbita-it/` prefix policy, and region variable. |
| R2 authentication or signature failure | Check the account id, complete access-key pair, `auto` signing region, and bucket scope. |
| Connection or DNS failure | Re-run once, then check vendor status and endpoint construction before changing code. |
| A stale conditional write succeeds | Treat this as a correctness finding. Reproduce it manually, then fix the backend integration or withdraw the support claim. |
| `client error (SendRequest)` after a losing write | Suspect connection reuse after the backend closed a socket. `docs/BUILD.md` records the earlier MinIO version of this failure. |

Do not close the tracking issue because a retry happened to pass. Record whether
the cause was vendor behavior, credentials, network, workflow configuration, or
Orbita, and link the run that established it.

## Periodic audit

Review the environment at least quarterly and before a release:

- The latest scheduled AWS and R2 rows passed.
- The AWS trust policy still names only `orbita-rocks/orbita` and
  `.github/workflows/live-object-store.yml`.
- The AWS role still reaches only `orbita-it/` in the dedicated bucket.
- The R2 token still reaches only the dedicated bucket.
- Both lifecycle rules still expire `orbita-it/` after one day.
- The provider lock file is committed and intentional.
- The state backup is readable by the current operator.
- The workflow failure assignee still matches the object-store code owner.

## Recovery and retirement

If local state is lost, stop before running `apply`. Build an inventory from the
Terraform outputs stored in GitHub and the provider consoles, restore the latest
state backup, and import any missing bucket, IAM, lifecycle, and R2 resources.
The first plan after recovery must show no create, replace, or destroy action for
an existing resource.

Do not use a full `terraform destroy` as routine cleanup. This environment is
persistent, and the OIDC provider may be shared account infrastructure. To
retire the tests:

1. Disable the scheduled workflow and preserve the last successful run.
2. Revoke the R2 workflow token and remove the repository settings.
3. Empty the dedicated test prefixes and confirm neither bucket has another
   purpose.
4. Confirm whether any other IAM role trusts the GitHub OIDC provider.
5. Produce and review a destroy plan from the intact state.
6. Preserve a final state backup and the reason the support check was retired.

If the OIDC provider is shared, it must remain. Change its ownership model before
destroying this root rather than deleting account-wide trust out from under
another repository.

## Scope

These tests prove conditional create and conditional replace behavior over real
AWS S3 and R2 sockets. They do not prove every transport failure, every status
code, general availability, performance, or all S3-compatible providers. A
green run supports the specific manifest-CAS claim and nothing broader.
