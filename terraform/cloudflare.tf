# Cloudflare side: the R2 test bucket and its expiry rule.
#
# Note what is NOT here: the R2 credentials the workflow authenticates with.
# Cloudflare has no equivalent of AWS's OIDC role assumption for R2 — R2's
# S3-compatible API authenticates with an Access Key ID and Secret Access Key
# minted from an "R2 API token", and the Terraform provider exposes no resource
# that returns that key pair. `cloudflare_api_token` produces a token value, not
# the derived S3 access-key/secret pair R2's S3 endpoint wants, so wiring it up
# in Terraform would mean reimplementing Cloudflare's key-derivation by hand and
# storing the result in state. That is worse than a documented manual step, so
# the R2 token stays a scoped secret created in the dashboard. README.md, under
# "Cloudflare R2 credentials", has the exact steps and the reasoning.
#
# The bucket and its lifecycle rule ARE managed here, because those the provider
# supports cleanly.

provider "cloudflare" {
  api_token = var.cloudflare_api_token
}

resource "cloudflare_r2_bucket" "live_test" {
  account_id = var.cloudflare_account_id
  name       = var.r2_bucket_name
  location   = var.r2_bucket_location
}

# The same backstop as the S3 bucket: objects under the test prefix are deleted
# after a day so a failed run does not leave litter behind. R2 expresses this as
# a delete transition keyed on object age in seconds rather than S3's whole
# days, so the day count is converted here.
resource "cloudflare_r2_bucket_lifecycle" "live_test" {
  account_id  = var.cloudflare_account_id
  bucket_name = cloudflare_r2_bucket.live_test.name

  rules = [{
    id      = "expire-test-prefix"
    enabled = true

    conditions = {
      prefix = var.test_prefix
    }

    delete_objects_transition = {
      condition = {
        type    = "Age"
        max_age = var.expire_after_days * 24 * 60 * 60
      }
    }
  }]
}
