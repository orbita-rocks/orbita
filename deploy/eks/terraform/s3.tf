# The object store bucket and the least-privilege policy the workload role
# carries against it.

resource "aws_s3_bucket" "data" {
  bucket = var.bucket_name

  # force_destroy is what makes `terraform destroy` mean it. A bucket with
  # objects in it refuses to delete, and this cluster's whole point is to write
  # objects, so without this teardown would stop at the bucket and leave the one
  # resource that keeps costing money. A test bucket holds nothing worth the
  # safety of refusing.
  force_destroy = true
}

# No public access, ever. A test bucket is still a bucket, and a world-readable
# one is the same mistake at any scale.
resource "aws_s3_bucket_public_access_block" "data" {
  bucket = aws_s3_bucket.data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# A backstop, not the teardown. `terraform destroy` empties and removes the
# bucket; this only bounds the damage of a cluster left running past its
# welcome, so a forgotten test does not accrue storage forever.
resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    id     = "expire-test-data"
    status = "Enabled"

    filter {}

    expiration {
      days = var.object_expiration_days
    }
  }
}

# The policy the workload role assumes: list the one bucket, and read, write,
# and delete objects in it. List is what the object-store client's
# ListObjectsV2 needs; the object verbs are a write, a read, a manifest CAS, and
# compaction reclamation. No s3:*, no second bucket.
data "aws_iam_policy_document" "orbita_s3" {
  statement {
    sid       = "ListTheTestBucket"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.data.arn]
  }

  statement {
    sid       = "ReadWriteObjects"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.data.arn}/*"]
  }
}

resource "aws_iam_policy" "orbita_s3" {
  name        = "${var.irsa_role_name}-access"
  description = "Bucket-scoped object access for the Orbita test cluster's workload role."
  policy      = data.aws_iam_policy_document.orbita_s3.json
}
