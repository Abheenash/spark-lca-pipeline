# EMR Serverless: no cluster to size, no instances to keep warm, and it releases
# capacity when the job ends. For a pipeline that runs when DOL publishes — a
# handful of times a year — a persistent EMR cluster would idle at full price
# between runs.
#
# NOT APPLIED. An EMR Serverless application costs nothing while idle, but any
# actual run bills per vCPU-second, and the data this processes is public DOL
# disclosure files that run to millions of rows. The job's behaviour is proven by
# running it locally instead — see the README's measured numbers.

data "aws_caller_identity" "current" {}

resource "aws_s3_bucket" "data" {
  bucket = "${var.name_prefix}-data-${data.aws_caller_identity.current.account_id}"
}

resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket                  = aws_s3_bucket.data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    id     = "expire-quarantine"
    status = "Enabled"
    filter { prefix = "quarantine/" }
    # Rejected rows are for diagnosing a bad feed, not for keeping. 90 days is
    # long enough to notice a pattern and short enough not to accumulate.
    expiration { days = 90 }
  }

  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload { days_after_initiation = 7 }
  }
}

resource "aws_emrserverless_application" "spark" {
  name          = "${var.name_prefix}-pipeline"
  release_label = "emr-7.5.0"
  type          = "SPARK"

  # Idle capacity is released after 15 minutes. Without this the application
  # holds pre-initialised workers — faster starts, billed continuously.
  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }

  # No pre-initialised capacity: this job runs a few times a year, so a cold
  # start of a minute or two is a trade worth making for zero idle cost.
  auto_start_configuration {
    enabled = true
  }

  maximum_capacity {
    cpu    = "${var.max_vcpu} vCPU"
    memory = "${var.max_vcpu * 4} GB"
  }
}

data "aws_iam_policy_document" "job_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
    # Without this confused-deputy guard, any EMR Serverless application in any
    # account could assume this role.
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_iam_role" "job" {
  name               = "${var.name_prefix}-job"
  assume_role_policy = data.aws_iam_policy_document.job_assume.json
}

# The job reads raw/ and writes the three output prefixes. It cannot read or
# write anything else in the bucket, and cannot touch another bucket at all.
data "aws_iam_policy_document" "job" {
  statement {
    sid       = "ListOnlyThisBucket"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.data.arn]
  }

  statement {
    sid       = "ReadRawInput"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.data.arn}/raw/*", "${aws_s3_bucket.data.arn}/code/*"]
  }

  statement {
    sid     = "WriteOutputsOnly"
    actions = ["s3:PutObject", "s3:DeleteObject"]
    resources = [
      "${aws_s3_bucket.data.arn}/certified/*",
      "${aws_s3_bucket.data.arn}/employer_summary/*",
      "${aws_s3_bucket.data.arn}/quarantine/*",
      "${aws_s3_bucket.data.arn}/logs/*",
    ]
  }
}

resource "aws_iam_role_policy" "job" {
  name   = "${var.name_prefix}-job"
  role   = aws_iam_role.job.id
  policy = data.aws_iam_policy_document.job.json
}

resource "aws_cloudwatch_log_group" "jobs" {
  name              = "/aws/emr-serverless/${var.name_prefix}"
  retention_in_days = 30
}
