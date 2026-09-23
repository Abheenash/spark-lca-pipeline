# Native terraform tests against a mocked provider — no EMR application, no cost.

mock_provider "aws" {
  override_data {
    target = data.aws_caller_identity.current
    values = { account_id = "111122223333" }
  }
  override_data {
    target = data.aws_iam_policy_document.job_assume
    values = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
  }
  override_data {
    target = data.aws_iam_policy_document.job
    values = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
  }
}

run "idle_capacity_is_released" {
  command = plan

  # EMR Serverless bills per vCPU-second. An application that keeps
  # pre-initialised workers is faster to start and billed continuously — the
  # wrong trade for a job that runs a few times a year.
  assert {
    condition     = aws_emrserverless_application.spark.auto_stop_configuration[0].enabled
    error_message = "Auto-stop must be enabled, or idle capacity bills indefinitely."
  }

  assert {
    condition     = aws_emrserverless_application.spark.auto_stop_configuration[0].idle_timeout_minutes <= 30
    error_message = "Idle timeout should be short — this is the cost guardrail."
  }
}

run "rejects_an_unbounded_capacity_ceiling" {
  command = plan

  variables {
    max_vcpu = 512
  }

  expect_failures = [var.max_vcpu]
}

run "the_data_bucket_is_private_and_versioned" {
  command = plan

  assert {
    condition = alltrue([
      aws_s3_bucket_public_access_block.data.block_public_acls,
      aws_s3_bucket_public_access_block.data.block_public_policy,
      aws_s3_bucket_public_access_block.data.ignore_public_acls,
      aws_s3_bucket_public_access_block.data.restrict_public_buckets,
    ])
    error_message = "The data bucket must block every public-access vector."
  }

  assert {
    condition     = aws_s3_bucket_versioning.data.versioning_configuration[0].status == "Enabled"
    error_message = "A job that writes with mode=overwrite needs versioning, or a bad run destroys the previous output with no way back."
  }
}

run "quarantine_output_expires" {
  command = plan

  # Rejected rows exist to diagnose a bad feed, not to accumulate forever.
  assert {
    condition = anytrue([
      for r in aws_s3_bucket_lifecycle_configuration.data.rule :
      r.id == "expire-quarantine" && r.status == "Enabled"
    ])
    error_message = "Quarantined rows must expire on a lifecycle rule."
  }
}
