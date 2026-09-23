output "application_id" {
  value = aws_emrserverless_application.spark.id
}

output "job_role_arn" {
  value = aws_iam_role.job.arn
}

output "data_bucket" {
  value = aws_s3_bucket.data.id
}
