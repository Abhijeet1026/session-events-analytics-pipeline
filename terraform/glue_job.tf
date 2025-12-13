resource "aws_glue_job" "api_to_s3" {
  name     = "${var.project_name}-api-to-s3-${var.environment}"
  role_arn = aws_iam_role.glue_role.arn

  command {
    name            = "batch_ingestion_job"
    python_version  = "3.9"
    script_location = "s3://${aws_s3_bucket.lakehouse.bucket}/glue_scripts/data_ingestion_job.py"
  }

  max_retries = 0
  timeout     = 10

  default_arguments = {
    "--S3_BUCKET" = aws_s3_bucket.lakehouse.bucket
    "--S3_PREFIX" = "raw/session_events"
  }

  depends_on = [aws_s3_object.glue_scripts]
}
