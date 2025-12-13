data "aws_iam_role" "glue_role" {
  name = "IAM_Glue"
}

resource "aws_glue_job" "api_to_s3" {
  name     = "${var.project_name}-api-to-s3-${var.environment}"
  role_arn = data.aws_iam_role.glue_role.arn

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.lakehouse.bucket}/glue_scripts/data_ingestion_job.py"
  }

  max_retries = 0
  timeout     = 480

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"

    # TempDir is commonly required for Spark jobs
    "--TempDir" = "s3://${aws_s3_bucket.lakehouse.bucket}/glue_temp/"

    # Your script args (getResolvedOptions reads these)
    "--S3_BUCKET" = aws_s3_bucket.lakehouse.bucket
    "--S3_PREFIX" = "raw/session_events"
  }

  depends_on = [aws_s3_object.glue_scripts]
}
