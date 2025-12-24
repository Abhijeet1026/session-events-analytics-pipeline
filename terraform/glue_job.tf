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
    # Glue runtime
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--enable-job-insights"              = "true"
    "--TempDir"                          = "s3://${aws_s3_bucket.lakehouse.bucket}/glue_temp/"

    "--extra-py-files" = "s3://${aws_s3_bucket.lakehouse.bucket}/glue_scripts/ingestion.zip"

    # Script args (REQUIRED)
    "--S3_BUCKET" = aws_s3_bucket.lakehouse.bucket
    "--S3_PREFIX" = "raw/session_events"

    # IMPORTANT: full endpoint including /generate
    "--API_URL" = "${aws_apigatewayv2_api.events_http_api.api_endpoint}/generate"

    # Tuning (OPTIONAL but recommended)
    "--COUNT"        = "10000"
    "--PLATFORM"     = "web"
    "--NUM_BATCHES"  = "5"
    "--TIMEOUT_SECS" = "30"
    "--MAX_RETRIES"  = "3"
  }

  depends_on = [aws_s3_object.glue_scripts]
}
