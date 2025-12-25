resource "aws_s3_object" "silver_transform_script" {
  bucket = aws_s3_bucket.lakehouse.bucket
  key    = "glue/scripts/silver_transform_job.py"
  source = "${path.module}/../glue/scripts/silver_transform_job.py"
  etag   = filemd5("${path.module}/../glue/scripts/silver_transform_job.py")
}

resource "aws_s3_object" "silver_deps_zip" {
  bucket = aws_s3_bucket.lakehouse.bucket
  key    = "glue/deps/silver.zip"
  source = "${path.module}/../glue/silver.zip"
  etag   = filemd5("${path.module}/../glue/silver.zip")
}

resource "aws_glue_job" "silver_transform" {
  name     = "session-events-lakehouse-silver-transform-dev"
  role_arn = data.aws_iam_role.glue_role.arn

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.lakehouse.bucket}/${aws_s3_object.silver_transform_script.key}"
  }

  default_arguments = {
    "--job-language"  = "python"
    "--S3_BUCKET"     = aws_s3_bucket.lakehouse.bucket
    "--RAW_PREFIX"    = "raw/session_events/"
    "--WATERMARK_KEY" = "watermarks/silver_session_events.json"
    "--LOOKBACK_DAYS" = "3"

    "--extra-py-files" = "s3://${aws_s3_bucket.lakehouse.bucket}/${aws_s3_object.silver_deps_zip.key}"


    "--datalake-formats" = "iceberg"
  }

  depends_on = [
    aws_s3_object.silver_transform_script,
    aws_s3_object.silver_deps_zip
  ]
}
