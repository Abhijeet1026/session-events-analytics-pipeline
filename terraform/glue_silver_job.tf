resource "aws_s3_object" "silver_transform_script" {
  bucket = aws_s3_bucket.lakehouse.bucket
  key    = "glue/scripts/silver_transform_job.py"
  source = "${path.module}/../glue/scripts/silver_transform_job.py"
  etag   = filemd5("${path.module}/../glue/scripts/silver_transform_job.py")
}

# NEW: upload the helper zip (contains the silver/ package)
resource "aws_s3_object" "silver_deps_zip" {
  bucket = aws_s3_bucket.lakehouse.bucket
  key    = "glue/deps/silver.zip"
  source = "${path.module}/../glue/silver.zip"
  etag   = filemd5("${path.module}/../glue/silver.zip")
}

resource "aws_glue_job" "silver_transform" {
  name     = "session-events-silver-transform-dev"
  role_arn = data.aws_iam_role.glue_role.arn


  glue_version      = "4.0"
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

    # NEW: adds your silver/ modules to PYTHONPATH
    "--extra-py-files" = "s3://${aws_s3_bucket.lakehouse.bucket}/${aws_s3_object.silver_deps_zip.key}"

    # NEW: Iceberg support (Glue Catalog)
    "--conf" = join(" ", [
      "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
      "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog",
      "spark.sql.catalog.glue_catalog.warehouse=s3://${aws_s3_bucket.lakehouse.bucket}/iceberg",
      "spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
      "spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
      "spark.sql.defaultCatalog=glue_catalog"
    ])
  }

  depends_on = [
    aws_s3_object.silver_transform_script,
    aws_s3_object.silver_deps_zip
  ]
}
