# Package lambda code into a zip
data "archive_file" "start_glue_zip" {
  type        = "zip"
  source_file = "${path.module}/../lambda/start_glue_job.py"
  output_path = "${path.module}/../lambda/start_glue_job.zip"
}

# Use an EXISTING IAM role for Lambda execution
# (best for you because your terraform user has tight permissions boundaries)
data "aws_iam_role" "lambda_exec" {
  name = var.lambda_execution_role_name
}

resource "aws_lambda_function" "start_glue" {
  function_name = "${var.project_name}-start-glue-${var.environment}"
  role          = data.aws_iam_role.lambda_exec.arn

  filename         = data.archive_file.start_glue_zip.output_path
  source_code_hash = data.archive_file.start_glue_zip.output_base64sha256

  handler = "start_glue_job.lambda_handler"
  runtime = "python3.11"

  timeout     = 30
  memory_size = 256

  environment {
    variables = {
      GLUE_JOB_NAME = var.glue_job_name
      S3_BUCKET     = aws_s3_bucket.lakehouse.bucket
      BASE_PREFIX   = "lakehouse/user_events"
    }
  }
  depends_on = [aws_s3_bucket.lakehouse]
}
