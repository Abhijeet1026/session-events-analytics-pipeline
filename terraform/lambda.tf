# ---------------------------------------------
# Lambda: Generate fake user events (Faker)
# Uses lambda_package.zip built by GitHub Actions
# ---------------------------------------------

# Use an EXISTING IAM role for Lambda execution
data "aws_iam_role" "lambda_exec" {
  name = var.lambda_execution_role_name
}

resource "aws_lambda_function" "generate_events" {
  function_name = "${var.project_name}-generate-events-${var.environment}"
  role          = data.aws_iam_role.lambda_exec.arn

  # Zip is built in GitHub Actions at repo root
  filename         = "${path.module}/../lambda_package.zip"
  source_code_hash = filebase64sha256("${path.module}/../lambda_package.zip")

  runtime = "python3.11"
  handler = "generate_events.lambda_handler"

  timeout     = 30
  memory_size = 256

  environment {
    variables = {
      SITE_HOST = "example-telecom.com"
    }
  }

  depends_on = [aws_s3_bucket.lakehouse]
}
