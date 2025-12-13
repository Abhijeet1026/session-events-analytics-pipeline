data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "glue_role" {
  name               = "${var.project_name}-glue-role-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
}

data "aws_iam_policy_document" "glue_logs" {
  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "glue_logs" {
  name   = "${var.project_name}-glue-logs-${var.environment}"
  policy = data.aws_iam_policy_document.glue_logs.json
}

resource "aws_iam_role_policy_attachment" "glue_logs" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.glue_logs.arn
}

data "aws_iam_policy_document" "glue_s3" {
  statement {
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetObject",
      "s3:PutObject"
    ]
    resources = [
      aws_s3_bucket.lakehouse.arn,
      "${aws_s3_bucket.lakehouse.arn}/*"
    ]
  }
}

resource "aws_iam_policy" "glue_s3" {
  name   = "${var.project_name}-glue-s3-${var.environment}"
  policy = data.aws_iam_policy_document.glue_s3.json
}

resource "aws_iam_role_policy_attachment" "glue_s3" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.glue_s3.arn
}
