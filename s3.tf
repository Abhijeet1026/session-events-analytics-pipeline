# ----------------------------
# S3 bucket for Glue scripts / data
# ----------------------------

# Main S3 bucket that we will use for Glue scripts and/or data
resource "aws_s3_bucket" "lakehouse" {
  # Bucket name must be globally unique across ALL AWS accounts,
  # so we combine project name + environment to reduce conflicts.
  bucket = "${var.project_name}-${var.environment}-glue-bucket"

  tags = {
    Name = "${var.project_name}-lakehouse"
  }

}

# Enable versioning for data protection
resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  
  versioning_configuration {
    status = "Enabled"
  }
}

# Block all public access

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  # Link this config to the bucket defined above
  bucket = aws_s3_bucket.lakehouse.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
