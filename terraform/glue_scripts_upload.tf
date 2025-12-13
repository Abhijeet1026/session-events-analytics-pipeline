# Upload all Glue scripts from local glue_scripts/ to S3 under glue/scripts/

locals {
  glue_scripts_dir = "${path.module}/../glue/scripts"
  glue_scripts     = fileset(local.glue_scripts_dir, "*.py")
}

resource "aws_s3_object" "glue_scripts" {
  for_each = toset(local.glue_scripts)

  bucket = aws_s3_bucket.lakehouse.bucket
  key    = "glue/scripts/${each.value}"
  source = "${local.glue_scripts_dir}/${each.value}"

  # Re-upload when script content changes
  etag = filemd5("${local.glue_scripts_dir}/${each.value}")
}
