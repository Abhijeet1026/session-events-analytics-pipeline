resource "aws_s3_object" "watermark_silver_session_events" {
  bucket       = aws_s3_bucket.lakehouse.bucket
  key          = "watermarks/silver_session_events.json"
  content      = jsonencode({ last_processed_event_ts = "1970-01-01T00:00:00Z" })
  content_type = "application/json"

  lifecycle {
    ignore_changes = [content]
  }
}
