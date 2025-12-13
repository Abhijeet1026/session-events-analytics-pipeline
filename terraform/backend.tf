terraform {
  backend "s3" {
    bucket         = "session-events-tfstat-lakehouse-dev"
    key            = "dev/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "terraform-locks"
    encrypt        = true
  }
}
