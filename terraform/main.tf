# ----------------------------
# Terraform configuration block
# ----------------------------
terraform {
  # Minimum version of Terraform required
  required_version = ">= 1.0"

  # Providers required by this project
  required_providers {
    aws = {
      # Source location of the AWS provider
      source = "hashicorp/aws"

      # Accept any AWS provider version in the 5.x series
      version = "~> 5.0"
    }
  }
}

# ----------------------------
# AWS Provider configuration
# ----------------------------
provider "aws" {
  # Use the region value supplied through variables.tf or dev.tfvars
  region = var.aws_region

  # Default tags applied automatically to ALL AWS resources created by Terraform
  default_tags {
    tags = {
      # Group resources under the project name
      Project = var.project_name

      # Specify environment (dev, test, prod, etc.)
      Environment = var.environment

      # Helps identify provisioning source
      ManagedBy = "Terraform"
    }
  }
}
