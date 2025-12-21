# -----------------------------------
# Input variables for this Terraform project
# -----------------------------------

# AWS region where resources will be created (e.g. ca-central-1, us-east-1, etc.)
variable "aws_region" {
  type        = string
  description = "AWS region to deploy resources into"
}

# Name of the environment (e.g. dev, test, prod)
variable "environment" {
  type        = string
  description = "Deployment environment (dev, test, prod, etc.)"
}

# Logical project name used for naming and tagging resources
variable "project_name" {
  type        = string
  description = "Project name used in resource names and tags"
}

variable "lambda_execution_role_name" {
  type        = string
  description = "Existing IAM role name for Lambda execution"
}

variable "glue_job_name" {
  type        = string
  description = "Existing Glue job name to start"
}
