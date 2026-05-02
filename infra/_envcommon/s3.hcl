# ---------------------------------------------------------------------------------------------------------------------
# COMMON S3 CONFIGURATION
# Shared Terragrunt configuration for S3 buckets across all environments.
# Uses terraform-aws-modules/s3-bucket/aws community module directly (no local wrapper).
# Each live config overrides `bucket` name via inputs.
# https://registry.terraform.io/modules/terraform-aws-modules/s3-bucket/aws/latest
# ---------------------------------------------------------------------------------------------------------------------

terraform {
  source = "tfr:///terraform-aws-modules/s3-bucket/aws?version=5.12.0"
}

# ---------------------------------------------------------------------------------------------------------------------
# Common inputs — shared across all S3 buckets
# Each live config (data-lake, pipes, spark-logs) overrides `bucket` name
# ---------------------------------------------------------------------------------------------------------------------
inputs = {
  versioning = {
    enabled = true
  }

  server_side_encryption_configuration = {
    rule = {
      apply_server_side_encryption_by_default = {
        sse_algorithm = "AES256"
      }
    }
  }

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
