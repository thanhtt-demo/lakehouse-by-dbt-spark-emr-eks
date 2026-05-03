# ---------------------------------------------------------------------------------------------------------------------
# GLUE TERRAGRUNT CONFIGURATION — non-prod
# Deploys Glue Data Catalog databases (staging, intermediate, marts) using local Glue module.
# Dependencies: S3 (data lake bucket name for location URI)
# ---------------------------------------------------------------------------------------------------------------------

# Include the root config (remote state, provider generation)
include "root" {
  path = find_in_parent_folders("root.hcl")
}

# Include the shared Glue configuration from _envcommon
include "envcommon" {
  path   = "${dirname(find_in_parent_folders("root.hcl"))}/_envcommon/glue.hcl"
  expose = true
}

# Glue depends on S3 data lake for bucket name (database location URI)
dependency "s3_data_lake" {
  config_path = "../s3/data-lake"

  mock_outputs = {
    s3_bucket_arn = "arn:aws:s3:::mock-data-lake"
    s3_bucket_id  = "mock-data-lake"
  }
  mock_outputs_allowed_terraform_commands = ["init", "validate", "plan", "destroy"]
}

# Pass S3 outputs to the Glue module
inputs = {
  data_lake_bucket = dependency.s3_data_lake.outputs.s3_bucket_id
}
