# ---------------------------------------------------------------------------------------------------------------------
# S3 DATA LAKE TERRAGRUNT CONFIGURATION — non-prod
# Deploys the data lake S3 bucket (Iceberg warehouse) using community module directly.
# ---------------------------------------------------------------------------------------------------------------------

include "root" {
  path = find_in_parent_folders("root.hcl")
}

include "envcommon" {
  path   = "${dirname(find_in_parent_folders("root.hcl"))}/_envcommon/s3.hcl"
  expose = true
}

inputs = {
  bucket = "lakehouse-at-scale-data-lake"

  tags = {
    Component = "s3-data-lake"
  }
}
