# ---------------------------------------------------------------------------------------------------------------------
# S3 SPARK LOGS TERRAGRUNT CONFIGURATION — non-prod
# Deploys the Spark job logs S3 bucket using community module directly.
# ---------------------------------------------------------------------------------------------------------------------

include "root" {
  path = find_in_parent_folders("root.hcl")
}

include "envcommon" {
  path   = "${dirname(find_in_parent_folders("root.hcl"))}/_envcommon/s3.hcl"
  expose = true
}

inputs = {
  bucket = "lakehouse-at-scale-spark-logs"

  tags = {
    Component = "s3-spark-logs"
  }
}
