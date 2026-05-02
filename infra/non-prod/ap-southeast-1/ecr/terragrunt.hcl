# ---------------------------------------------------------------------------------------------------------------------
# ECR TERRAGRUNT CONFIGURATION — non-prod
# Deploys ECR repositories (de-team-base/code, sales-team-base/code) using local wrapper module.
# No dependencies — ECR repositories are standalone resources.
# ---------------------------------------------------------------------------------------------------------------------

# Include the root config (remote state, provider generation)
include "root" {
  path = find_in_parent_folders("root.hcl")
}

# Include the shared ECR configuration from _envcommon
include "envcommon" {
  path   = "${dirname(find_in_parent_folders("root.hcl"))}/_envcommon/ecr.hcl"
  expose = true
}
