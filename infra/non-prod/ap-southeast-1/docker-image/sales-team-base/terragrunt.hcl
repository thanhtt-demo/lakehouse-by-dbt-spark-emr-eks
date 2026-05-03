# ---------------------------------------------------------------------------------------------------------------------
# DOCKER IMAGE TERRAGRUNT CONFIGURATION — sales-team-base (non-prod)
# Builds and pushes the sales-team Base Image to ECR.
# Dependencies: ECR (repository URL for sales-team-base)
# ---------------------------------------------------------------------------------------------------------------------

# Include the root config (remote state, provider generation)
include "root" {
  path = find_in_parent_folders("root.hcl")
}

# Include the shared docker-image configuration from _envcommon
include "envcommon" {
  path   = "${dirname(find_in_parent_folders("root.hcl"))}/_envcommon/docker-image.hcl"
  expose = true
}

# Docker image depends on ECR for the repository URL
dependency "ecr" {
  config_path = "../../ecr"

  mock_outputs = {
    repository_urls = {
      "de-team-base"    = "560503716668.dkr.ecr.ap-southeast-1.amazonaws.com/lakehouse-at-scale/de-team-base"
      "de-team-code"    = "560503716668.dkr.ecr.ap-southeast-1.amazonaws.com/lakehouse-at-scale/de-team-code"
      "sales-team-base" = "560503716668.dkr.ecr.ap-southeast-1.amazonaws.com/lakehouse-at-scale/sales-team-base"
      "sales-team-code" = "560503716668.dkr.ecr.ap-southeast-1.amazonaws.com/lakehouse-at-scale/sales-team-code"
    }
  }
  mock_outputs_allowed_terraform_commands = ["init", "validate", "plan", "destroy"]
}

# Pass team-specific inputs
inputs = {
  name                = "lakehouse-at-scale-sales-team-base"
  dockerfile_path     = "${dirname(find_in_parent_folders("root.hcl"))}/../dbt-dagster-project/sales-team/Dockerfile.base"
  docker_context      = "${dirname(find_in_parent_folders("root.hcl"))}/../dbt-dagster-project/sales-team"
  ecr_repository_url  = dependency.ecr.outputs.repository_urls["sales-team-base"]
}
