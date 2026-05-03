# ---------------------------------------------------------------------------------------------------------------------
# COMMON DOCKER IMAGE CONFIGURATION
# Shared Terragrunt configuration for building and pushing Base Images to ECR.
# Uses local docker-image module (null_resource + local-exec) to build and push.
# Each live config provides team-specific inputs (dockerfile_path, ecr_repository_url, docker_context).
# ---------------------------------------------------------------------------------------------------------------------

terraform {
  source = "${dirname(find_in_parent_folders("root.hcl"))}//modules/docker-image"
}

# ---------------------------------------------------------------------------------------------------------------------
# Common inputs — overridden by each live config
# ---------------------------------------------------------------------------------------------------------------------
inputs = {
  image_tag = "latest"
}
