# ---------------------------------------------------------------------------------------------------------------------
# COMMON ECR CONFIGURATION
# Shared Terragrunt configuration for ECR repositories across all environments.
# Uses local wrapper module that calls terraform-aws-modules/ecr/aws community module
# for each repository (de-team-base, de-team-code, sales-team-base, sales-team-code).
# ---------------------------------------------------------------------------------------------------------------------

terraform {
  source = "${dirname(find_in_parent_folders("root.hcl"))}//modules/ecr"
}

# ---------------------------------------------------------------------------------------------------------------------
# Common inputs for the ECR wrapper module
# Environment-specific terragrunt.hcl can override these values
# ---------------------------------------------------------------------------------------------------------------------
inputs = {
  name = "lakehouse-at-scale"

  repository_names = [
    "de-team-base",
    "de-team-code",
    "sales-team-base",
    "sales-team-code",
  ]

  image_tag_mutability = "MUTABLE"
  max_image_count      = 30
}
