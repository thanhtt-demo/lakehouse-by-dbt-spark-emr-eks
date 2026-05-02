# ---------------------------------------------------------------------------------------------------------------------
# COMMON GLUE CONFIGURATION
# Shared Terragrunt configuration for Glue Data Catalog databases across all environments.
# Uses local module to create Glue databases for dbt schemas (staging, intermediate, marts).
# ---------------------------------------------------------------------------------------------------------------------

terraform {
  source = "${dirname(find_in_parent_folders("root.hcl"))}//modules/glue"
}

# ---------------------------------------------------------------------------------------------------------------------
# Common inputs for the Glue module
# Environment-specific terragrunt.hcl can override these values
# ---------------------------------------------------------------------------------------------------------------------
inputs = {
  name = "lakehouse-at-scale"

  database_names = [
    "staging",
    "intermediate",
    "marts",
  ]
}
