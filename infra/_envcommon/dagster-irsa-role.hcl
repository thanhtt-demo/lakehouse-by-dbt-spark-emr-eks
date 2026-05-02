# ---------------------------------------------------------------------------------------------------------------------
# COMMON DAGSTER IRSA ROLE CONFIGURATION
# Shared Terragrunt configuration for Dagster team IRSA roles.
# Uses terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts community submodule.
# Each live config (de-team-role, sales-team-role) overrides `name`, `oidc_providers`, and `policies`.
# https://registry.terraform.io/modules/terraform-aws-modules/iam/aws/latest/submodules/iam-role-for-service-accounts
# ---------------------------------------------------------------------------------------------------------------------

terraform {
  source = "tfr:///terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts?version=6.6.0"
}

# ---------------------------------------------------------------------------------------------------------------------
# Common inputs — each live config overrides name, oidc_providers, and policies
# ---------------------------------------------------------------------------------------------------------------------
inputs = {}
