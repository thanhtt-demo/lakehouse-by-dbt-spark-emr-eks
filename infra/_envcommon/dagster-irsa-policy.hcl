# ---------------------------------------------------------------------------------------------------------------------
# COMMON DAGSTER IRSA POLICY CONFIGURATION
# Shared Terragrunt configuration for Dagster team IAM policies.
# Uses terraform-aws-modules/iam/aws//modules/iam-policy community submodule.
# Each live config (de-team-policy, sales-team-policy) overrides `name` and `policy` via inputs.
# https://registry.terraform.io/modules/terraform-aws-modules/iam/aws/latest/submodules/iam-policy
# ---------------------------------------------------------------------------------------------------------------------

terraform {
  source = "tfr:///terraform-aws-modules/iam/aws//modules/iam-policy?version=6.6.0"
}

# ---------------------------------------------------------------------------------------------------------------------
# Common inputs — each live config overrides name, description, and policy document
# ---------------------------------------------------------------------------------------------------------------------
inputs = {}
