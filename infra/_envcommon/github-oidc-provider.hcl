# ---------------------------------------------------------------------------------------------------------------------
# COMMON GITHUB OIDC PROVIDER CONFIGURATION
# Shared Terragrunt configuration for GitHub Actions OIDC Identity Provider.
# Uses terraform-aws-modules/iam/aws//modules/iam-oidc-provider community submodule.
# Creates the OIDC provider once per AWS account — GitHub Actions can then assume IAM roles via OIDC.
# https://registry.terraform.io/modules/terraform-aws-modules/iam/aws/latest/submodules/iam-oidc-provider
# ---------------------------------------------------------------------------------------------------------------------

terraform {
  source = "tfr:///terraform-aws-modules/iam/aws//modules/iam-oidc-provider?version=6.6.0"
}

# ---------------------------------------------------------------------------------------------------------------------
# Common inputs
# ---------------------------------------------------------------------------------------------------------------------
inputs = {
  url = "https://token.actions.githubusercontent.com"
}
