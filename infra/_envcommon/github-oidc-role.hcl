# ---------------------------------------------------------------------------------------------------------------------
# COMMON GITHUB OIDC ROLE CONFIGURATION
# Shared Terragrunt configuration for GitHub Actions CI/CD IAM role.
# Uses terraform-aws-modules/iam/aws//modules/iam-role community submodule with enable_github_oidc.
# The role trusts GitHub OIDC provider and grants ECR push permissions for CI/CD pipeline.
# https://registry.terraform.io/modules/terraform-aws-modules/iam/aws/latest/submodules/iam-role
# ---------------------------------------------------------------------------------------------------------------------

terraform {
  source = "tfr:///terraform-aws-modules/iam/aws//modules/iam-role?version=6.6.0"
}

# ---------------------------------------------------------------------------------------------------------------------
# Common inputs — live config overrides oidc_subjects and ECR resource ARNs
# ---------------------------------------------------------------------------------------------------------------------
inputs = {
  enable_github_oidc = true
}
