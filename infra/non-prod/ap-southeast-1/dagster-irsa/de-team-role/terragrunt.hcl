# ---------------------------------------------------------------------------------------------------------------------
# DAGSTER DE-TEAM IRSA ROLE — non-prod
# IRSA role for dagster:dagster-de-team service account.
# Dependencies: EKS (OIDC provider), de-team policy
# ---------------------------------------------------------------------------------------------------------------------

include "root" {
  path = find_in_parent_folders("root.hcl")
}

include "envcommon" {
  path   = "${dirname(find_in_parent_folders("root.hcl"))}/_envcommon/dagster-irsa-role.hcl"
  expose = true
}

dependency "eks" {
  config_path = "../../eks"

  mock_outputs = {
    cluster_name      = "mock-eks-cluster"
    oidc_provider_arn = "arn:aws:iam::123456789012:oidc-provider/oidc.eks.ap-southeast-1.amazonaws.com/id/MOCK"
  }
  mock_outputs_allowed_terraform_commands = ["init", "validate", "plan", "destroy"]
}

dependency "policy" {
  config_path = "../de-team-policy"

  mock_outputs = {
    arn = "arn:aws:iam::123456789012:policy/mock-dagster-de-team"
  }
  mock_outputs_allowed_terraform_commands = ["init", "validate", "plan", "destroy"]
}

dependency "sales_policy" {
  config_path = "../sales-team-policy"

  mock_outputs = {
    arn = "arn:aws:iam::123456789012:policy/mock-dagster-sales-team"
  }
  mock_outputs_allowed_terraform_commands = ["init", "validate", "plan", "destroy"]
}

inputs = {
  name = "lakehouse-at-scale-dagster-de-team"

  force_detach_policies = true

  oidc_providers = {
    this = {
      provider_arn               = dependency.eks.outputs.oidc_provider_arn
      namespace_service_accounts = ["dagster:dagster-user-deployments", "dagster:dagster"]
    }
  }

  policies = {
    dagster-de-team    = dependency.policy.outputs.arn
    dagster-sales-team = dependency.sales_policy.outputs.arn
  }
}
