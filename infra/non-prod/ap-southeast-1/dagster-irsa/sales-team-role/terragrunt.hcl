# ---------------------------------------------------------------------------------------------------------------------
# DAGSTER SALES-TEAM IRSA ROLE — non-prod
# IRSA role for dagster:dagster-sales-team service account.
# Dependencies: EKS (OIDC provider), sales-team policy
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
  mock_outputs_allowed_terraform_commands = ["init", "validate", "plan"]
}

dependency "policy" {
  config_path = "../sales-team-policy"

  mock_outputs = {
    arn = "arn:aws:iam::123456789012:policy/mock-dagster-sales-team"
  }
  mock_outputs_allowed_terraform_commands = ["init", "validate", "plan"]
}

inputs = {
  name = "lakehouse-at-scale-dagster-sales-team"

  oidc_providers = {
    this = {
      provider_arn               = dependency.eks.outputs.oidc_provider_arn
      namespace_service_accounts = ["dagster:dagster-sales-team"]
    }
  }

  policies = {
    dagster-sales-team = dependency.policy.outputs.arn
  }
}
