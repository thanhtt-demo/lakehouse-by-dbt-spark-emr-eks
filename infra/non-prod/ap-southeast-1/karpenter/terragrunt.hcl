# ---------------------------------------------------------------------------------------------------------------------
# KARPENTER TERRAGRUNT CONFIGURATION — non-prod
# Deploys Karpenter AWS resources (IAM, SQS, EventBridge) using community submodule.
# Karpenter Helm chart, NodePool CRDs, and EC2NodeClass are managed by ArgoCD (not Terraform).
# ---------------------------------------------------------------------------------------------------------------------

# Include the root config (remote state, provider generation)
include "root" {
  path = find_in_parent_folders("root.hcl")
}

# Include the shared Karpenter configuration from _envcommon
include "envcommon" {
  path   = "${dirname(find_in_parent_folders("root.hcl"))}/_envcommon/karpenter.hcl"
  expose = true
}

# Karpenter depends on EKS for cluster name
dependency "eks" {
  config_path = "../eks"

  mock_outputs = {
    cluster_name = "mock-eks-cluster"
  }
  mock_outputs_allowed_terraform_commands = ["init", "validate", "plan", "destroy"]
}

# Pass EKS cluster name to the Karpenter submodule
inputs = {
  cluster_name = dependency.eks.outputs.cluster_name
}
