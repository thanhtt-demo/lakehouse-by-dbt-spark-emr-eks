# ---------------------------------------------------------------------------------------------------------------------
# COMMON KARPENTER CONFIGURATION
# Shared Terragrunt configuration for Karpenter across all environments.
# Uses terraform-aws-modules/eks/aws//modules/karpenter community submodule directly.
# This creates AWS-side resources only (IAM roles, SQS queue, EventBridge rules, access entry).
# Karpenter Helm chart, NodePool CRDs, and EC2NodeClass are managed by ArgoCD (not Terraform).
# https://registry.terraform.io/modules/terraform-aws-modules/eks/aws/latest/submodules/karpenter
# ---------------------------------------------------------------------------------------------------------------------

terraform {
  source = "tfr:///terraform-aws-modules/eks/aws//modules/karpenter?version=21.19.0"
}

# ---------------------------------------------------------------------------------------------------------------------
# Common inputs for the Karpenter community submodule
# Environment-specific terragrunt.hcl can override these values
# ---------------------------------------------------------------------------------------------------------------------
inputs = {
  # Create a dedicated node IAM role for Karpenter-provisioned nodes
  create_node_iam_role          = true
  node_iam_role_use_name_prefix = false
  node_iam_role_name            = "lakehouse-at-scale-karpenter-node"

  # Pod Identity for Karpenter controller (recommended over IRSA in v1.x)
  create_pod_identity_association = true

  # Use inline policy to avoid 6,144 char limit on standard IAM policies
  enable_inline_policy = true

  # Enable Spot termination handling (SQS queue + EventBridge rules)
  enable_spot_termination = true

  # Attach SSM policy for node management
  node_iam_role_additional_policies = {
    AmazonSSMManagedInstanceCore = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
  }
}
