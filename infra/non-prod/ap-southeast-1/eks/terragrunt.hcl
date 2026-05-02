# ---------------------------------------------------------------------------------------------------------------------
# EKS TERRAGRUNT CONFIGURATION — non-prod
# Deploys the EKS community module for the non-prod environment.
# Uses terraform-aws-modules/eks/aws directly (no local wrapper module).
# ---------------------------------------------------------------------------------------------------------------------

# Include the root config (remote state, provider generation)
include "root" {
  path = find_in_parent_folders("root.hcl")
}

# Include the shared EKS configuration from _envcommon
include "envcommon" {
  path   = "${dirname(find_in_parent_folders("root.hcl"))}/_envcommon/eks.hcl"
  expose = true
}

# EKS depends on VPC for networking (vpc_id, subnet_ids)
dependency "vpc" {
  config_path = "../vpc"

  # Mock outputs allow `terragrunt plan` to succeed before VPC is applied
  mock_outputs = {
    vpc_id             = "vpc-mock-12345"
    private_subnet_ids = ["subnet-mock-1", "subnet-mock-2", "subnet-mock-3"]
  }
  mock_outputs_allowed_terraform_commands = ["init", "validate", "plan"]
}

# Pass VPC outputs as inputs to the EKS module
inputs = {
  vpc_id     = dependency.vpc.outputs.vpc_id
  subnet_ids = dependency.vpc.outputs.private_subnet_ids
}
