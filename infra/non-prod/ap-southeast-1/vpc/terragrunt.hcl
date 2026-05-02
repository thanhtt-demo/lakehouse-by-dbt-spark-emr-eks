# ---------------------------------------------------------------------------------------------------------------------
# VPC TERRAGRUNT CONFIGURATION — non-prod
# Deploys the VPC module for the non-prod environment.
# Includes root config (remote state, provider) and shared VPC config from _envcommon.
# ---------------------------------------------------------------------------------------------------------------------

# Include the root config (remote state, provider generation)
include "root" {
  path = find_in_parent_folders("root.hcl")
}

# Include the shared VPC configuration from _envcommon
include "envcommon" {
  path   = "${dirname(find_in_parent_folders("root.hcl"))}/_envcommon/vpc.hcl"
  expose = true
}

# Environment-specific overrides (if needed)
# Uncomment and modify to override values from _envcommon/vpc.hcl
inputs = {
  vpc_cidr = "10.0.0.0/16"

  private_subnet_cidrs = [
    "10.0.1.0/24",
    "10.0.2.0/24",
    "10.0.3.0/24",
  ]

  public_subnet_cidrs = [
    "10.0.101.0/24",
    "10.0.102.0/24",
    "10.0.103.0/24",
  ]
}
