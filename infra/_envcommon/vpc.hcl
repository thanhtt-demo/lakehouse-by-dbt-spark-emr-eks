# ---------------------------------------------------------------------------------------------------------------------
# COMMON VPC CONFIGURATION
# Shared Terragrunt configuration for the VPC module across all environments.
# This file is included by environment-specific terragrunt.hcl files.
# ---------------------------------------------------------------------------------------------------------------------

terraform {
  source = "${dirname(find_in_parent_folders("root.hcl"))}//modules/vpc"
}

# ---------------------------------------------------------------------------------------------------------------------
# Common inputs for the VPC module
# Environment-specific terragrunt.hcl can override these values
# ---------------------------------------------------------------------------------------------------------------------
inputs = {
  vpc_cidr = "10.0.0.0/16"

  availability_zones = [
    "ap-southeast-1a",
    "ap-southeast-1b",
    "ap-southeast-1c",
  ]

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

  name       = "lakehouse-at-scale"
  aws_region = "ap-southeast-1"
}
