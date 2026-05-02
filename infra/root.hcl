# ---------------------------------------------------------------------------------------------------------------------
# TERRAGRUNT ROOT CONFIGURATION
# This is the root Terragrunt configuration that all other Terragrunt configurations inherit from.
# It configures remote state storage (S3 + native state locking) and generates the AWS provider block.
# Pattern: https://github.com/thanhtt-demo/terragrunt-ecs-msk-connect-stask
# ---------------------------------------------------------------------------------------------------------------------

locals {
  # Automatically load account-level and region-level variables
  account_vars = read_terragrunt_config(find_in_parent_folders("env.hcl"))

  aws_account_id = local.account_vars.locals.aws_account_id
  aws_profile    = local.account_vars.locals.aws_profile
  aws_region     = local.account_vars.locals.aws_region
}

# ---------------------------------------------------------------------------------------------------------------------
# REMOTE STATE CONFIGURATION
# S3 bucket for state storage with native S3 state locking (use_lockfile)
# Terragrunt auto-creates the S3 bucket on first run if it doesn't exist.
# ---------------------------------------------------------------------------------------------------------------------
remote_state {
  backend = "s3"
  config = {
    encrypt      = true
    bucket       = "lakehouse-at-scale-tfstate-${local.aws_account_id}"
    key          = "${path_relative_to_include()}/terraform.tfstate"
    region       = local.aws_region
    profile      = local.aws_profile
    use_lockfile = true
  }
  generate = {
    path      = "backend.tf"
    if_exists = "overwrite_terragrunt"
  }
}

# ---------------------------------------------------------------------------------------------------------------------
# PROVIDER GENERATION
# Generate the AWS provider block for all child modules
# ---------------------------------------------------------------------------------------------------------------------
generate "provider" {
  path      = "provider.tf"
  if_exists = "overwrite_terragrunt"
  contents  = <<EOF
provider "aws" {
  region  = "${local.aws_region}"
  profile = "${local.aws_profile}"

  default_tags {
    tags = {
      ManagedBy   = "terragrunt"
      Project     = "lakehouse-at-scale"
      Environment = "non-prod"
    }
  }
}
EOF
}

# ---------------------------------------------------------------------------------------------------------------------
# GLOBAL INPUTS
# Inputs that are common to all child modules
# ---------------------------------------------------------------------------------------------------------------------
inputs = {
  aws_account_id = local.aws_account_id
  aws_region     = local.aws_region
}
