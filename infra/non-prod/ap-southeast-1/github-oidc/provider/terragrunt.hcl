# ---------------------------------------------------------------------------------------------------------------------
# GITHUB OIDC PROVIDER — non-prod
# Creates the GitHub Actions OIDC Identity Provider in this AWS account.
# One-time setup per account — allows GitHub Actions to assume IAM roles via OIDC.
# No dependencies — this is a standalone resource.
# ---------------------------------------------------------------------------------------------------------------------

include "root" {
  path = find_in_parent_folders("root.hcl")
}

include "envcommon" {
  path   = "${dirname(find_in_parent_folders("root.hcl"))}/_envcommon/github-oidc-provider.hcl"
  expose = true
}

inputs = {
  tags = {
    Name = "github-actions-oidc"
  }
}
