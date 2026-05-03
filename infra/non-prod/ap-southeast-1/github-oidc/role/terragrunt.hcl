# ---------------------------------------------------------------------------------------------------------------------
# GITHUB OIDC ROLE — non-prod
# IAM role for GitHub Actions CI/CD pipeline.
# Trusts GitHub OIDC provider, restricted to main branch of the application code repo.
# Grants ECR push permissions for all lakehouse-at-scale/* repositories.
# Dependencies: GitHub OIDC Provider, ECR (for repository ARNs)
# ---------------------------------------------------------------------------------------------------------------------

include "root" {
  path = find_in_parent_folders("root.hcl")
}

include "envcommon" {
  path   = "${dirname(find_in_parent_folders("root.hcl"))}/_envcommon/github-oidc-role.hcl"
  expose = true
}

dependency "ecr" {
  config_path = "../../ecr"

  mock_outputs = {
    repository_arns = {
      "de-team-base"    = "arn:aws:ecr:ap-southeast-1:123456789012:repository/lakehouse-at-scale/de-team-base"
      "de-team-code"    = "arn:aws:ecr:ap-southeast-1:123456789012:repository/lakehouse-at-scale/de-team-code"
      "sales-team-base" = "arn:aws:ecr:ap-southeast-1:123456789012:repository/lakehouse-at-scale/sales-team-base"
      "sales-team-code" = "arn:aws:ecr:ap-southeast-1:123456789012:repository/lakehouse-at-scale/sales-team-code"
    }
  }
  mock_outputs_allowed_terraform_commands = ["init", "validate", "plan", "destroy"]
}

# ---------------------------------------------------------------------------------------------------------------------
# IMPORTANT: Update the oidc_subjects to match your GitHub org/repo.
# Format: "repo:<GITHUB_ORG>/<REPO_NAME>:ref:refs/heads/<BRANCH>"
# ---------------------------------------------------------------------------------------------------------------------
inputs = {
  name = "lakehouse-at-scale-github-actions"

  enable_github_oidc = true

  # Restrict to main branch of the application code repo
  oidc_subjects = [
    "repo:thanhtt-demo/lakehouse-by-dbt-spark-emr-eks:ref:refs/heads/main",
  ]

  # ECR push permissions — inline policy
  create_inline_policy      = true
  inline_policy_permissions = {
    ecr_auth = {
      sid    = "ECRAuth"
      effect = "Allow"
      actions = [
        "ecr:GetAuthorizationToken",
      ]
      resources = ["*"]
    }
    ecr_push = {
      sid    = "ECRPush"
      effect = "Allow"
      actions = [
        "ecr:BatchCheckLayerAvailability",
        "ecr:GetDownloadUrlForLayer",
        "ecr:BatchGetImage",
        "ecr:PutImage",
        "ecr:InitiateLayerUpload",
        "ecr:UploadLayerPart",
        "ecr:CompleteLayerUpload",
      ]
      resources = values(dependency.ecr.outputs.repository_arns)
    }
  }

  tags = {
    Name = "lakehouse-at-scale-github-actions"
  }
}
