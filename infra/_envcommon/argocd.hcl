# ---------------------------------------------------------------------------------------------------------------------
# COMMON ARGOCD CONFIGURATION
# Shared Terragrunt configuration for ArgoCD across all environments.
# Uses local module infra/modules/argocd/ — installs ArgoCD Helm chart + bootstraps App-of-Apps.
# Requires helm and kubectl providers configured to talk to the EKS cluster.
# ---------------------------------------------------------------------------------------------------------------------

terraform {
  source = "${dirname(find_in_parent_folders("root.hcl"))}/modules/argocd/"
}

# ---------------------------------------------------------------------------------------------------------------------
# Common inputs for the ArgoCD module
# Environment-specific terragrunt.hcl can override these values
# ---------------------------------------------------------------------------------------------------------------------
inputs = {
  argocd_chart_version = "7.8.13"
  server_service_type  = "ClusterIP"
  argocd_repo_url      = "https://github.com/thanhtt-demo/lakehouse-by-dbt-spark-emr-eks.git"
  argocd_repo_revision = "main"
  argocd_apps_path     = "argocd/apps"
}
