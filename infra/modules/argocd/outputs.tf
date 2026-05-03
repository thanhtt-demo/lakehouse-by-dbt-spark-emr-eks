# ---------------------------------------------------------------------------------------------------------------------
# ARGOCD MODULE OUTPUTS
# ---------------------------------------------------------------------------------------------------------------------

output "argocd_namespace" {
  description = "Namespace where ArgoCD is installed"
  value       = helm_release.argocd.namespace
}

output "argocd_chart_version" {
  description = "Installed ArgoCD Helm chart version"
  value       = helm_release.argocd.version
}
