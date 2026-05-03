# ---------------------------------------------------------------------------------------------------------------------
# ARGOCD MODULE VARIABLES
# ---------------------------------------------------------------------------------------------------------------------

variable "argocd_chart_version" {
  description = "Version of the ArgoCD Helm chart"
  type        = string
  default     = "7.8.13"
}

variable "server_service_type" {
  description = "Kubernetes service type for ArgoCD server (LoadBalancer or ClusterIP)"
  type        = string
  default     = "ClusterIP"
}

variable "argocd_repo_url" {
  description = "Git repository URL for ArgoCD App-of-Apps"
  type        = string
}

variable "argocd_repo_revision" {
  description = "Git branch/tag/commit for ArgoCD App-of-Apps"
  type        = string
  default     = "main"
}

variable "argocd_apps_path" {
  description = "Path to the App-of-Apps Helm chart in the Git repository"
  type        = string
  default     = "argocd/apps"
}

variable "tags" {
  description = "Additional tags for resources"
  type        = map(string)
  default     = {}
}
