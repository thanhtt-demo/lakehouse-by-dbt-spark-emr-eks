# ---------------------------------------------------------------------------------------------------------------------
# ARGOCD TERRAGRUNT CONFIGURATION — non-prod
# Installs ArgoCD on EKS and bootstraps the App-of-Apps Application.
# Requires EKS cluster to be running — generates helm and kubectl providers from EKS outputs.
# ---------------------------------------------------------------------------------------------------------------------

# Include the root config (remote state, provider generation)
include "root" {
  path = find_in_parent_folders("root.hcl")
}

# Include the shared ArgoCD configuration from _envcommon
include "envcommon" {
  path   = "${dirname(find_in_parent_folders("root.hcl"))}/_envcommon/argocd.hcl"
  expose = true
}

# ArgoCD depends on EKS cluster
dependency "eks" {
  config_path = "../eks"

  mock_outputs = {
    cluster_endpoint                   = "https://mock-eks-endpoint.example.com"
    cluster_certificate_authority_data = "bW9jay1jZXJ0LWRhdGE="
    cluster_name                       = "lakehouse-at-scale-eks"
  }
  mock_outputs_allowed_terraform_commands = ["init", "validate", "plan", "destroy"]
}

# ---------------------------------------------------------------------------------------------------------------------
# GENERATE HELM + KUBECTL PROVIDERS
# These providers need EKS cluster credentials to deploy into the cluster.
# Uses data source for EKS token to avoid exec block compatibility issues.
# ---------------------------------------------------------------------------------------------------------------------
generate "k8s_providers" {
  path      = "k8s_providers.tf"
  if_exists = "overwrite_terragrunt"
  contents  = <<EOF
data "aws_eks_cluster_auth" "this" {
  name = "${dependency.eks.outputs.cluster_name}"
}

provider "helm" {
  kubernetes = {
    host                   = "${dependency.eks.outputs.cluster_endpoint}"
    cluster_ca_certificate = base64decode("${dependency.eks.outputs.cluster_certificate_authority_data}")
    token                  = data.aws_eks_cluster_auth.this.token
  }
}

provider "kubectl" {
  host                   = "${dependency.eks.outputs.cluster_endpoint}"
  cluster_ca_certificate = base64decode("${dependency.eks.outputs.cluster_certificate_authority_data}")
  token                  = data.aws_eks_cluster_auth.this.token
  load_config_file       = false
}
EOF
}
