# ---------------------------------------------------------------------------------------------------------------------
# ARGOCD MODULE
# Installs ArgoCD on EKS via Helm and bootstraps the App-of-Apps Application CRD.
# This eliminates manual steps — `terragrunt apply` handles everything.
# ---------------------------------------------------------------------------------------------------------------------

terraform {
  required_version = ">= 1.0"

  required_providers {
    helm = {
      source  = "hashicorp/helm"
      version = ">= 3.0"
    }
    kubectl = {
      source  = "gavinbunney/kubectl"
      version = ">= 1.14"
    }
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

# ---------------------------------------------------------------------------------------------------------------------
# ARGOCD HELM RELEASE
# Installs ArgoCD using the official Helm chart into the argocd namespace.
# ---------------------------------------------------------------------------------------------------------------------

resource "helm_release" "argocd" {
  name             = "argocd"
  repository       = "oci://ghcr.io/argoproj/argo-helm"
  chart            = "argo-cd"
  version          = var.argocd_chart_version
  namespace        = "argocd"
  create_namespace = true

  values = [
    yamlencode({
      server = {
        service = {
          type = var.server_service_type
        }
      }
      dex = {
        enabled = false
      }
    })
  ]

  wait    = true
  timeout = 600
}

# ---------------------------------------------------------------------------------------------------------------------
# BOOTSTRAP APP-OF-APPS
# Creates the root ArgoCD Application that manages all child applications.
# ArgoCD deploys everything in sync-wave order: namespaces → karpenter → dagster.
# ---------------------------------------------------------------------------------------------------------------------

resource "kubectl_manifest" "app_of_apps" {
  depends_on = [helm_release.argocd]

  yaml_body = yamlencode({
    apiVersion = "argoproj.io/v1alpha1"
    kind       = "Application"
    metadata = {
      name      = "app-of-apps"
      namespace = "argocd"
      finalizers = ["resources-finalizer.argocd.argoproj.io"]
    }
    spec = {
      project = "default"
      source = {
        repoURL        = var.argocd_repo_url
        targetRevision = var.argocd_repo_revision
        path           = var.argocd_apps_path
      }
      destination = {
        server    = "https://kubernetes.default.svc"
        namespace = "argocd"
      }
      syncPolicy = {
        automated = {
          prune    = true
          selfHeal = true
        }
        syncOptions = [
          "CreateNamespace=false",
          "ApplyOutOfSyncOnly=true",
        ]
        retry = {
          limit = 3
          backoff = {
            duration   = "30s"
            factor     = 2
            maxDuration = "3m"
          }
        }
      }
    }
  })
}
