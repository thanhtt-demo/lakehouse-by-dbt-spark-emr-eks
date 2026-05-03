# ---------------------------------------------------------------------------------------------------------------------
# COMMON EKS CONFIGURATION
# Shared Terragrunt configuration for EKS across all environments.
# Uses terraform-aws-modules/eks/aws community module directly (no local wrapper).
# https://registry.terraform.io/modules/terraform-aws-modules/eks/aws/latest
# ---------------------------------------------------------------------------------------------------------------------

terraform {
  source = "tfr:///terraform-aws-modules/eks/aws?version=21.19.0"
}

# ---------------------------------------------------------------------------------------------------------------------
# Common inputs for the EKS community module
# Environment-specific terragrunt.hcl can override these values
# ---------------------------------------------------------------------------------------------------------------------
inputs = {
  name               = "lakehouse-at-scale-eks"
  kubernetes_version = "1.35"

  # Cluster access
  endpoint_public_access  = true
  endpoint_private_access = true

  # Give Terraform identity admin access to cluster
  enable_cluster_creator_admin_permissions = true

  # IRSA
  enable_irsa = true

  # Cluster addons
  addons = {
    coredns = {
      most_recent = true
    }
    kube-proxy = {
      most_recent = true
    }
    vpc-cni = {
      most_recent    = true
      before_compute = true
    }
    eks-pod-identity-agent = {
      most_recent    = true
      before_compute = true
    }
    aws-ebs-csi-driver = {
      most_recent = true
      configuration_values = jsonencode({
        controller = {
          nodeSelector = {
            "role" = "system"
          }
        }
      })
    }
  }

  # Managed node group for system workloads (Dagster, ArgoCD, Karpenter controller)
  eks_managed_node_groups = {
    system = {
      ami_type       = "AL2023_x86_64_STANDARD"
      instance_types = ["t3.medium"]

      min_size     = 1
      max_size     = 3
      desired_size = 2

      labels = {
        role                      = "system"
        "karpenter.sh/controller" = "true"
      }

      iam_role_additional_policies = {
        AmazonEBSCSIDriverPolicy = "arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
      }

      # Allow pods to access IMDS (required for EBS CSI driver and other AWS SDK calls)
      metadata_options = {
        http_endpoint               = "enabled"
        http_tokens                 = "required"
        http_put_response_hop_limit = 2
      }
    }
  }

  # Tag node security group for Karpenter discovery
  node_security_group_tags = {
    "karpenter.sh/discovery" = "lakehouse-at-scale-eks"
  }
}
