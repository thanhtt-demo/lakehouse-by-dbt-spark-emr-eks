# ---------------------------------------------------------------------------------------------------------------------
# COMMON EMR VIRTUAL CLUSTER CONFIGURATION
# Shared Terragrunt configuration for EMR on EKS Virtual Cluster across all environments.
# Uses terraform-aws-modules/emr/aws//modules/virtual-cluster community submodule directly.
# This creates: EMR Virtual Cluster, Kubernetes RBAC (Role, RoleBinding),
# IAM execution role, and CloudWatch log group.
# https://registry.terraform.io/modules/terraform-aws-modules/emr/aws/latest/submodules/virtual-cluster
# ---------------------------------------------------------------------------------------------------------------------

terraform {
  source = "tfr:///terraform-aws-modules/emr/aws//modules/virtual-cluster?version=3.3.0"
}

# ---------------------------------------------------------------------------------------------------------------------
# Common inputs for the EMR Virtual Cluster community submodule
# Environment-specific terragrunt.hcl can override these values
# ---------------------------------------------------------------------------------------------------------------------
inputs = {
  name = "lakehouse-at-scale-emr-vc"

  # Namespace where Spark jobs will run — managed by ArgoCD, not Terraform
  namespace            = "spark"
  create_namespace     = false

  # IAM execution role for EMR Spark jobs
  create_iam_role      = true
  iam_role_name        = "lakehouse-at-scale-emr-execution"
  iam_role_description = "IAM execution role for EMR on EKS Spark jobs"

  # CloudWatch log group for Spark driver/executor logs
  create_cloudwatch_log_group = true
  cloudwatch_log_group_name   = "/emr-on-eks/lakehouse-at-scale"

  tags = {
    Component = "emr-virtual-cluster"
  }
}
