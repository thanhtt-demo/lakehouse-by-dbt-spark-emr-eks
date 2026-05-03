# ---------------------------------------------------------------------------------------------------------------------
# EMR VIRTUAL CLUSTER TERRAGRUNT CONFIGURATION — non-prod
# Deploys EMR on EKS Virtual Cluster using community submodule.
# Includes Kubernetes RBAC, IAM execution role, and CloudWatch log group.
# Dependencies: EKS (cluster_name, oidc_provider_arn), S3 (bucket ARNs for execution role)
# ---------------------------------------------------------------------------------------------------------------------

# Include the root config (remote state, provider generation)
include "root" {
  path = find_in_parent_folders("root.hcl")
}

# Include the shared EMR Virtual Cluster configuration from _envcommon
include "envcommon" {
  path   = "${dirname(find_in_parent_folders("root.hcl"))}/_envcommon/emr-virtual-cluster.hcl"
  expose = true
}

# ---------------------------------------------------------------------------------------------------------------------
# KUBERNETES PROVIDER
# The EMR community submodule creates kubernetes_role_v1 and kubernetes_role_binding_v1,
# so a Kubernetes provider must be configured to talk to the EKS cluster.
# ---------------------------------------------------------------------------------------------------------------------
generate "k8s_provider" {
  path      = "k8s-provider.tf"
  if_exists = "overwrite_terragrunt"
  contents  = <<EOF
data "aws_eks_cluster" "provider" {
  name = var.eks_cluster_name
}

data "aws_eks_cluster_auth" "provider" {
  name = var.eks_cluster_name
}

provider "kubernetes" {
  host                   = data.aws_eks_cluster.provider.endpoint
  cluster_ca_certificate = base64decode(data.aws_eks_cluster.provider.certificate_authority[0].data)
  token                  = data.aws_eks_cluster_auth.provider.token
}
EOF
}

# EMR Virtual Cluster depends on EKS for cluster registration
dependency "eks" {
  config_path = "../eks"

  mock_outputs = {
    cluster_name                       = "mock-eks-cluster"
    oidc_provider_arn                  = "arn:aws:iam:ap-southeast-1:123456789012:oidc-provider/oidc.eks.ap-southeast-1.amazonaws.com/id/MOCK"
    cluster_primary_security_group_id  = "sg-mock-12345"
  }
  mock_outputs_allowed_terraform_commands = ["init", "validate", "plan", "destroy"]
}

# EMR Virtual Cluster depends on S3 for bucket ARNs (execution role permissions)
dependency "s3_data_lake" {
  config_path = "../s3/data-lake"

  mock_outputs = {
    s3_bucket_arn = "arn:aws:s3:::mock-data-lake"
    s3_bucket_id  = "mock-data-lake"
  }
  mock_outputs_allowed_terraform_commands = ["init", "validate", "plan", "destroy"]
}

dependency "s3_pipes" {
  config_path = "../s3/pipes"

  mock_outputs = {
    s3_bucket_arn = "arn:aws:s3:::mock-pipes"
    s3_bucket_id  = "mock-pipes"
  }
  mock_outputs_allowed_terraform_commands = ["init", "validate", "plan", "destroy"]
}

dependency "s3_spark_logs" {
  config_path = "../s3/spark-logs"

  mock_outputs = {
    s3_bucket_arn = "arn:aws:s3:::mock-spark-logs"
    s3_bucket_id  = "mock-spark-logs"
  }
  mock_outputs_allowed_terraform_commands = ["init", "validate", "plan", "destroy"]
}

# Pass EKS and S3 outputs to the EMR Virtual Cluster module
inputs = {
  eks_cluster_name = dependency.eks.outputs.cluster_name

  # S3 bucket ARNs for the EMR execution role
  s3_bucket_arns = [
    dependency.s3_data_lake.outputs.s3_bucket_arn,
    "${dependency.s3_data_lake.outputs.s3_bucket_arn}/*",
    dependency.s3_pipes.outputs.s3_bucket_arn,
    "${dependency.s3_pipes.outputs.s3_bucket_arn}/*",
    dependency.s3_spark_logs.outputs.s3_bucket_arn,
    "${dependency.s3_spark_logs.outputs.s3_bucket_arn}/*",
  ]

  eks_oidc_provider_arn = dependency.eks.outputs.oidc_provider_arn
}
