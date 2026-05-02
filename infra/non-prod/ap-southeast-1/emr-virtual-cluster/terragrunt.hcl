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

# EMR Virtual Cluster depends on EKS for cluster registration
dependency "eks" {
  config_path = "../eks"

  mock_outputs = {
    cluster_name                       = "mock-eks-cluster"
    oidc_provider_arn                  = "arn:aws:iam::123456789012:oidc-provider/oidc.eks.ap-southeast-1.amazonaws.com/id/MOCK"
    cluster_primary_security_group_id  = "sg-mock-12345"
  }
  mock_outputs_allowed_terraform_commands = ["init", "validate", "plan"]
}

# EMR Virtual Cluster depends on S3 for bucket ARNs (execution role permissions)
dependency "s3_data_lake" {
  config_path = "../s3/data-lake"

  mock_outputs = {
    s3_bucket_arn = "arn:aws:s3:::mock-data-lake"
    s3_bucket_id  = "mock-data-lake"
  }
  mock_outputs_allowed_terraform_commands = ["init", "validate", "plan"]
}

dependency "s3_pipes" {
  config_path = "../s3/pipes"

  mock_outputs = {
    s3_bucket_arn = "arn:aws:s3:::mock-pipes"
    s3_bucket_id  = "mock-pipes"
  }
  mock_outputs_allowed_terraform_commands = ["init", "validate", "plan"]
}

dependency "s3_spark_logs" {
  config_path = "../s3/spark-logs"

  mock_outputs = {
    s3_bucket_arn = "arn:aws:s3:::mock-spark-logs"
    s3_bucket_id  = "mock-spark-logs"
  }
  mock_outputs_allowed_terraform_commands = ["init", "validate", "plan"]
}

# Pass EKS and S3 outputs to the EMR Virtual Cluster module
inputs = {
  eks_cluster_id = dependency.eks.outputs.cluster_name

  # S3 bucket ARNs for the EMR execution role
  s3_bucket_arns = [
    dependency.s3_data_lake.outputs.s3_bucket_arn,
    "${dependency.s3_data_lake.outputs.s3_bucket_arn}/*",
    dependency.s3_pipes.outputs.s3_bucket_arn,
    "${dependency.s3_pipes.outputs.s3_bucket_arn}/*",
    dependency.s3_spark_logs.outputs.s3_bucket_arn,
    "${dependency.s3_spark_logs.outputs.s3_bucket_arn}/*",
  ]

  oidc_provider_arn = dependency.eks.outputs.oidc_provider_arn
}
