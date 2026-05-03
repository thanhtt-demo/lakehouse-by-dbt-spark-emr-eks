# ---------------------------------------------------------------------------------------------------------------------
# DAGSTER DE-TEAM IAM POLICY — non-prod
# Permissions: submit/monitor EMR job runs, read/write S3 Pipes, read S3 logs.
# Dependencies: S3 Pipes, S3 Spark logs, EMR Virtual Cluster
# ---------------------------------------------------------------------------------------------------------------------

include "root" {
  path = find_in_parent_folders("root.hcl")
}

include "envcommon" {
  path   = "${dirname(find_in_parent_folders("root.hcl"))}/_envcommon/dagster-irsa-policy.hcl"
  expose = true
}

dependency "s3_pipes" {
  config_path = "../../s3/pipes"

  mock_outputs = {
    s3_bucket_arn = "arn:aws:s3:::mock-pipes"
  }
  mock_outputs_allowed_terraform_commands = ["init", "validate", "plan", "destroy"]
}

dependency "s3_spark_logs" {
  config_path = "../../s3/spark-logs"

  mock_outputs = {
    s3_bucket_arn = "arn:aws:s3:::mock-spark-logs"
  }
  mock_outputs_allowed_terraform_commands = ["init", "validate", "plan", "destroy"]
}

dependency "emr" {
  config_path = "../../emr-virtual-cluster"

  mock_outputs = {
    virtual_cluster_arn = "arn:aws:emr-containers:ap-southeast-1:123456789012:/virtualclusters/mock"
  }
  mock_outputs_allowed_terraform_commands = ["init", "validate", "plan", "destroy"]
}

inputs = {
  name        = "lakehouse-at-scale-dagster-de-team"
  description = "Policy for Dagster de-team: EMR job runs, S3 Pipes, S3 logs"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "EMRJobRuns"
        Effect = "Allow"
        Action = [
          "emr-containers:StartJobRun",
          "emr-containers:DescribeJobRun",
          "emr-containers:CancelJobRun",
          "emr-containers:ListJobRuns",
        ]
        Resource = dependency.emr.outputs.virtual_cluster_arn
      },
      {
        Sid    = "PipesBucketReadWrite"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
        ]
        Resource = [
          dependency.s3_pipes.outputs.s3_bucket_arn,
          "${dependency.s3_pipes.outputs.s3_bucket_arn}/*",
        ]
      },
      {
        Sid    = "LogsBucketRead"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
        ]
        Resource = [
          dependency.s3_spark_logs.outputs.s3_bucket_arn,
          "${dependency.s3_spark_logs.outputs.s3_bucket_arn}/*",
        ]
      },
    ]
  })
}
