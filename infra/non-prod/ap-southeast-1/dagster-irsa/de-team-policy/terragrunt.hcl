# ---------------------------------------------------------------------------------------------------------------------
# DAGSTER DE-TEAM IAM POLICY — non-prod
# Combined permissions for both de-team and sales-team (shared SA):
# - EMR job runs (de-team)
# - S3 Pipes read/write, S3 logs read
# - S3 Data Lake read/write (Iceberg tables)
# - Glue Data Catalog full access (all databases)
# - Athena query execution (sales-team)
# Dependencies: S3 Pipes, S3 Spark logs, S3 Data Lake, EMR Virtual Cluster
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

dependency "s3_data_lake" {
  config_path = "../../s3/data-lake"

  mock_outputs = {
    s3_bucket_arn = "arn:aws:s3:::mock-data-lake"
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
  description = "Combined policy for Dagster user deployments: EMR, S3, Glue, Athena"

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
          "emr-containers:TagResource",
          "emr-containers:DescribeJobRun"
        ]
        Resource = [
          dependency.emr.outputs.virtual_cluster_arn,
          "${dependency.emr.outputs.virtual_cluster_arn}/*"
        ]
      },
      {
        Sid    = "PipesBucketReadWrite"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ]
        Resource = [
          dependency.s3_pipes.outputs.s3_bucket_arn,
          "${dependency.s3_pipes.outputs.s3_bucket_arn}/*",
        ]
      },
      {
        Sid    = "LogsBucketReadWrite"
        Effect = "Allow"
        Action = [
          # Read: the Spark History Server (running as this same de-team role via the
          # spark:spark-history-server SA) replays event logs from this bucket.
          "s3:GetObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
          # Write: in eks_client mode the Dagster step pod (Spark driver) and its executor
          # pods run as this role and must write Spark event logs to spark-events/. Rolling
          # event logs also rename/replace chunk objects, so DeleteObject is required.
          "s3:PutObject",
          "s3:DeleteObject",
        ]
        Resource = [
          dependency.s3_spark_logs.outputs.s3_bucket_arn,
          "${dependency.s3_spark_logs.outputs.s3_bucket_arn}/*",
        ]
      },
      {
        Sid    = "DataLakeReadWrite"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ]
        Resource = [
          dependency.s3_data_lake.outputs.s3_bucket_arn,
          "${dependency.s3_data_lake.outputs.s3_bucket_arn}/*",
        ]
      },
      {
        Sid    = "GlueCatalogFullAccess"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:CreateDatabase",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:CreatePartition",
          "glue:BatchCreatePartition",
          "glue:BatchDeletePartition",
          "glue:BatchGetPartition",
        ]
        Resource = [
          "arn:aws:glue:ap-southeast-1:560503716668:catalog",
          "arn:aws:glue:ap-southeast-1:560503716668:database/*",
          "arn:aws:glue:ap-southeast-1:560503716668:table/*/*",
        ]
      },
      {
        Sid    = "AthenaQueryExecution"
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:StopQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:GetWorkGroup",
          "athena:ListWorkGroups",
          "athena:GetDataCatalog",
          "athena:ListDataCatalogs",
        ]
        Resource = "*"
      },
    ]
  })
}
