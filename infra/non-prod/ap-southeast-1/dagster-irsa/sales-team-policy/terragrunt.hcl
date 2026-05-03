# ---------------------------------------------------------------------------------------------------------------------
# DAGSTER SALES-TEAM IAM POLICY — non-prod
# Permissions: Athena queries, S3 data lake read/write, Glue catalog access.
# Dependencies: S3 data lake, S3 Spark logs
# ---------------------------------------------------------------------------------------------------------------------

include "root" {
  path = find_in_parent_folders("root.hcl")
}

include "envcommon" {
  path   = "${dirname(find_in_parent_folders("root.hcl"))}/_envcommon/dagster-irsa-policy.hcl"
  expose = true
}

dependency "s3_data_lake" {
  config_path = "../../s3/data-lake"

  mock_outputs = {
    s3_bucket_arn = "arn:aws:s3:::mock-data-lake"
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

inputs = {
  name        = "lakehouse-at-scale-dagster-sales-team"
  description = "Policy for Dagster sales-team: Athena, S3 data lake, Glue catalog"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AthenaAccess"
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:StopQueryExecution",
          "athena:ListQueryExecutions",
        ]
        Resource = "*"
      },
      {
        Sid    = "DataLakeBucketReadWrite"
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
        Sid    = "AthenaResultsBucket"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ]
        Resource = [
          dependency.s3_spark_logs.outputs.s3_bucket_arn,
          "${dependency.s3_spark_logs.outputs.s3_bucket_arn}/*",
        ]
      },
      {
        Sid    = "GlueCatalogAccess"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:BatchGetPartition",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
        ]
        Resource = "*"
      },
    ]
  })
}
