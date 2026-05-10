# ---------------------------------------------------------------------------------------------------------------------
# EMR EXECUTION ROLE — ICEBERG + GLUE POLICY (non-prod)
# Additional IAM policy attached to the EMR on EKS job execution role so that
# dbt-spark (running inside Spark driver/executor pods) can read and write
# Iceberg tables through the AWS Glue Data Catalog.
#
# Grants:
#   - Glue Data Catalog read/write (databases, tables, partitions)
#   - Glue access for tables and partitions used by Iceberg writes
#
# S3 permissions for the data lake bucket are already granted by the EMR
# virtual-cluster community submodule via `s3_bucket_arns` input.
#
# Attached to the execution role via `iam_role_additional_policies` input
# of the emr-virtual-cluster unit (see dependency wiring there).
# ---------------------------------------------------------------------------------------------------------------------

include "root" {
  path = find_in_parent_folders("root.hcl")
}

include "envcommon" {
  path   = "${dirname(find_in_parent_folders("root.hcl"))}/_envcommon/dagster-irsa-policy.hcl"
  expose = true
}

inputs = {
  name        = "lakehouse-at-scale-emr-execution-iceberg"
  description = "Glue Data Catalog access for EMR on EKS Spark jobs writing Iceberg tables"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "GlueCatalogReadWrite"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:CreateDatabase",
          "glue:UpdateDatabase",
          "glue:GetTable",
          "glue:GetTables",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:CreatePartition",
          "glue:UpdatePartition",
          "glue:DeletePartition",
          "glue:BatchCreatePartition",
          "glue:BatchDeletePartition",
          "glue:BatchGetPartition",
          "glue:BatchUpdatePartition",
        ]
        Resource = [
          "arn:aws:glue:ap-southeast-1:560503716668:catalog",
          "arn:aws:glue:ap-southeast-1:560503716668:database/*",
          "arn:aws:glue:ap-southeast-1:560503716668:table/*/*",
        ]
      },
    ]
  })
}
