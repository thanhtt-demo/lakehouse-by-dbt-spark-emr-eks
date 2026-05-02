# ---------------------------------------------------------------------------------------------------------------------
# GLUE MODULE
# Creates Glue Data Catalog databases for dbt schemas (staging, intermediate, marts).
# Each database points to the S3 data lake bucket as its location.
# ---------------------------------------------------------------------------------------------------------------------

resource "aws_glue_catalog_database" "this" {
  for_each = toset(var.database_names)

  name         = "${var.name}_${each.value}"
  description  = "Glue Data Catalog database for dbt ${each.value} schema"
  location_uri = "s3://${var.data_lake_bucket}/warehouse/${each.value}/"

  tags = merge(var.tags, {
    Name = "${var.name}_${each.value}"
  })
}
