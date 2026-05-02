# ---------------------------------------------------------------------------------------------------------------------
# GLUE MODULE OUTPUTS
# ---------------------------------------------------------------------------------------------------------------------

output "database_names" {
  description = "Map of schema name to Glue Data Catalog database name"
  value       = { for k, v in aws_glue_catalog_database.this : k => v.name }
}

output "catalog_id" {
  description = "The ID of the Glue Catalog (AWS account ID)"
  value       = values(aws_glue_catalog_database.this)[0].catalog_id
}
