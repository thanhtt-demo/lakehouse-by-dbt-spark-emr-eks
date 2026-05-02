# ---------------------------------------------------------------------------------------------------------------------
# GLUE MODULE VARIABLES
# ---------------------------------------------------------------------------------------------------------------------

variable "name" {
  description = "Name prefix for all Glue resources"
  type        = string
  default     = "lakehouse-at-scale"
}

variable "database_names" {
  description = "List of Glue Data Catalog database names to create (dbt schemas)"
  type        = list(string)
  default     = ["staging", "intermediate", "marts"]
}

variable "data_lake_bucket" {
  description = "Name of the S3 data lake bucket for Glue database location URI"
  type        = string
}

variable "tags" {
  description = "Additional tags for all resources"
  type        = map(string)
  default     = {}
}
