# ---------------------------------------------------------------------------------------------------------------------
# ECR MODULE OUTPUTS
# ---------------------------------------------------------------------------------------------------------------------

output "repository_urls" {
  description = "Map of repository name to repository URL"
  value       = { for k, v in module.ecr : k => v.repository_url }
}

output "repository_arns" {
  description = "Map of repository name to repository ARN"
  value       = { for k, v in module.ecr : k => v.repository_arn }
}
