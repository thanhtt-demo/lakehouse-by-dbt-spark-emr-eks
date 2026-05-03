# ---------------------------------------------------------------------------------------------------------------------
# DOCKER IMAGE MODULE OUTPUTS
# ---------------------------------------------------------------------------------------------------------------------

output "image_uri" {
  description = "Full image URI (ecr_repository_url:tag)"
  value       = local.image_uri
}

output "dockerfile_hash" {
  description = "MD5 hash of the Dockerfile (used for change detection)"
  value       = local.dockerfile_hash
}
