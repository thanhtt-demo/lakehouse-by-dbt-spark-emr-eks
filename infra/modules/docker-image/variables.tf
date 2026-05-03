# ---------------------------------------------------------------------------------------------------------------------
# DOCKER IMAGE MODULE VARIABLES
# ---------------------------------------------------------------------------------------------------------------------

variable "name" {
  description = "Name prefix for resources"
  type        = string
}

variable "dockerfile_path" {
  description = "Path to the Dockerfile.base to build"
  type        = string
}

variable "ecr_repository_url" {
  description = "ECR repository URL to push the image to"
  type        = string
}

variable "image_tag" {
  description = "Tag for the Docker image"
  type        = string
  default     = "latest"
}

variable "docker_context" {
  description = "Docker build context directory"
  type        = string
}

variable "tags" {
  description = "Additional tags for all resources"
  type        = map(string)
  default     = {}
}
