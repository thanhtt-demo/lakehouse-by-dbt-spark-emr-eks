# ---------------------------------------------------------------------------------------------------------------------
# ECR MODULE VARIABLES
# ---------------------------------------------------------------------------------------------------------------------

variable "name" {
  description = "Name prefix for all ECR resources"
  type        = string
  default     = "lakehouse-at-scale"
}

variable "repository_names" {
  description = "List of ECR repository names to create"
  type        = list(string)
  default = [
    "de-team-base",
    "de-team-code",
    "sales-team-base",
    "sales-team-code",
  ]
}

variable "image_tag_mutability" {
  description = "The tag mutability setting for the repositories"
  type        = string
  default     = "MUTABLE"
}

variable "max_image_count" {
  description = "Maximum number of images to keep per repository (lifecycle policy)"
  type        = number
  default     = 30
}

variable "tags" {
  description = "Additional tags for all resources"
  type        = map(string)
  default     = {}
}
