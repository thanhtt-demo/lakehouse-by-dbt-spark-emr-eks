# ---------------------------------------------------------------------------------------------------------------------
# ECR MODULE
# Creates multiple ECR repositories using the terraform-aws-modules/ecr/aws community module.
# Each repository has lifecycle policy (keep N recent images) and image scanning on push.
# ---------------------------------------------------------------------------------------------------------------------

module "ecr" {
  source  = "terraform-aws-modules/ecr/aws"
  version = "3.2.0"

  for_each = toset(var.repository_names)

  repository_name = "${var.name}/${each.value}"

  repository_image_tag_mutability = var.image_tag_mutability

  # Enable image scanning on push
  repository_image_scan_on_push = true

  # Lifecycle policy — keep N most recent images
  repository_lifecycle_policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep last ${var.max_image_count} images"
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = var.max_image_count
        }
        action = {
          type = "expire"
        }
      }
    ]
  })

  tags = merge(var.tags, {
    Name = "${var.name}/${each.value}"
  })
}
