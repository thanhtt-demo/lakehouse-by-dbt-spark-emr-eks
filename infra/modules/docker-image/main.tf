# ---------------------------------------------------------------------------------------------------------------------
# DOCKER IMAGE MODULE
# Builds and pushes a Base Image to ECR using null_resource + local-exec provisioner.
# Only rebuilds when the Dockerfile content changes (triggered by file hash).
# Cross-platform: no interpreter specified — Terraform uses OS default shell.
# ---------------------------------------------------------------------------------------------------------------------

locals {
  image_uri       = "${var.ecr_repository_url}:${var.image_tag}"
  dockerfile_hash = filemd5(var.dockerfile_path)
  # Extract ECR registry URL (everything before the first slash after the protocol-less URL)
  ecr_registry = split("/", var.ecr_repository_url)[0]
  # Extract AWS region from ECR URL (format: <account_id>.dkr.ecr.<region>.amazonaws.com)
  aws_region = split(".", var.ecr_repository_url)[3]
}

resource "null_resource" "docker_build_push" {
  triggers = {
    dockerfile_hash = local.dockerfile_hash
    image_uri       = local.image_uri
  }

  provisioner "local-exec" {
    command = "aws ecr get-login-password --region ${local.aws_region} | docker login --username AWS --password-stdin ${local.ecr_registry}"
  }

  provisioner "local-exec" {
    command = "docker build -f ${var.dockerfile_path} -t ${local.image_uri} ${var.docker_context}"
  }

  provisioner "local-exec" {
    command = "docker push ${local.image_uri}"
  }
}
