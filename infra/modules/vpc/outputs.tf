# ---------------------------------------------------------------------------------------------------------------------
# VPC MODULE OUTPUTS
# ---------------------------------------------------------------------------------------------------------------------

output "vpc_id" {
  description = "The ID of the VPC"
  value       = aws_vpc.this.id
}

output "private_subnet_ids" {
  description = "List of private subnet IDs"
  value       = aws_subnet.private[*].id
}

output "public_subnet_ids" {
  description = "List of public subnet IDs"
  value       = aws_subnet.public[*].id
}

output "nat_gateway_id" {
  description = "The ID of the NAT Gateway"
  value       = aws_nat_gateway.this.id
}

output "internet_gateway_id" {
  description = "The ID of the Internet Gateway"
  value       = aws_internet_gateway.this.id
}

output "vpc_cidr_block" {
  description = "The CIDR block of the VPC"
  value       = aws_vpc.this.cidr_block
}

# ---------------------------------------------------------------------------------------------------------------------
# VPC ENDPOINT OUTPUTS
# ---------------------------------------------------------------------------------------------------------------------

output "s3_endpoint_id" {
  description = "ID of the S3 Gateway VPC Endpoint"
  value       = aws_vpc_endpoint.s3.id
}

output "ecr_api_endpoint_id" {
  description = "ID of the ECR API Interface VPC Endpoint"
  value       = aws_vpc_endpoint.ecr_api.id
}

output "ecr_dkr_endpoint_id" {
  description = "ID of the ECR Docker Interface VPC Endpoint"
  value       = aws_vpc_endpoint.ecr_dkr.id
}

output "eks_endpoint_id" {
  description = "ID of the EKS API Interface VPC Endpoint"
  value       = aws_vpc_endpoint.eks.id
}
