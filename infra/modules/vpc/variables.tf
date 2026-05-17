# ---------------------------------------------------------------------------------------------------------------------
# VPC MODULE VARIABLES
# ---------------------------------------------------------------------------------------------------------------------

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "availability_zones" {
  description = "List of availability zones to use for subnets"
  type        = list(string)
  default     = ["ap-southeast-1a", "ap-southeast-1b", "ap-southeast-1c"]
}

variable "private_subnet_cidrs" {
  description = "CIDR blocks for private subnets (one per AZ)"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for public subnets (one per AZ)"
  type        = list(string)
  default     = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]
}

variable "name" {
  description = "Name prefix for all VPC resources"
  type        = string
  default     = "lakehouse-at-scale"
}

variable "tags" {
  description = "Additional tags for all resources"
  type        = map(string)
  default     = {}
}

variable "cluster_name" {
  description = "EKS cluster name — used for karpenter.sh/discovery tag on private subnets"
  type        = string
  default     = "lakehouse-at-scale-eks"
}

variable "aws_region" {
  description = "AWS region — used for VPC Endpoint service names"
  type        = string
  default     = "ap-southeast-1"
}
