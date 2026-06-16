variable "aws_region" {
  description = "AWS region used for the dev environment."
  type        = string
  default     = "eu-west-2"
}

variable "project_name" {
  description = "Project name used for tagging and naming resources."
  type        = string
  default     = "predictiva-realtime-platform"
}

variable "environment" {
  description = "Deployment environment."
  type        = string
  default     = "dev"
}

variable "vpc_id" {
  description = "VPC ID where the MSK cluster is deployed."
  type        = string
  default     = "vpc-xxxxxxxx"
}

variable "private_subnet_ids" {
  description = "Private subnet IDs across at least two or three availability zones."
  type        = list(string)
  default = [
    "subnet-aaaaaaaa",
    "subnet-bbbbbbbb",
    "subnet-cccccccc"
  ]
}

variable "allowed_client_security_group_ids" {
  description = "Security groups allowed to connect to MSK brokers."
  type        = list(string)
  default = [
    "sg-client-placeholder"
  ]
}
