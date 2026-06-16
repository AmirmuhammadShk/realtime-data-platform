variable "project_name" {
  description = "Project name used in resource naming."
  type        = string
}

variable "environment" {
  description = "Deployment environment such as dev, staging, or prod."
  type        = string
}

variable "vpc_id" {
  description = "VPC ID where the MSK cluster will be deployed."
  type        = string
}

variable "subnet_ids" {
  description = "Private subnet IDs for MSK broker placement."
  type        = list(string)

  validation {
    condition     = length(var.subnet_ids) >= 2
    error_message = "At least two private subnets are required for high availability."
  }
}

variable "allowed_client_security_group_ids" {
  description = "Security group IDs allowed to connect to the MSK brokers."
  type        = list(string)
  default     = []
}

variable "kafka_version" {
  description = "Apache Kafka version for the MSK cluster."
  type        = string
  default     = "3.6.0"
}

variable "broker_instance_type" {
  description = "MSK broker instance type."
  type        = string
  default     = "kafka.m5.large"
}

variable "number_of_broker_nodes" {
  description = "Number of broker nodes. For production, use at least one broker per AZ."
  type        = number
  default     = 3

  validation {
    condition     = var.number_of_broker_nodes >= 3
    error_message = "Use at least three broker nodes for a production-like multi-AZ MSK cluster."
  }
}

variable "broker_volume_size_gb" {
  description = "EBS volume size per broker in GB."
  type        = number
  default     = 500

  validation {
    condition     = var.broker_volume_size_gb >= 100
    error_message = "Broker volume size should be at least 100 GB."
  }
}

variable "client_broker_encryption" {
  description = "Client-broker encryption setting. Valid values are TLS, TLS_PLAINTEXT, or PLAINTEXT."
  type        = string
  default     = "TLS"

  validation {
    condition     = contains(["TLS", "TLS_PLAINTEXT", "PLAINTEXT"], var.client_broker_encryption)
    error_message = "client_broker_encryption must be one of TLS, TLS_PLAINTEXT, or PLAINTEXT."
  }
}

variable "enhanced_monitoring" {
  description = "MSK enhanced monitoring level."
  type        = string
  default     = "PER_BROKER"

  validation {
    condition = contains([
      "DEFAULT",
      "PER_BROKER",
      "PER_TOPIC_PER_BROKER",
      "PER_TOPIC_PER_PARTITION"
    ], var.enhanced_monitoring)

    error_message = "enhanced_monitoring must be a valid MSK monitoring level."
  }
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days."
  type        = number
  default     = 14
}
