output "cluster_arn" {
  description = "ARN of the MSK cluster."
  value       = aws_msk_cluster.this.arn
}

output "cluster_name" {
  description = "Name of the MSK cluster."
  value       = aws_msk_cluster.this.cluster_name
}

output "bootstrap_brokers_tls" {
  description = "TLS bootstrap broker connection string."
  value       = aws_msk_cluster.this.bootstrap_brokers_tls
  sensitive   = true
}

output "security_group_id" {
  description = "Security group ID attached to the MSK brokers."
  value       = aws_security_group.msk.id
}

output "cloudwatch_log_group_name" {
  description = "CloudWatch log group used for MSK broker logs."
  value       = aws_cloudwatch_log_group.msk_broker_logs.name
}
