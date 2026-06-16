output "msk_cluster_arn" {
  description = "ARN of the MSK cluster."
  value       = module.msk.cluster_arn
}

output "msk_cluster_name" {
  description = "Name of the MSK cluster."
  value       = module.msk.cluster_name
}

output "msk_bootstrap_brokers_tls" {
  description = "TLS bootstrap brokers for MSK clients."
  value       = module.msk.bootstrap_brokers_tls
  sensitive   = true
}

output "msk_security_group_id" {
  description = "Security group ID attached to MSK brokers."
  value       = module.msk.security_group_id
}
