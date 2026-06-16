module "msk" {
  source = "../../modules/msk"

  project_name = var.project_name
  environment  = var.environment
  vpc_id       = var.vpc_id
  subnet_ids   = var.private_subnet_ids

  allowed_client_security_group_ids = var.allowed_client_security_group_ids

  kafka_version          = "3.6.0"
  broker_instance_type   = "kafka.m5.large"
  number_of_broker_nodes = 3
  broker_volume_size_gb  = 500

  client_broker_encryption = "TLS"
  enhanced_monitoring      = "PER_BROKER"

  log_retention_days = 14
}
