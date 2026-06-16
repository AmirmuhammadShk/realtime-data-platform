locals {
  name = "${var.project_name}-${var.environment}-msk"
}

resource "aws_security_group" "msk" {
  name        = "${local.name}-sg"
  description = "Security group for Amazon MSK brokers"
  vpc_id      = var.vpc_id

  egress {
    description = "Allow all outbound traffic from MSK brokers"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group_rule" "client_tls" {
  count = length(var.allowed_client_security_group_ids)

  type                     = "ingress"
  description              = "Allow Kafka TLS traffic from approved client security groups"
  from_port                = 9094
  to_port                  = 9094
  protocol                 = "tcp"
  security_group_id        = aws_security_group.msk.id
  source_security_group_id = var.allowed_client_security_group_ids[count.index]
}

resource "aws_cloudwatch_log_group" "msk_broker_logs" {
  name              = "/aws/msk/${local.name}/broker-logs"
  retention_in_days = var.log_retention_days
}

resource "aws_msk_configuration" "this" {
  name           = "${local.name}-configuration"
  kafka_versions = [var.kafka_version]

  server_properties = <<-PROPERTIES
    auto.create.topics.enable=false
    default.replication.factor=3
    min.insync.replicas=2
    num.partitions=12
    log.retention.hours=168
    log.cleanup.policy=delete
    unclean.leader.election.enable=false
  PROPERTIES
}

resource "aws_msk_cluster" "this" {
  cluster_name           = local.name
  kafka_version          = var.kafka_version
  number_of_broker_nodes = var.number_of_broker_nodes
  enhanced_monitoring    = var.enhanced_monitoring

  broker_node_group_info {
    instance_type   = var.broker_instance_type
    client_subnets  = var.subnet_ids
    security_groups = [aws_security_group.msk.id]

    storage_info {
      ebs_storage_info {
        volume_size = var.broker_volume_size_gb
      }
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.this.arn
    revision = aws_msk_configuration.this.latest_revision
  }

  encryption_info {
    encryption_in_transit {
      client_broker = var.client_broker_encryption
      in_cluster    = true
    }
  }

  client_authentication {
    unauthenticated = false

    sasl {
      iam = true
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk_broker_logs.name
      }
    }
  }

  lifecycle {
    prevent_destroy = true
  }
}
