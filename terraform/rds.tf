# laminar:node:imp-aws-aws_rds-7:begin
# ---------------------------------------------------------------------------
# Component: Export Config Store (RDS Postgres) — imp-aws-aws_rds-7
# Resource name base: export_config_store_rds
# ---------------------------------------------------------------------------

# Customer-managed KMS key for storage, Performance Insights and the
# RDS-managed master user secret.
resource "aws_kms_key" "export_config_store_rds_kms" {
  description             = "CMK for Export Config Store (RDS Postgres) - ${var.project_name}-${var.env}"
  enable_key_rotation     = true
  deletion_window_in_days = local.cfg.export_config_store_rds.kms_deletion_window_days

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "EnableAccountRootAdministration"
        Effect = "Allow"
        Principal = {
          AWS = "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:root"
        }
        Action   = "kms:*"
        Resource = "*"
      },
      {
        Sid    = "AllowRdsStorageEncryptionInAccount"
        Effect = "Allow"
        Principal = {
          Service = "rds.amazonaws.com"
        }
        Action = [
          "kms:Decrypt",
          "kms:Encrypt",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:CreateGrant",
          "kms:DescribeKey"
        ]
        Resource = "*"
        Condition = {
          StringEquals = {
            "kms:ViaService"    = "rds.${data.aws_region.current.name}.amazonaws.com"
            "kms:CallerAccount" = data.aws_caller_identity.current.account_id
          }
        }
      },
      {
        Sid    = "AllowSecretsManagerForRdsMasterUserSecret"
        Effect = "Allow"
        Principal = {
          Service = "secretsmanager.amazonaws.com"
        }
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ]
        Resource = "*"
        Condition = {
          StringEquals = {
            "kms:ViaService"    = "secretsmanager.${data.aws_region.current.name}.amazonaws.com"
            "kms:CallerAccount" = data.aws_caller_identity.current.account_id
          }
        }
      }
    ]
  })

  tags = {
    Component  = "export-config-store"
    Owner      = "data-platform-engineering@acme.com"
    CostCenter = local.cfg.export_config_store_rds.cost_center
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_kms_alias" "export_config_store_rds_kms" {
  name          = "alias/${var.project_name}-${var.env}-export-config-store-rds-key"
  target_key_id = aws_kms_key.export_config_store_rds_kms.key_id
}

# Isolated private database subnets (no public routing).
resource "aws_db_subnet_group" "export_config_store_rds_subnet_group" {
  name        = "${var.project_name}-${var.env}-export-config-store-rds-subnet-group"
  description = "Private database subnets for Export Config Store"
  subnet_ids  = local.cfg.export_config_store_rds.db_subnet_ids

  tags = {
    Component  = "export-config-store"
    Owner      = "data-platform-engineering@acme.com"
    CostCenter = local.cfg.export_config_store_rds.cost_center
  }
}

# Security group: no ingress or egress defined inline; ingress is granted
# only from named client security groups (MWAA, Export Admin API Lambda).
resource "aws_security_group" "export_config_store_rds_sg" {
  name        = "${var.project_name}-${var.env}-export-config-store-rds-sg"
  description = "Export Config Store PostgreSQL - ingress from approved clients only"
  vpc_id      = local.cfg.export_config_store_rds.vpc_id

  tags = {
    Component  = "export-config-store"
    Owner      = "data-platform-engineering@acme.com"
    CostCenter = local.cfg.export_config_store_rds.cost_center
  }
}

resource "aws_security_group_rule" "export_config_store_rds_ingress" {
  for_each = toset(local.cfg.export_config_store_rds.allowed_client_security_group_ids)

  type                     = "ingress"
  description              = "PostgreSQL 5432 from approved client security group"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  source_security_group_id = each.value
  security_group_id        = aws_security_group.export_config_store_rds_sg.id
}

# Parameter group enforcing TLS for all client connections.
resource "aws_db_parameter_group" "export_config_store_rds_params" {
  name        = "${var.project_name}-${var.env}-export-config-store-rds-params"
  family      = "postgres15"
  description = "Export Config Store - enforce SSL/TLS"

  parameter {
    name         = "rds.force_ssl"
    value        = "1"
    apply_method = "pending-reboot"
  }

  tags = {
    Component  = "export-config-store"
    Owner      = "data-platform-engineering@acme.com"
    CostCenter = local.cfg.export_config_store_rds.cost_center
  }
}

# Enhanced Monitoring role (RDS publishes OS metrics to CloudWatch).
resource "aws_iam_role" "export_config_store_rds_monitoring_role" {
  name = "${var.project_name}-${var.env}-export-config-store-rds-monitoring-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowRdsEnhancedMonitoringAssume"
        Effect = "Allow"
        Principal = {
          Service = "monitoring.rds.amazonaws.com"
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "aws:SourceAccount" = data.aws_caller_identity.current.account_id
          }
        }
      }
    ]
  })

  tags = {
    Component  = "export-config-store"
    Owner      = "data-platform-engineering@acme.com"
    CostCenter = local.cfg.export_config_store_rds.cost_center
  }
}

resource "aws_iam_role_policy_attachment" "export_config_store_rds_monitoring" {
  role       = aws_iam_role.export_config_store_rds_monitoring_role.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AmazonRDSEnhancedMonitoringRole"
}

# Export Config Store: PostgreSQL 15.4, encrypted, private, IAM-auth enabled.
resource "aws_db_instance" "export_config_store_rds" {
  identifier     = "${var.project_name}-${var.env}-export-config-store-rds"
  engine         = "postgres"
  engine_version = local.cfg.export_config_store_rds.engine_version
  instance_class = local.cfg.export_config_store_rds.instance_class
  db_name        = "export_config"

  # Storage: gp3 with automatic growth to prevent out-of-disk failures.
  storage_type          = "gp3"
  allocated_storage     = local.cfg.export_config_store_rds.allocated_storage
  max_allocated_storage = local.cfg.export_config_store_rds.max_allocated_storage
  storage_encrypted     = true
  kms_key_id            = aws_kms_key.export_config_store_rds_kms.arn

  # Credentials: RDS-managed master secret encrypted with the CMK; no plaintext
  # password exists in Terraform state or code.
  username                      = "dataoutbound_admin"
  manage_master_user_password   = true
  master_user_secret_kms_key_id = aws_kms_key.export_config_store_rds_kms.arn

  # IAM database authentication backs the rds-db:connect grants used by
  # Export Admin API, MWAA and Child Export DAG roles.
  iam_database_authentication_enabled = true

  # Network isolation
  db_subnet_group_name   = aws_db_subnet_group.export_config_store_rds_subnet_group.name
  vpc_security_group_ids = [aws_security_group.export_config_store_rds_sg.id]
  publicly_accessible    = false
  multi_az               = local.cfg.export_config_store_rds.multi_az
  parameter_group_name   = aws_db_parameter_group.export_config_store_rds_params.name

  # Backups and lifecycle
  backup_retention_period    = local.cfg.export_config_store_rds.backup_retention_period
  backup_window              = local.cfg.export_config_store_rds.backup_window
  maintenance_window         = local.cfg.export_config_store_rds.maintenance_window
  copy_tags_to_snapshot      = true
  auto_minor_version_upgrade = true
  apply_immediately          = false
  deletion_protection        = local.cfg.export_config_store_rds.deletion_protection
  skip_final_snapshot        = local.cfg.export_config_store_rds.skip_final_snapshot
  final_snapshot_identifier  = local.cfg.export_config_store_rds.skip_final_snapshot ? null : "${var.project_name}-${var.env}-export-config-store-rds-final"

  # Observability
  enabled_cloudwatch_logs_exports       = ["postgresql", "upgrade"]
  monitoring_interval                   = local.cfg.export_config_store_rds.monitoring_interval
  monitoring_role_arn                   = aws_iam_role.export_config_store_rds_monitoring_role.arn
  performance_insights_enabled          = true
  performance_insights_kms_key_id       = aws_kms_key.export_config_store_rds_kms.arn
  performance_insights_retention_period = 7

  tags = {
    Component  = "export-config-store"
    Owner      = "data-platform-engineering@acme.com"
    CostCenter = local.cfg.export_config_store_rds.cost_center
  }

  lifecycle {
    prevent_destroy = true
  }
}

output "export_config_store_rds_endpoint" {
  value = aws_db_instance.export_config_store_rds.endpoint
}

output "export_config_store_rds_port" {
  value = aws_db_instance.export_config_store_rds.port
}

output "export_config_store_rds_arn" {
  value = aws_db_instance.export_config_store_rds.arn
}

output "export_config_store_rds_identifier" {
  value = aws_db_instance.export_config_store_rds.identifier
}

output "export_config_store_rds_resource_id" {
  description = "DbiResourceId used to build rds-db:connect ARNs"
  value       = aws_db_instance.export_config_store_rds.resource_id
}

output "export_config_store_rds_security_group_id" {
  value = aws_security_group.export_config_store_rds_sg.id
}

output "export_config_store_rds_kms_key_arn" {
  value = aws_kms_key.export_config_store_rds_kms.arn
}

output "export_config_store_rds_master_user_secret_arn" {
  value = try(aws_db_instance.export_config_store_rds.master_user_secret[0].secret_arn, null)
}
# laminar:node:imp-aws-aws_rds-7:end
