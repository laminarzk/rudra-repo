# laminar:node:imp-aws-aws_emr_serverless-5:begin
locals {
  export_extract_spark_job_emr_name      = "${var.project_name}-${var.env}-export-extract-spark-job-emr"
  export_extract_spark_job_emr_log_group = "/aws/laminar/emr-serverless/export-extract-spark-job/${var.env}"
  export_extract_spark_job_emr_arn_base  = "arn:${data.aws_partition.current.partition}"
  export_extract_spark_job_emr_account   = data.aws_caller_identity.current.account_id
  export_extract_spark_job_emr_region    = data.aws_region.current.name

  export_extract_spark_job_emr_tags = {
    Component  = "export_extract_spark_job_emr"
    Owner      = "data-platform-engineering@acme.com"
    CostCenter = local.cfg.export_extract_spark_job_emr.cost_center
  }
}

# ---------------------------------------------------------------------------
# Encryption: component CMK (logs, EMR monitoring output, job-run data)
# ---------------------------------------------------------------------------
resource "aws_kms_key" "export_extract_spark_job_emr_kms" {
  description             = "CMK for ${local.export_extract_spark_job_emr_name} logs, monitoring and job-run data"
  enable_key_rotation     = true
  deletion_window_in_days = local.cfg.export_extract_spark_job_emr.kms_deletion_window_days

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "EnableAccountRootAdministration"
        Effect    = "Allow"
        Principal = { AWS = "${local.export_extract_spark_job_emr_arn_base}:iam::${local.export_extract_spark_job_emr_account}:root" }
        Action    = "kms:*"
        Resource  = "*"
      },
      {
        Sid       = "AllowJobExecutionRoleUse"
        Effect    = "Allow"
        Principal = { AWS = aws_iam_role.export_extract_spark_job_emr_role.arn }
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ]
        Resource = "*"
      },
      {
        Sid       = "AllowCloudWatchLogsForComponentLogGroup"
        Effect    = "Allow"
        Principal = { Service = "logs.${local.export_extract_spark_job_emr_region}.amazonaws.com" }
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ]
        Resource = "*"
        Condition = {
          ArnLike = {
            "kms:EncryptionContext:aws:logs:arn" = "${local.export_extract_spark_job_emr_arn_base}:logs:${local.export_extract_spark_job_emr_region}:${local.export_extract_spark_job_emr_account}:log-group:${local.export_extract_spark_job_emr_log_group}*"
          }
        }
      }
    ]
  })

  tags = local.export_extract_spark_job_emr_tags
}

resource "aws_kms_alias" "export_extract_spark_job_emr_kms" {
  name          = "alias/${var.project_name}-${var.env}-export-extract-spark-job-emr-key"
  target_key_id = aws_kms_key.export_extract_spark_job_emr_kms.key_id
}

# ---------------------------------------------------------------------------
# Logging: explicitly provisioned log group, KMS-encrypted, configured retention
# ---------------------------------------------------------------------------
resource "aws_cloudwatch_log_group" "export_extract_spark_job_emr_logs" {
  name              = local.export_extract_spark_job_emr_log_group
  retention_in_days = local.cfg.export_extract_spark_job_emr.log_retention_days
  kms_key_id        = aws_kms_key.export_extract_spark_job_emr_kms.arn
  tags              = local.export_extract_spark_job_emr_tags
}

# ---------------------------------------------------------------------------
# Network: private-only security group (no public ingress, scoped egress)
# ---------------------------------------------------------------------------
resource "aws_security_group" "export_extract_spark_job_emr_sg" {
  name        = "${local.export_extract_spark_job_emr_name}-sg"
  description = "EMR Serverless Export Extract Spark Job - private subnets only"
  vpc_id      = local.cfg.export_extract_spark_job_emr.vpc_id
  tags        = local.export_extract_spark_job_emr_tags
}

# Spark worker-to-worker shuffle (self-referencing only)
resource "aws_vpc_security_group_ingress_rule" "export_extract_spark_job_emr_self" {
  security_group_id            = aws_security_group.export_extract_spark_job_emr_sg.id
  referenced_security_group_id = aws_security_group.export_extract_spark_job_emr_sg.id
  ip_protocol                  = "tcp"
  from_port                    = 0
  to_port                      = 65535
  description                  = "Spark worker-to-worker shuffle"
}

# Spark shuffle egress to self (default egress is removed by Terraform)
resource "aws_vpc_security_group_egress_rule" "export_extract_spark_job_emr_self" {
  security_group_id            = aws_security_group.export_extract_spark_job_emr_sg.id
  referenced_security_group_id = aws_security_group.export_extract_spark_job_emr_sg.id
  ip_protocol                  = "tcp"
  from_port                    = 0
  to_port                      = 65535
  description                  = "Spark worker-to-worker shuffle"
}

# TLS to S3, KMS, CloudWatch, EMR Serverless APIs via NAT gateway
resource "aws_vpc_security_group_egress_rule" "export_extract_spark_job_emr_https" {
  security_group_id = aws_security_group.export_extract_spark_job_emr_sg.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
  description       = "TLS to AWS service endpoints"
}

# JDBC read from Golden Entity Source Store (Redshift), narrow CIDRs only
resource "aws_vpc_security_group_egress_rule" "export_extract_spark_job_emr_redshift" {
  for_each          = toset(local.cfg.export_extract_spark_job_emr.redshift_cidr_blocks)
  security_group_id = aws_security_group.export_extract_spark_job_emr_sg.id
  cidr_ipv4         = each.value
  ip_protocol       = "tcp"
  from_port         = local.cfg.export_extract_spark_job_emr.redshift_port
  to_port           = local.cfg.export_extract_spark_job_emr.redshift_port
  description       = "JDBC to Golden Entity Source Store (Redshift)"
}

# ---------------------------------------------------------------------------
# IAM: dedicated job execution role (least privilege, scoped trust)
# ---------------------------------------------------------------------------
resource "aws_iam_role" "export_extract_spark_job_emr_role" {
  name = "${local.export_extract_spark_job_emr_name}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "EmrServerlessJobRunTrust"
        Effect    = "Allow"
        Principal = { Service = "emr-serverless.amazonaws.com" }
        Action    = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "aws:SourceAccount" = local.export_extract_spark_job_emr_account
          }
          ArnEquals = {
            "aws:SourceArn" = aws_emrserverless_application.export_extract_spark_job_emr.arn
          }
        }
      }
    ]
  })

  tags = local.export_extract_spark_job_emr_tags
}

resource "aws_iam_role_policy" "export_extract_spark_job_emr_policy" {
  name = "${local.export_extract_spark_job_emr_name}-policy"
  role = aws_iam_role.export_extract_spark_job_emr_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "StagingObjectsReadWrite"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = "${aws_s3_bucket.customer_staging_buckets_s3.arn}/exports/*"
      },
      {
        Sid    = "StagingBucketList"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = aws_s3_bucket.customer_staging_buckets_s3.arn
        Condition = {
          StringLike = {
            "s3:prefix" = ["exports/*"]
          }
        }
      },
      {
        Sid    = "AuditAndEmrLogWrite"
        Effect = "Allow"
        Action = [
          "s3:PutObject"
        ]
        Resource = [
          "${aws_s3_bucket.audit_execution_log_store_s3.arn}/execution-audit/*",
          "${aws_s3_bucket.audit_execution_log_store_s3.arn}/emr-serverless-logs/*"
        ]
      },
      {
        Sid    = "CrossComponentBucketKmsUse"
        Effect = "Allow"
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ]
        Resource = [
          aws_kms_key.customer_staging_buckets_s3_kms.arn,
          aws_kms_key.audit_execution_log_store_s3_kms.arn
        ]
      },
      {
        Sid    = "ComponentKmsUse"
        Effect = "Allow"
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ]
        Resource = aws_kms_key.export_extract_spark_job_emr_kms.arn
      },
      {
        Sid    = "RedshiftIamAuthenticatedJdbc"
        Effect = "Allow"
        Action = [
          "redshift:GetClusterCredentials"
        ]
        Resource = [
          "${local.export_extract_spark_job_emr_arn_base}:redshift:${local.export_extract_spark_job_emr_region}:${local.export_extract_spark_job_emr_account}:dbuser:${local.cfg.export_extract_spark_job_emr.redshift_cluster_identifier}/${local.cfg.export_extract_spark_job_emr.redshift_db_user}",
          "${local.export_extract_spark_job_emr_arn_base}:redshift:${local.export_extract_spark_job_emr_region}:${local.export_extract_spark_job_emr_account}:dbname:${local.cfg.export_extract_spark_job_emr.redshift_cluster_identifier}/${local.cfg.export_extract_spark_job_emr.redshift_database_name}"
        ]
      },
      {
        Sid    = "ComponentLogWrite"
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogStreams"
        ]
        Resource = "${local.export_extract_spark_job_emr_arn_base}:logs:${local.export_extract_spark_job_emr_region}:${local.export_extract_spark_job_emr_account}:log-group:${local.export_extract_spark_job_emr_log_group}:*"
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# EMR Serverless application (Spark, private networking, no interactive access)
# ---------------------------------------------------------------------------
resource "aws_emrserverless_application" "export_extract_spark_job_emr" {
  name          = local.export_extract_spark_job_emr_name
  release_label = local.cfg.export_extract_spark_job_emr.release_label
  type          = "Spark"

  auto_start_configuration {
    enabled = true
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = local.cfg.export_extract_spark_job_emr.auto_stop_idle_minutes
  }

  maximum_capacity {
    cpu    = "${local.cfg.export_extract_spark_job_emr.max_vcpu} vCPU"
    memory = "${local.cfg.export_extract_spark_job_emr.max_memory_gb} GB"
    disk   = "${local.cfg.export_extract_spark_job_emr.max_disk_gb} GB"
  }

  initial_capacity {
    initial_capacity_type = "Executor"

    initial_capacity_config {
      worker_count = local.cfg.export_extract_spark_job_emr.initial_worker_count

      worker_configuration {
        cpu    = "${local.cfg.export_extract_spark_job_emr.initial_worker_vcpu} vCPU"
        memory = "${local.cfg.export_extract_spark_job_emr.initial_worker_memory_gb} GB"
        disk   = "${local.cfg.export_extract_spark_job_emr.initial_worker_disk_gb} GB"
      }
    }
  }

  network_configuration {
    subnet_ids         = local.cfg.export_extract_spark_job_emr.private_subnet_ids
    security_group_ids = [aws_security_group.export_extract_spark_job_emr_sg.id]
  }

  interactive_configuration {
    livy_endpoint_enabled = false
    studio_enabled        = false
  }

  monitoring_configuration {
    cloudwatch_logging_configuration {
      log_group_name     = aws_cloudwatch_log_group.export_extract_spark_job_emr_logs.name
      encryption_key_arn = aws_kms_key.export_extract_spark_job_emr_kms.arn
    }

    s3_monitoring_configuration {
      log_uri            = "s3://${aws_s3_bucket.audit_execution_log_store_s3.bucket}/emr-serverless-logs/"
      encryption_key_arn = aws_kms_key.export_extract_spark_job_emr_kms.arn
    }
  }

  tags = local.export_extract_spark_job_emr_tags
}

output "export_extract_spark_job_emr_id" {
  value = aws_emrserverless_application.export_extract_spark_job_emr.id
}

output "export_extract_spark_job_emr_arn" {
  value = aws_emrserverless_application.export_extract_spark_job_emr.arn
}

output "export_extract_spark_job_emr_execution_role_arn" {
  value = aws_iam_role.export_extract_spark_job_emr_role.arn
}

output "export_extract_spark_job_emr_security_group_id" {
  value = aws_security_group.export_extract_spark_job_emr_sg.id
}

output "export_extract_spark_job_emr_kms_key_arn" {
  value = aws_kms_key.export_extract_spark_job_emr_kms.arn
}
# laminar:node:imp-aws-aws_emr_serverless-5:end
