# laminar:node:imp-standard-standard_api-1:begin
# Export Admin API (do-admin-api): REST over a Lambda Function URL.
# JWT bearer tokens are validated in application code (export-admin role check);
# the Function URL itself requires SigV4 (AWS_IAM) as a second layer.
# API Gateway is intentionally not used (outside the SCRUM-485 allowlist).

locals {
  export_admin_api_log_group_name = "/aws/laminar/lambda/export-admin-api/${var.env}"

  export_admin_api_tags = {
    Component  = "export-admin-api"
    Owner      = local.cfg.export_admin_api.owner
    CostCenter = local.cfg.export_admin_api.cost_center
  }
}

# ---------------------------------------------------------------------------
# Customer-managed KMS key: credential field encryption, log group and env var encryption
# ---------------------------------------------------------------------------
resource "aws_kms_key" "export_admin_api_kms" {
  description             = "CMK for Export Admin API credential encryption, logs and environment variables (${var.env})"
  enable_key_rotation     = true
  deletion_window_in_days = local.cfg.export_admin_api.kms_deletion_window_days

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "EnableRootAccountAdministration"
        Effect    = "Allow"
        Principal = { AWS = "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:root" }
        Action    = "kms:*"
        Resource  = "*"
      },
      {
        Sid       = "AllowExportAdminApiRoleCryptoOperations"
        Effect    = "Allow"
        Principal = { AWS = aws_iam_role.export_admin_api_role.arn }
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:GenerateDataKey",
          "kms:DescribeKey"
        ]
        Resource = "*"
      },
      {
        Sid       = "AllowCloudWatchLogsUseOfKeyForExportAdminApiLogGroup"
        Effect    = "Allow"
        Principal = { Service = "logs.${data.aws_region.current.name}.amazonaws.com" }
        Action = [
          "kms:Encrypt*",
          "kms:Decrypt*",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:Describe*"
        ]
        Resource = "*"
        Condition = {
          ArnLike = {
            "kms:EncryptionContext:aws:logs:arn" = "arn:${data.aws_partition.current.partition}:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:${local.export_admin_api_log_group_name}"
          }
          StringEquals = {
            "aws:SourceAccount" = data.aws_caller_identity.current.account_id
          }
        }
      }
    ]
  })

  tags = local.export_admin_api_tags

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_kms_alias" "export_admin_api_kms_alias" {
  name          = "alias/${var.project_name}-${var.env}-export-admin-api-key"
  target_key_id = aws_kms_key.export_admin_api_kms.key_id
}

# ---------------------------------------------------------------------------
# Execution role (trust scoped to this account)
# ---------------------------------------------------------------------------
resource "aws_iam_role" "export_admin_api_role" {
  name = "${var.project_name}-${var.env}-export-admin-api-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "LambdaServiceAssumeRole"
        Effect    = "Allow"
        Principal = { Service = "lambda.amazonaws.com" }
        Action    = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "aws:SourceAccount" = data.aws_caller_identity.current.account_id
          }
        }
      }
    ]
  })

  tags = local.export_admin_api_tags
}

# Least-privilege inline policy: every action is scoped to a specific resource ARN
resource "aws_iam_role_policy" "export_admin_api_policy" {
  name = "${var.project_name}-${var.env}-export-admin-api-policy"
  role = aws_iam_role.export_admin_api_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "AllowIamAuthConnectToConfigStore"
        Effect   = "Allow"
        Action   = ["rds-db:connect"]
        Resource = "arn:${data.aws_partition.current.partition}:rds-db:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:dbuser:${aws_db_instance.export_config_store_rds.resource_id}/${local.cfg.export_admin_api.db_user}"
      },
      {
        Sid    = "AllowReadDataOutboundParameters"
        Effect = "Allow"
        Action = [
          "ssm:GetParameter",
          "ssm:GetParameters"
        ]
        Resource = "arn:${data.aws_partition.current.partition}:ssm:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:parameter/dataoutbound/db/cofig_db/*"
      },
      {
        Sid      = "AllowSecureStringDecryptViaSsmOnly"
        Effect   = "Allow"
        Action   = ["kms:Decrypt"]
        Resource = aws_kms_key.export_admin_api_kms.arn
        Condition = {
          StringEquals = {
            "kms:ViaService" = "ssm.${data.aws_region.current.name}.amazonaws.com"
          }
        }
      },
      {
        Sid    = "AllowCredentialFieldEncryption"
        Effect = "Allow"
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:GenerateDataKey",
          "kms:DescribeKey"
        ]
        Resource = aws_kms_key.export_admin_api_kms.arn
      },
      {
        Sid      = "AllowTriggerOrchestratorDagViaWebToken"
        Effect   = "Allow"
        Action   = ["airflow:CreateWebLoginToken"]
        Resource = aws_mwaa_environment.orchestrator_dag_mwaa.arn
      },
      {
        Sid      = "AllowAdminAuditWritesToAuditStore"
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = "${aws_s3_bucket.audit_execution_log_store_s3.arn}/export-admin-api/*"
      },
      {
        Sid    = "AllowWriteToComponentLogGroupOnly"
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:${data.aws_partition.current.partition}:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:${local.export_admin_api_log_group_name}:*"
      }
    ]
  })
}

# Lambda VPC ENI management (AWS managed, scoped by the service itself)
resource "aws_iam_role_policy_attachment" "export_admin_api_vpc_access" {
  role       = aws_iam_role.export_admin_api_role.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

# ---------------------------------------------------------------------------
# Network: private subnets, no public IP, egress only to DB, MWAA endpoints and S3
# ---------------------------------------------------------------------------
resource "aws_security_group" "export_admin_api_sg" {
  name        = "${var.project_name}-${var.env}-export-admin-api-sg"
  description = "Export Admin API Lambda: egress to Export Config Store, VPC endpoints and S3 only"
  vpc_id      = local.cfg.export_admin_api.vpc_id

  egress {
    description     = "PostgreSQL to Export Config Store"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.export_config_store_rds_sg.id]
  }

  egress {
    description = "HTTPS to MWAA web server and VPC interface endpoints (KMS, SSM)"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [local.cfg.export_admin_api.vpc_cidr]
  }

  egress {
    description     = "HTTPS to S3 via gateway endpoint (audit writes)"
    from_port       = 443
    to_port         = 443
    protocol        = "tcp"
    prefix_list_ids = [local.cfg.export_admin_api.s3_prefix_list_id]
  }

  tags = local.export_admin_api_tags
}

# ---------------------------------------------------------------------------
# Logging: explicitly provisioned log group, org retention, CMK-encrypted
# ---------------------------------------------------------------------------
resource "aws_cloudwatch_log_group" "export_admin_api_logs" {
  name              = local.export_admin_api_log_group_name
  retention_in_days = local.cfg.export_admin_api.log_retention_days
  kms_key_id        = aws_kms_key.export_admin_api_kms.arn

  tags = local.export_admin_api_tags
}

# ---------------------------------------------------------------------------
# Compute: Export Admin API (Lambda, immutable artifact, VPC-attached)
# ---------------------------------------------------------------------------
resource "aws_lambda_function" "export_admin_api" {
  function_name = "${var.project_name}-${var.env}-export-admin-api"
  description   = "Export Admin API (do-admin-api): CRUD for export configurations, manual trigger and stop"
  role          = aws_iam_role.export_admin_api_role.arn

  runtime     = "python3.11"
  handler     = "dataoutbound.export_admin_api.app.lambda_handler"
  memory_size = local.cfg.export_admin_api.memory_mb
  timeout     = local.cfg.export_admin_api.timeout_seconds
  publish     = true

  s3_bucket        = local.cfg.export_admin_api.artifact_bucket
  s3_key           = local.cfg.export_admin_api.artifact_key
  source_code_hash = local.cfg.export_admin_api.artifact_sha256

  reserved_concurrent_executions = local.cfg.export_admin_api.reserved_concurrency

  kms_key_arn = aws_kms_key.export_admin_api_kms.arn

  # Non-secret runtime configuration only; credentials are resolved from SSM at runtime
  environment {
    variables = {
      DB_HOST            = aws_db_instance.export_config_store_rds.address
      DB_PORT            = tostring(aws_db_instance.export_config_store_rds.port)
      DB_NAME            = local.cfg.export_admin_api.db_name
      DB_USER            = local.cfg.export_admin_api.db_user
      DB_IAM_AUTH        = "true"
      DB_SSL_MODE        = "require"
      MWAA_ENVIRONMENT   = aws_mwaa_environment.orchestrator_dag_mwaa.name
      MWAA_WEBSERVER_URL = aws_mwaa_environment.orchestrator_dag_mwaa.webserver_url
      AUDIT_BUCKET       = aws_s3_bucket.audit_execution_log_store_s3.bucket
      AUDIT_PREFIX       = "export-admin-api/"
      KMS_KEY_ID         = aws_kms_key.export_admin_api_kms.key_id
      JWT_ISSUER         = local.cfg.export_admin_api.jwt_issuer
      JWT_AUDIENCE       = local.cfg.export_admin_api.jwt_audience
      REQUIRED_ROLE      = local.cfg.export_admin_api.required_role
    }
  }

  vpc_config {
    subnet_ids         = local.cfg.export_admin_api.private_subnet_ids
    security_group_ids = [aws_security_group.export_admin_api_sg.id]
  }

  logging_config {
    log_format = "JSON"
    log_group  = aws_cloudwatch_log_group.export_admin_api_logs.name
  }

  tags = local.export_admin_api_tags

  depends_on = [
    aws_iam_role_policy.export_admin_api_policy,
    aws_iam_role_policy_attachment.export_admin_api_vpc_access,
    aws_cloudwatch_log_group.export_admin_api_logs,
  ]
}

# HTTPS endpoint for REST clients; callers must hold lambda:InvokeFunctionUrl and sign with SigV4
resource "aws_lambda_function_url" "export_admin_api_url" {
  function_name      = aws_lambda_function.export_admin_api.function_name
  authorization_type = "AWS_IAM"
  invoke_mode        = "BUFFERED"
}

# ---------------------------------------------------------------------------
# Observability: alarms per tech spec section 4.4.5
# ---------------------------------------------------------------------------
resource "aws_cloudwatch_metric_alarm" "export_admin_api_errors" {
  alarm_name          = "${var.project_name}-${var.env}-export-admin-api-errors"
  alarm_description   = "Export Admin API: >= 5 errors in 5 minutes"
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 5
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  dimensions = {
    FunctionName = aws_lambda_function.export_admin_api.function_name
  }
  alarm_actions = [local.cfg.export_admin_api.alerts_topic_arn]

  tags = local.export_admin_api_tags
}

resource "aws_cloudwatch_metric_alarm" "export_admin_api_throttles" {
  alarm_name          = "${var.project_name}-${var.env}-export-admin-api-throttles"
  alarm_description   = "Export Admin API: any throttle in 5 minutes"
  namespace           = "AWS/Lambda"
  metric_name         = "Throttles"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  dimensions = {
    FunctionName = aws_lambda_function.export_admin_api.function_name
  }
  alarm_actions = [local.cfg.export_admin_api.alerts_topic_arn]

  tags = local.export_admin_api_tags
}

resource "aws_cloudwatch_metric_alarm" "export_admin_api_duration_p99" {
  alarm_name          = "${var.project_name}-${var.env}-export-admin-api-duration-p99"
  alarm_description   = "Export Admin API: p99 duration > 12000 ms over 5 minutes"
  namespace           = "AWS/Lambda"
  metric_name         = "Duration"
  extended_statistic  = "p99"
  period              = 300
  evaluation_periods  = 1
  threshold           = 12000
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  dimensions = {
    FunctionName = aws_lambda_function.export_admin_api.function_name
  }
  alarm_actions = [local.cfg.export_admin_api.alerts_topic_arn]

  tags = local.export_admin_api_tags
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------
output "export_admin_api_function_url" {
  description = "HTTPS endpoint of the Export Admin API (Lambda Function URL, JWT-authorised at the application layer)"
  value       = aws_lambda_function_url.export_admin_api.function_url
}

output "export_admin_api_function_name" {
  description = "Physical name of the Export Admin API Lambda function"
  value       = aws_lambda_function.export_admin_api.function_name
}

output "export_admin_api_role_arn" {
  description = "ARN of the least-privilege execution role for the Export Admin API"
  value       = aws_iam_role.export_admin_api_role.arn
}
# laminar:node:imp-standard-standard_api-1:end
