# laminar:node:imp-aws-aws_lambda-9:begin
# [File Path: terraform/lambda.tf]  (fragment: egress_lambda)

locals {
  egress_lambda_function_name = "${var.project_name}-${var.env}-egress-lambda"
  egress_lambda_log_group     = "/aws/laminar/lambda/egress-lambda/${var.env}"
  egress_lambda_tags = {
    Component  = "egress-lambda"
    Owner      = "data-platform-engineering@acme.com"
    CostCenter = local.cfg.egress_lambda.cost_center
  }
}

# ---------------------------------------------------------------------------
# Encryption: customer-managed KMS key for DLQ, log group, alarm topic
# ---------------------------------------------------------------------------
resource "aws_kms_key" "egress_lambda_kms" {
  description             = "CMK for Egress Lambda DLQ, CloudWatch logs and alarm topic (${var.env})"
  enable_key_rotation     = true
  deletion_window_in_days = local.cfg.egress_lambda.kms_deletion_window_days

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
        Sid       = "AllowCloudWatchLogsUseOfKeyForOwnLogGroup"
        Effect    = "Allow"
        Principal = { Service = "logs.${data.aws_region.current.name}.amazonaws.com" }
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ]
        Resource = "*"
        Condition = {
          StringEquals = {
            "kms:EncryptionContext:aws:logs:arn" = "arn:${data.aws_partition.current.partition}:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:${local.egress_lambda_log_group}"
          }
        }
      },
      {
        Sid       = "AllowCloudWatchAlarmsToPublishToEncryptedTopic"
        Effect    = "Allow"
        Principal = { Service = "cloudwatch.amazonaws.com" }
        Action    = ["kms:Decrypt", "kms:GenerateDataKey*"]
        Resource  = "*"
        Condition = {
          StringEquals = {
            "aws:SourceAccount" = data.aws_caller_identity.current.account_id
          }
        }
      }
    ]
  })

  tags = local.egress_lambda_tags
}

resource "aws_kms_alias" "egress_lambda_kms" {
  name          = "alias/${var.project_name}-${var.env}-egress-lambda-key"
  target_key_id = aws_kms_key.egress_lambda_kms.key_id
}

# ---------------------------------------------------------------------------
# Dead-letter queue for retry-exhausted asynchronous invocations
# ---------------------------------------------------------------------------
resource "aws_sqs_queue" "egress_lambda_dlq" {
  name                              = "${var.project_name}-${var.env}-egress-lambda-dlq"
  kms_master_key_id                 = aws_kms_key.egress_lambda_kms.arn
  kms_data_key_reuse_period_seconds = 300
  message_retention_seconds         = 1209600

  tags = local.egress_lambda_tags
}

# ---------------------------------------------------------------------------
# Explicitly provisioned log group (retention from confirmed logging standard)
# ---------------------------------------------------------------------------
resource "aws_cloudwatch_log_group" "egress_lambda_logs" {
  name              = local.egress_lambda_log_group
  retention_in_days = local.cfg.egress_lambda.log_retention_days
  kms_key_id        = aws_kms_key.egress_lambda_kms.arn

  tags = local.egress_lambda_tags
}

# ---------------------------------------------------------------------------
# Execution role (least privilege) and inline policy
# ---------------------------------------------------------------------------
resource "aws_iam_role" "egress_lambda_role" {
  name = "${var.project_name}-${var.env}-egress-lambda-role"

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

  tags = local.egress_lambda_tags
}

resource "aws_iam_role_policy" "egress_lambda_policy" {
  name = "egress-lambda-least-privilege"
  role = aws_iam_role.egress_lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ReadStagedExportObjectsOnly"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = "${aws_s3_bucket.customer_staging_buckets_s3.arn}/${local.cfg.egress_lambda.staging_prefix}*"
      },
      {
        Sid      = "TriggerTransformEncryptDagRun"
        Effect   = "Allow"
        Action   = ["airflow:InvokeRestApi"]
        Resource = aws_mwaa_environment.orchestrator_dag_mwaa.arn
      },
      {
        Sid      = "WriteOwnLogStreams"
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:${data.aws_partition.current.partition}:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:${aws_cloudwatch_log_group.egress_lambda_logs.name}:*"
      },
      {
        Sid      = "SendToDeadLetterQueueOnly"
        Effect   = "Allow"
        Action   = ["sqs:SendMessage"]
        Resource = aws_sqs_queue.egress_lambda_dlq.arn
      },
      {
        Sid      = "UseCmkForDeadLetterQueueViaSqsOnly"
        Effect   = "Allow"
        Action   = ["kms:GenerateDataKey", "kms:Decrypt"]
        Resource = aws_kms_key.egress_lambda_kms.arn
        Condition = {
          StringEquals = {
            "kms:ViaService" = "sqs.${data.aws_region.current.name}.amazonaws.com"
          }
        }
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# Deployment package (source lives in the dataoutbound package root)
# ---------------------------------------------------------------------------
data "archive_file" "egress_lambda_package" {
  type        = "zip"
  source_dir  = "${path.root}/../dataoutbound/egress_lambda"
  output_path = "${path.root}/build/egress_lambda.zip"
}

# ---------------------------------------------------------------------------
# Lambda function (non-VPC, per node properties)
# ---------------------------------------------------------------------------
resource "aws_lambda_function" "egress_lambda" {
  function_name = local.egress_lambda_function_name
  role          = aws_iam_role.egress_lambda_role.arn
  runtime       = "python3.11"
  handler       = "lambda_function.lambda_handler"

  filename         = data.archive_file.egress_lambda_package.output_path
  source_code_hash = data.archive_file.egress_lambda_package.output_base64sha256

  memory_size                    = local.cfg.egress_lambda.memory_mb
  timeout                        = local.cfg.egress_lambda.timeout_seconds
  reserved_concurrent_executions = local.cfg.egress_lambda.reserved_concurrency

  environment {
    variables = {
      LOG_LEVEL             = local.cfg.egress_lambda.log_level
      TRANSFORM_DAG_ID      = local.cfg.egress_lambda.dag_id
      MWAA_ENVIRONMENT_NAME = aws_mwaa_environment.orchestrator_dag_mwaa.name
      STAGING_PREFIX        = local.cfg.egress_lambda.staging_prefix
    }
  }

  dead_letter_config {
    target_arn = aws_sqs_queue.egress_lambda_dlq.arn
  }

  tracing_config {
    mode = "PassThrough"
  }

  logging_config {
    log_format            = "JSON"
    log_group             = aws_cloudwatch_log_group.egress_lambda_logs.name
    application_log_level = local.cfg.egress_lambda.log_level
    system_log_level      = "WARN"
  }

  depends_on = [
    aws_iam_role_policy.egress_lambda_policy,
    aws_cloudwatch_log_group.egress_lambda_logs,
  ]

  tags = local.egress_lambda_tags
}

resource "aws_lambda_function_event_invoke_config" "egress_lambda_async" {
  function_name                = aws_lambda_function.egress_lambda.function_name
  maximum_event_age_in_seconds = 3600
  maximum_retry_attempts       = local.cfg.egress_lambda.max_retry_attempts
}

# ---------------------------------------------------------------------------
# Edge conn-imp-10: Customer Staging Buckets -> Egress Lambda (ObjectCreated)
# ---------------------------------------------------------------------------
resource "aws_lambda_permission" "egress_lambda_from_staging_s3" {
  statement_id   = "AllowExecutionFromStagingBucket"
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.egress_lambda.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = aws_s3_bucket.customer_staging_buckets_s3.arn
  source_account = data.aws_caller_identity.current.account_id
}

resource "aws_s3_bucket_notification" "egress_lambda_staging_trigger" {
  bucket = aws_s3_bucket.customer_staging_buckets_s3.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.egress_lambda.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = local.cfg.egress_lambda.staging_prefix
  }

  depends_on = [aws_lambda_permission.egress_lambda_from_staging_s3]
}

# ---------------------------------------------------------------------------
# Observability: encrypted alert topic, alarms on errors, throttles,
# duration (83% of timeout) and DLQ depth (retries exhausted)
# ---------------------------------------------------------------------------
resource "aws_sns_topic" "egress_lambda_alerts" {
  name              = "${var.project_name}-${var.env}-egress-lambda-alerts"
  kms_master_key_id = aws_kms_key.egress_lambda_kms.arn

  tags = local.egress_lambda_tags
}

resource "aws_cloudwatch_metric_alarm" "egress_lambda_errors" {
  alarm_name          = "${local.egress_lambda_function_name}-errors"
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  dimensions          = { FunctionName = aws_lambda_function.egress_lambda.function_name }
  statistic           = "Sum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.egress_lambda_alerts.arn]

  tags = local.egress_lambda_tags
}

resource "aws_cloudwatch_metric_alarm" "egress_lambda_throttles" {
  alarm_name          = "${local.egress_lambda_function_name}-throttles"
  namespace           = "AWS/Lambda"
  metric_name         = "Throttles"
  dimensions          = { FunctionName = aws_lambda_function.egress_lambda.function_name }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.egress_lambda_alerts.arn]

  tags = local.egress_lambda_tags
}

resource "aws_cloudwatch_metric_alarm" "egress_lambda_duration" {
  alarm_name          = "${local.egress_lambda_function_name}-duration"
  namespace           = "AWS/Lambda"
  metric_name         = "Duration"
  dimensions          = { FunctionName = aws_lambda_function.egress_lambda.function_name }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 1
  threshold           = floor(local.cfg.egress_lambda.timeout_seconds * 1000 * 0.83)
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.egress_lambda_alerts.arn]

  tags = local.egress_lambda_tags
}

resource "aws_cloudwatch_metric_alarm" "egress_lambda_dlq_depth" {
  alarm_name          = "${var.project_name}-${var.env}-egress-lambda-dlq-depth"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.egress_lambda_dlq.name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.egress_lambda_alerts.arn]

  tags = local.egress_lambda_tags
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------
output "egress_lambda_arn" {
  value = aws_lambda_function.egress_lambda.arn
}

output "egress_lambda_name" {
  value = aws_lambda_function.egress_lambda.function_name
}

output "egress_lambda_role_arn" {
  value = aws_iam_role.egress_lambda_role.arn
}

output "egress_lambda_dlq_arn" {
  value = aws_sqs_queue.egress_lambda_dlq.arn
}

output "egress_lambda_kms_key_arn" {
  value = aws_kms_key.egress_lambda_kms.arn
}

output "egress_lambda_log_group_name" {
  value = aws_cloudwatch_log_group.egress_lambda_logs.name
}
# laminar:node:imp-aws-aws_lambda-9:end
