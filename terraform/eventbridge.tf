# laminar:node:imp-aws-aws_eventbridge-2:begin
locals {
  export_scheduler_eventbridge_name         = "${var.project_name}-${var.env}-export-scheduler-eventbridge"
  export_scheduler_eventbridge_trigger_name = "${var.project_name}-${var.env}-export-scheduler-eventbridge-trigger"

  export_scheduler_eventbridge_tags = {
    Component  = "export-scheduler"
    Owner      = "data-platform-engineering@acme.com"
    CostCenter = "dataoutbound-${var.env}-01"
  }
}

# ---------------------------------------------------------------------------
# Customer-managed KMS key (encrypts DLQ, trigger Lambda logs and alarm topic)
# ---------------------------------------------------------------------------
resource "aws_kms_key" "export_scheduler_eventbridge_kms" {
  description             = "CMK for ${local.export_scheduler_eventbridge_name}"
  enable_key_rotation     = true
  deletion_window_in_days = local.cfg.export_scheduler_eventbridge.kms_deletion_window_days

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "EnableAccountRootAdministration"
        Effect    = "Allow"
        Principal = { AWS = "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:root" }
        Action    = "kms:*"
        Resource  = "*"
      },
      {
        Sid       = "AllowEventBridgeDlqEncryption"
        Effect    = "Allow"
        Principal = { Service = "events.amazonaws.com" }
        Action    = ["kms:GenerateDataKey*", "kms:Decrypt"]
        Resource  = "*"
        Condition = {
          StringEquals = { "aws:SourceAccount" = data.aws_caller_identity.current.account_id }
          ArnLike      = { "aws:SourceArn" = aws_cloudwatch_event_rule.export_scheduler_eventbridge.arn }
        }
      },
      {
        Sid       = "AllowTriggerLambdaRoleViaSqs"
        Effect    = "Allow"
        Principal = { AWS = aws_iam_role.export_scheduler_eventbridge_trigger_role.arn }
        Action    = ["kms:GenerateDataKey*", "kms:Decrypt"]
        Resource  = "*"
        Condition = {
          StringEquals = { "kms:ViaService" = "sqs.${data.aws_region.current.name}.amazonaws.com" }
        }
      },
      {
        Sid       = "AllowCloudWatchLogsEncryption"
        Effect    = "Allow"
        Principal = { Service = "logs.${data.aws_region.current.name}.amazonaws.com" }
        Action    = ["kms:Encrypt*", "kms:Decrypt*", "kms:ReEncrypt*", "kms:GenerateDataKey*", "kms:Describe*"]
        Resource  = "*"
        Condition = {
          ArnLike = {
            "kms:EncryptionContext:aws:logs:arn" = "arn:${data.aws_partition.current.partition}:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${local.export_scheduler_eventbridge_trigger_name}"
          }
        }
      },
      {
        Sid       = "AllowCloudWatchAlarmsToPublishToEncryptedTopic"
        Effect    = "Allow"
        Principal = { Service = "cloudwatch.amazonaws.com" }
        Action    = ["kms:GenerateDataKey*", "kms:Decrypt"]
        Resource  = "*"
        Condition = {
          StringEquals = { "aws:SourceAccount" = data.aws_caller_identity.current.account_id }
        }
      }
    ]
  })

  tags = local.export_scheduler_eventbridge_tags

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_kms_alias" "export_scheduler_eventbridge_kms" {
  name          = "alias/${local.export_scheduler_eventbridge_name}-key"
  target_key_id = aws_kms_key.export_scheduler_eventbridge_kms.key_id
}

# ---------------------------------------------------------------------------
# Export Scheduler: EventBridge rule (cron cadence)
# ---------------------------------------------------------------------------
resource "aws_cloudwatch_event_rule" "export_scheduler_eventbridge" {
  name                = local.export_scheduler_eventbridge_name
  description         = "Scheduled cadence trigger for the Export Orchestrator DAG (${var.env})"
  schedule_expression = local.cfg.export_scheduler_eventbridge.schedule_expression
  state               = local.cfg.export_scheduler_eventbridge.rule_state

  tags = local.export_scheduler_eventbridge_tags
}

# ---------------------------------------------------------------------------
# Dead-letter queue for rule target and async Lambda failures
# ---------------------------------------------------------------------------
resource "aws_sqs_queue" "export_scheduler_eventbridge_dlq" {
  name                              = "${local.export_scheduler_eventbridge_name}-dlq"
  kms_master_key_id                 = aws_kms_key.export_scheduler_eventbridge_kms.arn
  kms_data_key_reuse_period_seconds = 300
  message_retention_seconds         = local.cfg.export_scheduler_eventbridge.dlq_message_retention_seconds

  tags = local.export_scheduler_eventbridge_tags
}

resource "aws_sqs_queue_policy" "export_scheduler_eventbridge_dlq" {
  queue_url = aws_sqs_queue.export_scheduler_eventbridge_dlq.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowEventBridgeRuleSendMessage"
        Effect    = "Allow"
        Principal = { Service = "events.amazonaws.com" }
        Action    = "sqs:SendMessage"
        Resource  = aws_sqs_queue.export_scheduler_eventbridge_dlq.arn
        Condition = {
          ArnEquals = { "aws:SourceArn" = aws_cloudwatch_event_rule.export_scheduler_eventbridge.arn }
        }
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# Scheduler trigger Lambda (mediator: EventBridge -> MWAA REST API)
# ---------------------------------------------------------------------------
resource "aws_cloudwatch_log_group" "export_scheduler_eventbridge_trigger" {
  name              = "/aws/lambda/${local.export_scheduler_eventbridge_trigger_name}"
  retention_in_days = local.cfg.export_scheduler_eventbridge.log_retention_days
  kms_key_id        = aws_kms_key.export_scheduler_eventbridge_kms.arn

  tags = local.export_scheduler_eventbridge_tags
}

resource "aws_iam_role" "export_scheduler_eventbridge_trigger_role" {
  name = "${local.export_scheduler_eventbridge_trigger_name}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowLambdaAssumeRole"
        Effect    = "Allow"
        Principal = { Service = "lambda.amazonaws.com" }
        Action    = "sts:AssumeRole"
        Condition = {
          StringEquals = { "aws:SourceAccount" = data.aws_caller_identity.current.account_id }
        }
      }
    ]
  })

  tags = local.export_scheduler_eventbridge_tags
}

resource "aws_iam_role_policy" "export_scheduler_eventbridge_trigger_policy" {
  name = "${local.export_scheduler_eventbridge_trigger_name}-policy"
  role = aws_iam_role.export_scheduler_eventbridge_trigger_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "AllowTriggerLambdaLogs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:${data.aws_partition.current.partition}:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${local.export_scheduler_eventbridge_trigger_name}:*"
      },
      {
        Sid      = "AllowMwaaWebLoginTokenForOrchestratorDagRun"
        Effect   = "Allow"
        Action   = ["airflow:CreateWebLoginToken"]
        Resource = aws_mwaa_environment.orchestrator_dag_mwaa.arn
      },
      {
        Sid      = "AllowFailureDestinationSend"
        Effect   = "Allow"
        Action   = ["sqs:SendMessage"]
        Resource = aws_sqs_queue.export_scheduler_eventbridge_dlq.arn
      },
      {
        Sid      = "AllowKmsForDlqEncryption"
        Effect   = "Allow"
        Action   = ["kms:Decrypt", "kms:GenerateDataKey*"]
        Resource = aws_kms_key.export_scheduler_eventbridge_kms.arn
      },
      {
        Sid      = "AllowScheduledTriggerAuditWrite"
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = "${aws_s3_bucket.audit_execution_log_store_s3.arn}/*"
      }
    ]
  })
}

resource "aws_lambda_function" "export_scheduler_eventbridge_trigger" {
  function_name    = local.export_scheduler_eventbridge_trigger_name
  role             = aws_iam_role.export_scheduler_eventbridge_trigger_role.arn
  runtime          = "python3.11"
  handler          = "lambda_function.lambda_handler"
  filename         = local.cfg.export_scheduler_eventbridge.trigger_package_path
  source_code_hash = filebase64sha256(local.cfg.export_scheduler_eventbridge.trigger_package_path)
  memory_size      = local.cfg.export_scheduler_eventbridge.trigger_memory_mb
  timeout          = local.cfg.export_scheduler_eventbridge.trigger_timeout_seconds

  environment {
    variables = {
      MWAA_ENVIRONMENT_NAME = aws_mwaa_environment.orchestrator_dag_mwaa.name
      ORCHESTRATOR_DAG_ID   = "orchestrator"
      AUDIT_BUCKET          = aws_s3_bucket.audit_execution_log_store_s3.bucket
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.export_scheduler_eventbridge_trigger,
    aws_iam_role_policy.export_scheduler_eventbridge_trigger_policy,
  ]

  tags = local.export_scheduler_eventbridge_tags
}

resource "aws_lambda_function_event_invoke_config" "export_scheduler_eventbridge_trigger" {
  function_name                = aws_lambda_function.export_scheduler_eventbridge_trigger.function_name
  maximum_retry_attempts       = 2
  maximum_event_age_in_seconds = local.cfg.export_scheduler_eventbridge.max_event_age_seconds

  destination_config {
    on_failure {
      destination = aws_sqs_queue.export_scheduler_eventbridge_dlq.arn
    }
  }
}

resource "aws_cloudwatch_event_target" "export_scheduler_eventbridge_trigger" {
  rule      = aws_cloudwatch_event_rule.export_scheduler_eventbridge.name
  target_id = "export-scheduler-trigger"
  arn       = aws_lambda_function.export_scheduler_eventbridge_trigger.arn

  retry_policy {
    maximum_retry_attempts       = local.cfg.export_scheduler_eventbridge.max_retry_attempts
    maximum_event_age_in_seconds = local.cfg.export_scheduler_eventbridge.max_event_age_seconds
  }

  dead_letter_config {
    arn = aws_sqs_queue.export_scheduler_eventbridge_dlq.arn
  }
}

resource "aws_lambda_permission" "export_scheduler_eventbridge_invoke" {
  statement_id  = "AllowEventBridgeInvokeTrigger"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.export_scheduler_eventbridge_trigger.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.export_scheduler_eventbridge.arn
}

# ---------------------------------------------------------------------------
# Alerting
# ---------------------------------------------------------------------------
resource "aws_sns_topic" "export_scheduler_eventbridge_alerts" {
  name              = "${local.export_scheduler_eventbridge_name}-alerts"
  kms_master_key_id = aws_kms_key.export_scheduler_eventbridge_kms.arn

  tags = local.export_scheduler_eventbridge_tags
}

resource "aws_cloudwatch_metric_alarm" "export_scheduler_eventbridge_failed_invocations" {
  alarm_name          = "${local.export_scheduler_eventbridge_name}-failed-invocations"
  alarm_description   = "Scheduler events failed delivery to the trigger Lambda after retries"
  namespace           = "AWS/Events"
  metric_name         = "FailedInvocations"
  dimensions          = { RuleName = aws_cloudwatch_event_rule.export_scheduler_eventbridge.name }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = local.cfg.export_scheduler_eventbridge.failed_invocations_threshold
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.export_scheduler_eventbridge_alerts.arn]

  tags = local.export_scheduler_eventbridge_tags
}

resource "aws_cloudwatch_metric_alarm" "export_scheduler_eventbridge_dlq_depth" {
  alarm_name          = "${local.export_scheduler_eventbridge_name}-dlq-depth"
  alarm_description   = "Undelivered scheduler events are waiting in the DLQ"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.export_scheduler_eventbridge_dlq.name }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.export_scheduler_eventbridge_alerts.arn]

  tags = local.export_scheduler_eventbridge_tags
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------
output "export_scheduler_eventbridge_rule_arn" {
  value = aws_cloudwatch_event_rule.export_scheduler_eventbridge.arn
}

output "export_scheduler_eventbridge_rule_name" {
  value = aws_cloudwatch_event_rule.export_scheduler_eventbridge.name
}

output "export_scheduler_eventbridge_dlq_url" {
  value = aws_sqs_queue.export_scheduler_eventbridge_dlq.url
}

output "export_scheduler_eventbridge_kms_key_arn" {
  value = aws_kms_key.export_scheduler_eventbridge_kms.arn
}

output "export_scheduler_eventbridge_trigger_lambda_arn" {
  value = aws_lambda_function.export_scheduler_eventbridge_trigger.arn
}

output "export_scheduler_eventbridge_alerts_topic_arn" {
  value = aws_sns_topic.export_scheduler_eventbridge_alerts.arn
}
# laminar:node:imp-aws-aws_eventbridge-2:end
