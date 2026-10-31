# laminar:node:imp-aws-aws_mwaa_dag-10:begin
# ---------------------------------------------------------------------------
# Transform & Encrypt DAG (MWAA) : resource name base = transform_encrypt_dag_mwaa
# The DAG runs on the shared MWAA environment (single environment per tech spec).
# This fragment provisions the DAG artifact, its CMK, scoped execution policy,
# network boundary, audit log group and failure dead-letter queue.
# ---------------------------------------------------------------------------

locals {
  transform_encrypt_dag_mwaa_log_group_name = "/aws/laminar/mwaa/transform-encrypt-dag/${var.env}"
  transform_encrypt_dag_mwaa_log_group_arn  = "arn:${data.aws_partition.current.partition}:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:${local.transform_encrypt_dag_mwaa_log_group_name}"
}

# ---------- Customer-managed KMS key ----------------------------------------

resource "aws_kms_key" "transform_encrypt_dag_mwaa_kms" {
  description             = "CMK for Transform & Encrypt DAG (${var.env})"
  enable_key_rotation     = true
  deletion_window_in_days = local.cfg.transform_encrypt_dag_mwaa.kms_deletion_window_days
  policy                  = data.aws_iam_policy_document.transform_encrypt_dag_mwaa_kms.json
}

resource "aws_kms_alias" "transform_encrypt_dag_mwaa_kms" {
  name          = "alias/${var.project_name}-${var.env}-transform-encrypt-dag-mwaa"
  target_key_id = aws_kms_key.transform_encrypt_dag_mwaa_kms.key_id
}

data "aws_iam_policy_document" "transform_encrypt_dag_mwaa_kms" {
  # Mandatory key self-reference: account root administration (AWS default statement).
  statement {
    sid    = "EnableAccountRootAdministration"
    effect = "Allow"
    principals {
      type        = "AWS"
      identifiers = ["arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:root"]
    }
    actions   = ["kms:*"]
    resources = ["*"]
  }

  statement {
    sid    = "AllowTransformDagExecutionRole"
    effect = "Allow"
    principals {
      type        = "AWS"
      identifiers = [aws_iam_role.transform_encrypt_dag_mwaa_role.arn]
    }
    actions   = ["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
    resources = ["*"]
  }

  statement {
    sid    = "AllowCloudWatchLogsForTransformDagGroup"
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["logs.${data.aws_region.current.name}.amazonaws.com"]
    }
    actions   = ["kms:Encrypt", "kms:Decrypt", "kms:ReEncrypt*", "kms:GenerateDataKey*", "kms:DescribeKey"]
    resources = ["*"]
    condition {
      test     = "ArnEquals"
      variable = "kms:EncryptionContext:aws:logs:arn"
      values   = [local.transform_encrypt_dag_mwaa_log_group_arn]
    }
  }
}

# ---------- Execution role and least-privilege policy -----------------------

data "aws_iam_policy_document" "transform_encrypt_dag_mwaa_trust" {
  statement {
    sid     = "AllowMwaaAssumeRole"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["airflow.amazonaws.com", "airflow-env.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

data "aws_iam_policy_document" "transform_encrypt_dag_mwaa_policy" {
  # Read staged export files only (never write unencrypted packages back to staging).
  statement {
    sid       = "StagingReadExportObjects"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.customer_staging_buckets_s3.arn}/${local.cfg.transform_encrypt_dag_mwaa.staging_input_prefix}*"]
  }

  statement {
    sid       = "StagingListExportPrefix"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.customer_staging_buckets_s3.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${local.cfg.transform_encrypt_dag_mwaa.staging_input_prefix}*"]
    }
  }

  # Delivery to the customer S3 destination (write + read-back for SHA-256 verification).
  statement {
    sid       = "CustomerS3DestinationDelivery"
    effect    = "Allow"
    actions   = ["s3:PutObject", "s3:GetObject"]
    resources = ["${local.cfg.transform_encrypt_dag_mwaa.customer_s3_destination_arn}/${local.cfg.transform_encrypt_dag_mwaa.customer_s3_destination_prefix}*"]
  }

  # Transfer audit events to the Audit & Execution Log Store.
  statement {
    sid       = "AuditStoreWriteTransferEvents"
    effect    = "Allow"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.audit_execution_log_store_s3.arn}/${local.cfg.transform_encrypt_dag_mwaa.audit_prefix}*"]
  }

  # Customer Azure/GCS/SFTP credentials: SecureString parameters for this DAG only.
  statement {
    sid    = "SsmCustomerDestinationCredentials"
    effect = "Allow"
    actions = [
      "ssm:GetParameter",
      "ssm:GetParameters"
    ]
    resources = [
      "arn:${data.aws_partition.current.partition}:ssm:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:parameter/dataoutbound/${var.env}/transform-encrypt-dag/*"
    ]
  }

  statement {
    sid    = "KmsUseTransformDagKey"
    effect = "Allow"
    actions = [
      "kms:Decrypt",
      "kms:GenerateDataKey",
      "kms:DescribeKey"
    ]
    resources = [aws_kms_key.transform_encrypt_dag_mwaa_kms.arn]
  }

  statement {
    sid    = "WriteTransformDagLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["${local.transform_encrypt_dag_mwaa_log_group_arn}:*"]
  }

  # Failed transfers after retries are routed to the dead-letter path.
  statement {
    sid       = "SendFailedTransferToDeadLetter"
    effect    = "Allow"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.transform_encrypt_dag_mwaa_dlq.arn]
  }
}

resource "aws_iam_role" "transform_encrypt_dag_mwaa_role" {
  name               = "${var.project_name}-${var.env}-transform-encrypt-dag-mwaa-role"
  description        = "Execution role for the Transform & Encrypt DAG on the shared MWAA environment"
  assume_role_policy = data.aws_iam_policy_document.transform_encrypt_dag_mwaa_trust.json
}

resource "aws_iam_policy" "transform_encrypt_dag_mwaa_policy" {
  name        = "${var.project_name}-${var.env}-transform-encrypt-dag-mwaa-policy"
  description = "Least-privilege permissions for the Transform & Encrypt DAG"
  policy      = data.aws_iam_policy_document.transform_encrypt_dag_mwaa_policy.json
}

resource "aws_iam_role_policy_attachment" "transform_encrypt_dag_mwaa_role_attach" {
  role       = aws_iam_role.transform_encrypt_dag_mwaa_role.name
  policy_arn = aws_iam_policy.transform_encrypt_dag_mwaa_policy.arn
}

# ---------- Network boundary -------------------------------------------------

resource "aws_security_group" "transform_encrypt_dag_mwaa_sg" {
  name        = "${var.project_name}-${var.env}-transform-encrypt-dag-mwaa-sg"
  description = "Transform & Encrypt DAG worker boundary: no public ingress"
  vpc_id      = local.cfg.transform_encrypt_dag_mwaa.vpc_id

  ingress {
    description = "MWAA worker-to-scheduler traffic within the security group"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    self        = true
  }

  egress {
    description = "HTTPS to AWS endpoints (S3, KMS, SSM), Azure Blob and GCS over TLS"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    description = "SSH to customer SFTP endpoint (strict host-key verification in DAG code)"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [local.cfg.transform_encrypt_dag_mwaa.sftp_cidr]
  }
}

# ---------- Observability and failure path -----------------------------------

resource "aws_cloudwatch_log_group" "transform_encrypt_dag_mwaa_logs" {
  name              = local.transform_encrypt_dag_mwaa_log_group_name
  retention_in_days = local.cfg.transform_encrypt_dag_mwaa.log_retention_days
  kms_key_id        = aws_kms_key.transform_encrypt_dag_mwaa_kms.arn
}

resource "aws_sqs_queue" "transform_encrypt_dag_mwaa_dlq" {
  name                              = "${var.project_name}-${var.env}-transform-encrypt-dag-mwaa-dlq"
  message_retention_seconds         = local.cfg.transform_encrypt_dag_mwaa.dlq_message_retention_seconds
  kms_master_key_id                 = aws_kms_key.transform_encrypt_dag_mwaa_kms.arn
  kms_data_key_reuse_period_seconds = 300
}

# ---------- DAG artifact (deployed to the shared MWAA DAGs bucket) -----------

resource "aws_s3_object" "transform_encrypt_dag_mwaa" {
  bucket                 = aws_s3_bucket.orchestrator_dag_mwaa_dags.id
  key                    = "dags/dataoutbound/transform_encrypt_dag.py"
  source                 = "${path.root}/../dags/dataoutbound/transform_encrypt_dag.py"
  etag                   = filemd5("${path.root}/../dags/dataoutbound/transform_encrypt_dag.py")
  server_side_encryption = "aws:kms"
  kms_key_id             = aws_kms_key.transform_encrypt_dag_mwaa_kms.arn
}

# ---------- Outputs -----------------------------------------------------------

output "transform_encrypt_dag_mwaa_dag_key" {
  value = aws_s3_object.transform_encrypt_dag_mwaa.key
}

output "transform_encrypt_dag_mwaa_role_arn" {
  value = aws_iam_role.transform_encrypt_dag_mwaa_role.arn
}

output "transform_encrypt_dag_mwaa_policy_arn" {
  value = aws_iam_policy.transform_encrypt_dag_mwaa_policy.arn
}

output "transform_encrypt_dag_mwaa_kms_key_arn" {
  value = aws_kms_key.transform_encrypt_dag_mwaa_kms.arn
}

output "transform_encrypt_dag_mwaa_sg_id" {
  value = aws_security_group.transform_encrypt_dag_mwaa_sg.id
}

output "transform_encrypt_dag_mwaa_log_group_name" {
  value = aws_cloudwatch_log_group.transform_encrypt_dag_mwaa_logs.name
}

output "transform_encrypt_dag_mwaa_dlq_url" {
  value = aws_sqs_queue.transform_encrypt_dag_mwaa_dlq.url
}
# laminar:node:imp-aws-aws_mwaa_dag-10:end

# laminar:node:imp-aws-aws_mwaa_dag-3:begin
# ---- BEGIN component: export_orchestrator_dag_mwaa ----
# The DAG itself (dag_id "orchestrator") is code deployed to the DAGs bucket below;
# it is not a Terraform resource. The MWAA environment hosts it.

locals {
  export_orchestrator_dag_mwaa_name = "${var.project_name}-${var.env}-export-orchestrator-dag-mwaa"

  export_orchestrator_dag_mwaa_env_arn = "arn:${data.aws_partition.current.partition}:airflow:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:environment/${local.export_orchestrator_dag_mwaa_name}"

  export_orchestrator_dag_mwaa_log_types = toset(["DAGProcessing", "Scheduler", "Task", "WebServer", "Worker"])

  export_orchestrator_dag_mwaa_tags = {
    Component  = "export-orchestrator-dag-mwaa"
    Owner      = "data-platform-engineering@acme.com"
    CostCenter = "dataoutbound-${var.env}-01"
  }
}

# ---------------------------------------------------------------- KMS (CMK)

resource "aws_kms_key" "export_orchestrator_dag_mwaa_kms" {
  description             = "CMK for the export orchestrator MWAA environment, DAGs bucket and log groups"
  enable_key_rotation     = true
  deletion_window_in_days = local.cfg.export_orchestrator_dag_mwaa.kms_deletion_window_days

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "EnableIAMRootAdministration"
        Effect    = "Allow"
        Principal = { AWS = "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:root" }
        Action    = "kms:*"
        Resource  = "*"
      },
      {
        Sid       = "AllowCloudWatchLogsEncryption"
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
          ArnLike = {
            "kms:EncryptionContext:aws:logs:arn" = "arn:${data.aws_partition.current.partition}:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:airflow-${local.export_orchestrator_dag_mwaa_name}-*"
          }
        }
      },
      {
        Sid       = "AllowExecutionRoleViaService"
        Effect    = "Allow"
        Principal = { AWS = aws_iam_role.export_orchestrator_dag_mwaa_role.arn }
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ]
        Resource = "*"
        Condition = {
          StringEquals = {
            "kms:ViaService" = [
              "s3.${data.aws_region.current.name}.amazonaws.com",
              "sqs.${data.aws_region.current.name}.amazonaws.com"
            ]
          }
        }
      },
      {
        Sid       = "AllowMwaaServiceScoped"
        Effect    = "Allow"
        Principal = { Service = ["airflow.amazonaws.com", "airflow-env.amazonaws.com"] }
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ]
        Resource = "*"
        Condition = {
          StringEquals = {
            "aws:SourceAccount" = data.aws_caller_identity.current.account_id
          }
          ArnLike = {
            "aws:SourceArn" = local.export_orchestrator_dag_mwaa_env_arn
          }
        }
      }
    ]
  })

  tags = local.export_orchestrator_dag_mwaa_tags
}

resource "aws_kms_alias" "export_orchestrator_dag_mwaa_kms" {
  name          = "alias/${var.project_name}-${var.env}-export-orchestrator-dag-mwaa-kms"
  target_key_id = aws_kms_key.export_orchestrator_dag_mwaa_kms.key_id
}

# ---------------------------------------------------------------- IAM

resource "aws_iam_role" "export_orchestrator_dag_mwaa_role" {
  name = "${var.project_name}-${var.env}-export-orchestrator-dag-mwaa-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "MwaaExecutionTrust"
        Effect    = "Allow"
        Principal = { Service = ["airflow.amazonaws.com", "airflow-env.amazonaws.com"] }
        Action    = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "aws:SourceAccount" = data.aws_caller_identity.current.account_id
          }
        }
      }
    ]
  })

  tags = local.export_orchestrator_dag_mwaa_tags
}

resource "aws_iam_role_policy" "export_orchestrator_dag_mwaa_policy" {
  name = "${var.project_name}-${var.env}-export-orchestrator-dag-mwaa-policy"
  role = aws_iam_role.export_orchestrator_dag_mwaa_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "PublishMwaaMetrics"
        Effect   = "Allow"
        Action   = ["airflow:PublishMetrics"]
        Resource = local.export_orchestrator_dag_mwaa_env_arn
      },
      {
        Sid    = "ReadDagsBucket"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          "arn:${data.aws_partition.current.partition}:s3:::${var.project_name}-${var.env}-mwaa-dags",
          "arn:${data.aws_partition.current.partition}:s3:::${var.project_name}-${var.env}-mwaa-dags/*"
        ]
      },
      {
        Sid    = "WriteMwaaTaskLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:GetLogEvents",
          "logs:DescribeLogStreams"
        ]
        Resource = "arn:${data.aws_partition.current.partition}:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/laminar/mwaa/orchestrator/${var.env}:*"
      },
      {
        Sid      = "ConnectExportConfigStore"
        Effect   = "Allow"
        Action   = ["rds-db:connect"]
        Resource = "arn:${data.aws_partition.current.partition}:rds-db:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:dbuser:${aws_db_instance.export_config_store_rds.resource_id}/${local.cfg.export_orchestrator_dag_mwaa.config_db_user}"
      },
      {
        Sid      = "ReadDataoutboundParameters"
        Effect   = "Allow"
        Action   = ["ssm:GetParameter", "ssm:GetParameters"]
        Resource = "arn:${data.aws_partition.current.partition}:ssm:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:parameter/dataoutbound/*"
      },
      {
        Sid      = "UseComponentKmsKey"
        Effect   = "Allow"
        Action   = ["kms:Decrypt", "kms:GenerateDataKey*", "kms:DescribeKey"]
        Resource = aws_kms_key.export_orchestrator_dag_mwaa_kms.arn
      }
    ]
  })
}

output "export_orchestrator_dag_mwaa_role_arn" {
  value = aws_iam_role.export_orchestrator_dag_mwaa_role.arn
}
# laminar:node:imp-aws-aws_mwaa_dag-3:end

# laminar:node:imp-aws-aws_mwaa_dag-4:begin
# ---------------------------------------------------------------------------
# Child Export DAG (MWAA) — component region: child_export_dag_mwaa
# ---------------------------------------------------------------------------

locals {
  child_export_dag_mwaa_name_prefix = "${var.project_name}-${var.env}-child-export-dag-mwaa"
  child_export_dag_mwaa_log_group   = "/aws/${var.project_name}/mwaa/child-export-dag/${var.env}"
  child_export_dag_mwaa_log_arn     = "arn:${data.aws_partition.current.partition}:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:${local.child_export_dag_mwaa_log_group}"
}

# Customer-managed KMS key: encrypts the DAG log group and any DAG-scoped
# artifacts. Deletion window is environment-driven so dev can be torn down.
# NOTE: lifecycle.prevent_destroy cannot take an expression, so a literal
# `true` would block SCRUM-485 dev teardown. Deletion protection is therefore
# enforced through config-driven attributes (deletion_window_in_days) instead.
resource "aws_kms_key" "child_export_dag_mwaa_kms" {
  description             = "CMK for Child Export DAG (MWAA) logs and DAG-scoped artifacts (${var.env})"
  enable_key_rotation     = true
  deletion_window_in_days = local.cfg.child_export_dag_mwaa.kms_deletion_window_days

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
        Sid       = "AllowChildExportExecRoleUse"
        Effect    = "Allow"
        Principal = { AWS = aws_iam_role.child_export_dag_mwaa_role.arn }
        Action    = ["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
        Resource  = "*"
      },
      {
        Sid       = "AllowCloudWatchLogsForChildExportLogGroup"
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
          ArnLike = {
            "kms:EncryptionContext:aws:logs:arn" = local.child_export_dag_mwaa_log_arn
          }
        }
      }
    ]
  })

  tags = {
    Component  = "child_export_dag"
    Owner      = "data-platform-engineering@acme.com"
    CostCenter = local.cfg.child_export_dag_mwaa.cost_center
  }
}

resource "aws_kms_alias" "child_export_dag_mwaa_kms" {
  name          = "alias/${local.child_export_dag_mwaa_name_prefix}-key"
  target_key_id = aws_kms_key.child_export_dag_mwaa_kms.key_id
}

# Explicitly provisioned log group (SCRUM-486): retention is the single
# organisation logging-standard value supplied via local.cfg.
resource "aws_cloudwatch_log_group" "child_export_dag_mwaa_logs" {
  name              = local.child_export_dag_mwaa_log_group
  retention_in_days = local.cfg.child_export_dag_mwaa.log_retention_days
  kms_key_id        = aws_kms_key.child_export_dag_mwaa_kms.arn

  tags = {
    Component  = "child_export_dag"
    Owner      = "data-platform-engineering@acme.com"
    CostCenter = local.cfg.child_export_dag_mwaa.cost_center
  }
}

# Execution identity for the Child Export DAG (MWAA). Trusted by the MWAA
# service principals and scoped to the source account.
resource "aws_iam_role" "child_export_dag_mwaa_role" {
  name = "${local.child_export_dag_mwaa_name_prefix}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowMwaaServiceAssume"
        Effect = "Allow"
        Principal = {
          Service = ["airflow.amazonaws.com", "airflow-env.amazonaws.com"]
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
    Component  = "child_export_dag"
    Owner      = "data-platform-engineering@acme.com"
    CostCenter = local.cfg.child_export_dag_mwaa.cost_center
  }
}

# Least-privilege policy: every action is enumerated and every resource is
# a concrete address or ARN derived from another resource.
resource "aws_iam_role_policy" "child_export_dag_mwaa_policy" {
  name = "${local.child_export_dag_mwaa_name_prefix}-policy"
  role = aws_iam_role.child_export_dag_mwaa_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowEmrServerlessJobRunLifecycle"
        Effect = "Allow"
        Action = [
          "emr-serverless:StartJobRun",
          "emr-serverless:GetJobRun",
          "emr-serverless:CancelJobRun"
        ]
        Resource = [
          aws_emrserverless_application.export_extract_spark_job_emr.arn,
          "${aws_emrserverless_application.export_extract_spark_job_emr.arn}/jobruns/*"
        ]
      },
      {
        Sid      = "AllowPassEmrExecutionRole"
        Effect   = "Allow"
        Action   = ["iam:PassRole"]
        Resource = aws_iam_role.export_extract_spark_job_emr_role.arn
        Condition = {
          StringEquals = {
            "iam:PassedToService" = "emr-serverless.amazonaws.com"
          }
        }
      },
      {
        Sid      = "AllowConfigStoreIamDbConnect"
        Effect   = "Allow"
        Action   = ["rds-db:connect"]
        Resource = "arn:${data.aws_partition.current.partition}:rds-db:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:dbuser:${aws_db_instance.export_config_store_rds.resource_id}/child_export_reader"
      },
      {
        Sid    = "AllowStagingBucketAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.customer_staging_buckets_s3.arn,
          "${aws_s3_bucket.customer_staging_buckets_s3.arn}/*"
        ]
      },
      {
        Sid      = "AllowAuditEventWrites"
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = "${aws_s3_bucket.audit_execution_log_store_s3.arn}/*"
      },
      {
        Sid    = "AllowComponentKmsUse"
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey",
          "kms:DescribeKey"
        ]
        Resource = aws_kms_key.child_export_dag_mwaa_kms.arn
      },
      {
        Sid    = "AllowComponentLogWrites"
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogStreams"
        ]
        Resource = "${aws_cloudwatch_log_group.child_export_dag_mwaa_logs.arn}:*"
      }
    ]
  })
}

# Managed prefix list for S3 gateway endpoint egress (no hardcoded IPs).
data "aws_prefix_list" "child_export_dag_mwaa_s3" {
  name = "com.amazonaws.${data.aws_region.current.name}.s3"
}

# Worker security group: private VPC only, no public ingress, narrow egress.
resource "aws_security_group" "child_export_dag_mwaa_sg" {
  name        = "${local.child_export_dag_mwaa_name_prefix}-sg"
  description = "Child Export DAG (MWAA) workers: Config Store, S3 gateway and VPC endpoint egress only"
  vpc_id      = local.cfg.child_export_dag_mwaa.vpc_id

  ingress {
    description = "Worker-to-scheduler traffic within the MWAA security group"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    self        = true
  }

  egress {
    description = "PostgreSQL to Export Config Store isolated DB subnets"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = local.cfg.child_export_dag_mwaa.db_subnet_cidrs
  }

  egress {
    description     = "HTTPS to S3 via gateway endpoint"
    from_port       = 443
    to_port         = 443
    protocol        = "tcp"
    prefix_list_ids = [data.aws_prefix_list.child_export_dag_mwaa_s3.id]
  }

  egress {
    description = "HTTPS to interface VPC endpoints (EMR Serverless, KMS, STS, CloudWatch Logs)"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [local.cfg.child_export_dag_mwaa.vpc_cidr]
  }

  tags = {
    Component  = "child_export_dag"
    Owner      = "data-platform-engineering@acme.com"
    CostCenter = local.cfg.child_export_dag_mwaa.cost_center
  }
}

output "child_export_dag_mwaa_role_arn" {
  value = aws_iam_role.child_export_dag_mwaa_role.arn
}

output "child_export_dag_mwaa_role_name" {
  value = aws_iam_role.child_export_dag_mwaa_role.name
}

output "child_export_dag_mwaa_log_group_name" {
  value = aws_cloudwatch_log_group.child_export_dag_mwaa_logs.name
}

output "child_export_dag_mwaa_kms_key_arn" {
  value = aws_kms_key.child_export_dag_mwaa_kms.arn
}

output "child_export_dag_mwaa_sg_id" {
  value = aws_security_group.child_export_dag_mwaa_sg.id
}
# laminar:node:imp-aws-aws_mwaa_dag-4:end
