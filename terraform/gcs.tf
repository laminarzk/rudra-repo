# laminar:node:imp-gcp-gcs-14:begin
# ---------------------------------------------------------------------------
# Customer GCS Destination (imp-gcp-gcs-14) - resource name base:
# customer_gcs_destination_gcs
# The GCS bucket is customer-owned (SCRUM-485 exclusion) and is NOT created here.
# ---------------------------------------------------------------------------

resource "aws_kms_key" "customer_gcs_destination_gcs_kms" {
  description             = "${var.project_name}-${var.env} CMK for Customer GCS Destination delivery secrets"
  enable_key_rotation     = true
  deletion_window_in_days = local.cfg.customer_gcs_destination_gcs.kms_deletion_window_days

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "EnableIAMUserPermissions"
        Effect    = "Allow"
        Principal = { AWS = "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:root" }
        Action    = "kms:*"
        Resource  = "*"
      },
      {
        Sid       = "AllowSSMDecryptViaServiceInAccount"
        Effect    = "Allow"
        Principal = { AWS = "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:root" }
        Action    = ["kms:Decrypt", "kms:DescribeKey"]
        Resource  = "*"
        Condition = {
          StringEquals = {
            "kms:ViaService"    = "ssm.${data.aws_region.current.name}.amazonaws.com"
            "kms:CallerAccount" = data.aws_caller_identity.current.account_id
          }
        }
      }
    ]
  })

  tags = {
    Component  = "customer_gcs_destination_gcs"
    Owner      = "data-platform-engineering@acme.com"
    CostCenter = "dataoutbound-${var.env}-01"
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_kms_alias" "customer_gcs_destination_gcs_kms_alias" {
  name          = "alias/${var.project_name}-${var.env}-customer-gcs-destination-gcs"
  target_key_id = aws_kms_key.customer_gcs_destination_gcs_kms.key_id
}

# Primary resource: destination contract consumed by the Transform & Encrypt DAG.
# Matches edge conn-imp-16 fields: gcs_bucket_name, gcs_object_path, service_account_secret.
# Non-secret configuration, so type String.
resource "aws_ssm_parameter" "customer_gcs_destination_gcs" {
  name        = local.cfg.customer_gcs_destination_gcs.destination_config_param_name
  description = "Customer GCS Destination delivery contract for ${var.project_name}-${var.env}"
  type        = "String"
  value = jsonencode({
    gcs_bucket_name        = local.cfg.customer_gcs_destination_gcs.gcs_bucket_name
    gcs_object_path        = local.cfg.customer_gcs_destination_gcs.gcs_object_path
    service_account_secret = local.cfg.customer_gcs_destination_gcs.service_account_secret_param_name
  })

  tags = {
    Component  = "customer_gcs_destination_gcs"
    Owner      = "data-platform-engineering@acme.com"
    CostCenter = "dataoutbound-${var.env}-01"
  }
}

# GCS service-account key JSON. Stored only as SecureString under KMS CMK.
# The real key is injected out-of-band (CLI/console) and rotated there; Terraform
# never receives or stores the key material.
resource "aws_ssm_parameter" "customer_gcs_destination_gcs_sa_secret" {
  name        = local.cfg.customer_gcs_destination_gcs.service_account_secret_param_name
  description = "GCS service-account JSON key for Customer GCS Destination (rotated out-of-band)"
  type        = "SecureString"
  key_id      = aws_kms_key.customer_gcs_destination_gcs_kms.arn
  value       = "PLACEHOLDER-INJECT-OUT-OF-BAND"

  tags = {
    Component  = "customer_gcs_destination_gcs"
    Owner      = "data-platform-engineering@acme.com"
    CostCenter = "dataoutbound-${var.env}-01"
  }

  lifecycle {
    ignore_changes = [value]
  }
}

# Least-privilege read policy. Attached to the Transform & Encrypt DAG execution
# role by the transform_encrypt_dag_mwaa component (via output below).
resource "aws_iam_policy" "customer_gcs_destination_gcs_policy" {
  name        = "${var.project_name}-${var.env}-customer-gcs-destination-gcs-read"
  description = "Read GCS destination contract and service-account secret for the Transform & Encrypt DAG"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadGcsDestinationParameters"
        Effect = "Allow"
        Action = ["ssm:GetParameter"]
        Resource = [
          aws_ssm_parameter.customer_gcs_destination_gcs.arn,
          aws_ssm_parameter.customer_gcs_destination_gcs_sa_secret.arn
        ]
      },
      {
        Sid      = "DecryptGcsDestinationSecretViaSSM"
        Effect   = "Allow"
        Action   = ["kms:Decrypt", "kms:DescribeKey"]
        Resource = aws_kms_key.customer_gcs_destination_gcs_kms.arn
        Condition = {
          StringEquals = {
            "kms:ViaService" = "ssm.${data.aws_region.current.name}.amazonaws.com"
          }
        }
      }
    ]
  })

  tags = {
    Component  = "customer_gcs_destination_gcs"
    Owner      = "data-platform-engineering@acme.com"
    CostCenter = "dataoutbound-${var.env}-01"
  }
}

output "customer_gcs_destination_gcs_config_param_name" {
  value = aws_ssm_parameter.customer_gcs_destination_gcs.name
}

output "customer_gcs_destination_gcs_sa_secret_param_name" {
  value = aws_ssm_parameter.customer_gcs_destination_gcs_sa_secret.name
}

output "customer_gcs_destination_gcs_kms_key_arn" {
  value = aws_kms_key.customer_gcs_destination_gcs_kms.arn
}

output "customer_gcs_destination_gcs_read_policy_arn" {
  value = aws_iam_policy.customer_gcs_destination_gcs_policy.arn
}
# laminar:node:imp-gcp-gcs-14:end
