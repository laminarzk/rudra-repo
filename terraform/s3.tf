# laminar:node:imp-aws-aws_s3_bucket-15:begin
# ---------------------------------------------------------------------------
# Audit & Execution Log Store (imp-aws-aws_s3_bucket-15)
# Append-protected, KMS-encrypted, object-locked audit bucket.
# ---------------------------------------------------------------------------

locals {
  audit_execution_log_store_s3_bucket_name = "${var.project_name}-${var.env}-audit-execution-log-store-s3"
  audit_execution_log_store_s3_bucket_arn  = "arn:${data.aws_partition.current.partition}:s3:::${local.audit_execution_log_store_s3_bucket_name}"
  audit_execution_log_store_s3_tags = {
    Component  = "audit-execution-log-store"
    Owner      = "data-platform-engineering@acme.com"
    CostCenter = local.cfg.audit_execution_log_store_s3.cost_center
  }
}

# --- KMS customer-managed key -------------------------------------------------

data "aws_iam_policy_document" "audit_execution_log_store_s3_kms" {
  # Mandatory account-root administration statement (self-reference).
  statement {
    sid    = "EnableIamUserPermissions"
    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = ["arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:root"]
    }

    actions   = ["kms:*"]
    resources = ["*"]
  }

  # Only the audit bucket, in this account, via S3, may use the key.
  statement {
    sid    = "AllowAuditBucketS3Encryption"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["s3.amazonaws.com"]
    }

    actions = [
      "kms:Decrypt",
      "kms:GenerateDataKey*",
    ]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }

    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values   = [local.audit_execution_log_store_s3_bucket_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["s3.${data.aws_region.current.name}.amazonaws.com"]
    }
  }
}

resource "aws_kms_key" "audit_execution_log_store_s3_kms" {
  description             = "CMK for ${local.audit_execution_log_store_s3_bucket_name}"
  enable_key_rotation     = true
  deletion_window_in_days = 30
  policy                  = data.aws_iam_policy_document.audit_execution_log_store_s3_kms.json
  tags                    = local.audit_execution_log_store_s3_tags

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_kms_alias" "audit_execution_log_store_s3_kms" {
  name          = "alias/${local.audit_execution_log_store_s3_bucket_name}"
  target_key_id = aws_kms_key.audit_execution_log_store_s3_kms.key_id
}

# --- Bucket -----------------------------------------------------------------

resource "aws_s3_bucket" "audit_execution_log_store_s3" {
  bucket              = local.audit_execution_log_store_s3_bucket_name
  force_destroy       = local.cfg.audit_execution_log_store_s3.force_destroy
  object_lock_enabled = true
  tags                = local.audit_execution_log_store_s3_tags
}

resource "aws_s3_bucket_ownership_controls" "audit_execution_log_store_s3" {
  bucket = aws_s3_bucket.audit_execution_log_store_s3.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_public_access_block" "audit_execution_log_store_s3" {
  bucket = aws_s3_bucket.audit_execution_log_store_s3.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "audit_execution_log_store_s3" {
  bucket = aws_s3_bucket.audit_execution_log_store_s3.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "audit_execution_log_store_s3" {
  bucket = aws_s3_bucket.audit_execution_log_store_s3.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.audit_execution_log_store_s3_kms.arn
    }
    bucket_key_enabled = true
  }
}

# WORM: objects cannot be overwritten or deleted before the retention period.
resource "aws_s3_bucket_object_lock_configuration" "audit_execution_log_store_s3" {
  bucket = aws_s3_bucket.audit_execution_log_store_s3.id

  rule {
    default_retention {
      mode = local.cfg.audit_execution_log_store_s3.object_lock_mode
      days = local.cfg.audit_execution_log_store_s3.log_retention_days
    }
  }

  depends_on = [aws_s3_bucket_versioning.audit_execution_log_store_s3]
}

resource "aws_s3_bucket_lifecycle_configuration" "audit_execution_log_store_s3" {
  bucket = aws_s3_bucket.audit_execution_log_store_s3.id

  rule {
    id     = "audit-log-tiering-and-retention"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }

    transition {
      days          = local.cfg.audit_execution_log_store_s3.transition_days
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = local.cfg.audit_execution_log_store_s3.glacier_days
      storage_class = "GLACIER"
    }

    expiration {
      days = local.cfg.audit_execution_log_store_s3.log_retention_days
    }

    noncurrent_version_transition {
      noncurrent_days = local.cfg.audit_execution_log_store_s3.glacier_days
      storage_class   = "GLACIER"
    }

    noncurrent_version_expiration {
      noncurrent_days = local.cfg.audit_execution_log_store_s3.log_retention_days
    }
  }

  depends_on = [aws_s3_bucket_versioning.audit_execution_log_store_s3]
}

# --- Bucket policy: scoped denies only --------------------------------------
# Producer writes (EMR Serverless, MWAA, Export Admin API) are granted by the
# producer roles' own least-privilege IAM policies; this policy never grants
# access, it only blocks non-TLS access and writes not encrypted with this CMK.

data "aws_iam_policy_document" "audit_execution_log_store_s3_policy" {
  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    resources = [
      local.audit_execution_log_store_s3_bucket_arn,
      "${local.audit_execution_log_store_s3_bucket_arn}/*",
    ]

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }

  statement {
    sid     = "DenyWritesNotEncryptedWithAuditCmk"
    effect  = "Deny"
    actions = ["s3:PutObject"]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    resources = ["${local.audit_execution_log_store_s3_bucket_arn}/*"]

    condition {
      test     = "StringEquals"
      variable = "s3:x-amz-server-side-encryption"
      values   = ["aws:kms"]
    }

    condition {
      test     = "StringNotEquals"
      variable = "s3:x-amz-server-side-encryption-aws-kms-key-id"
      values   = [aws_kms_key.audit_execution_log_store_s3_kms.arn]
    }
  }
}

resource "aws_s3_bucket_policy" "audit_execution_log_store_s3" {
  bucket = aws_s3_bucket.audit_execution_log_store_s3.id
  policy = data.aws_iam_policy_document.audit_execution_log_store_s3_policy.json

  depends_on = [aws_s3_bucket_public_access_block.audit_execution_log_store_s3]
}

# --- Outputs ----------------------------------------------------------------

output "audit_execution_log_store_s3_bucket_id" {
  value = aws_s3_bucket.audit_execution_log_store_s3.id
}

output "audit_execution_log_store_s3_bucket_arn" {
  value = aws_s3_bucket.audit_execution_log_store_s3.arn
}

output "audit_execution_log_store_s3_kms_key_arn" {
  value = aws_kms_key.audit_execution_log_store_s3_kms.arn
}
# laminar:node:imp-aws-aws_s3_bucket-15:end

# laminar:node:imp-aws-aws_s3_bucket-8:begin
# ---------------------------------------------------------------------------
# Component: Customer Staging Buckets (S3) - stage 1 (imp-aws-aws_s3_bucket-8)
# Written by Export Extract Spark Job (conn-imp-9), read by Transform & Encrypt
# DAG (conn-imp-12, stage 2 is a separate module), and ObjectCreated trigger
# to Egress Lambda (conn-imp-10).
# ---------------------------------------------------------------------------

resource "aws_kms_key" "customer_staging_buckets_s3_kms" {
  description             = "CMK for ${var.project_name}-${var.env} customer staging buckets"
  enable_key_rotation     = true
  deletion_window_in_days = local.cfg.customer_staging_buckets_s3.kms_deletion_window_days

  # The account-root statement is the mandatory self-reference that delegates
  # key administration and lets IAM identity policies (EMR Serverless job role,
  # MWAA execution role) grant kms:GenerateDataKey / kms:Decrypt explicitly.
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "EnableIAMPolicyDelegation"
        Effect    = "Allow"
        Principal = { AWS = "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:root" }
        Action    = "kms:*"
        Resource  = "*"
      }
    ]
  })

  tags = {
    Component  = "customer_staging_buckets_s3"
    Owner      = "data-platform-engineering@acme.com"
    CostCenter = local.cfg.customer_staging_buckets_s3.cost_center
  }
}

resource "aws_kms_alias" "customer_staging_buckets_s3_kms" {
  name          = "alias/${var.project_name}-${var.env}-customer-staging-buckets-s3-key"
  target_key_id = aws_kms_key.customer_staging_buckets_s3_kms.key_id
}

resource "aws_s3_bucket" "customer_staging_buckets_s3" {
  bucket = "${var.project_name}-${var.env}-customer-staging-buckets-s3"

  # Deletion protection driven by environment config:
  # dev = true (teardown may delete non-empty bucket), staging/prod = false
  # (terraform destroy fails while objects exist).
  force_destroy = local.cfg.customer_staging_buckets_s3.force_destroy

  tags = {
    Component  = "customer_staging_buckets_s3"
    Owner      = "data-platform-engineering@acme.com"
    CostCenter = local.cfg.customer_staging_buckets_s3.cost_center
  }
}

resource "aws_s3_bucket_ownership_controls" "customer_staging_buckets_s3" {
  bucket = aws_s3_bucket.customer_staging_buckets_s3.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_public_access_block" "customer_staging_buckets_s3" {
  bucket = aws_s3_bucket.customer_staging_buckets_s3.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "customer_staging_buckets_s3" {
  bucket = aws_s3_bucket.customer_staging_buckets_s3.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.customer_staging_buckets_s3_kms.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_versioning" "customer_staging_buckets_s3" {
  bucket = aws_s3_bucket.customer_staging_buckets_s3.id

  versioning_configuration {
    status = "Enabled"
  }
}

# SCRUM-486 AC#5: every staged object expires after exactly 7 days.
# Noncurrent versions are expired on the same horizon so versioning
# does not cause temporary exports to accumulate.
resource "aws_s3_bucket_lifecycle_configuration" "customer_staging_buckets_s3" {
  bucket = aws_s3_bucket.customer_staging_buckets_s3.id

  depends_on = [aws_s3_bucket_versioning.customer_staging_buckets_s3]

  rule {
    id     = "expire-staged-export-files"
    status = "Enabled"

    filter {}

    expiration {
      days = local.cfg.customer_staging_buckets_s3.lifecycle_expiration_days
    }

    noncurrent_version_expiration {
      noncurrent_days = local.cfg.customer_staging_buckets_s3.noncurrent_version_expiration_days
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

resource "aws_s3_bucket_policy" "customer_staging_buckets_s3" {
  bucket = aws_s3_bucket.customer_staging_buckets_s3.id

  depends_on = [aws_s3_bucket_public_access_block.customer_staging_buckets_s3]

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "DenyInsecureTransport"
        Effect    = "Deny"
        Principal = "*"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.customer_staging_buckets_s3.arn,
          "${aws_s3_bucket.customer_staging_buckets_s3.arn}/*"
        ]
        Condition = {
          Bool = { "aws:SecureTransport" = "false" }
        }
      }
    ]
  })
}

# Trigger edge conn-imp-10: S3 ObjectCreated -> Egress Lambda.
resource "aws_lambda_permission" "customer_staging_buckets_s3_egress_invoke" {
  statement_id   = "AllowStagingBucketObjectCreated"
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.egress_lambda.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = aws_s3_bucket.customer_staging_buckets_s3.arn
  source_account = data.aws_caller_identity.current.account_id
}

resource "aws_s3_bucket_notification" "customer_staging_buckets_s3" {
  bucket = aws_s3_bucket.customer_staging_buckets_s3.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.egress_lambda.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = local.cfg.customer_staging_buckets_s3.event_filter_prefix
  }

  depends_on = [aws_lambda_permission.customer_staging_buckets_s3_egress_invoke]
}

output "customer_staging_buckets_s3_bucket_id" {
  value = aws_s3_bucket.customer_staging_buckets_s3.id
}

output "customer_staging_buckets_s3_arn" {
  value = aws_s3_bucket.customer_staging_buckets_s3.arn
}

output "customer_staging_buckets_s3_kms_key_arn" {
  value = aws_kms_key.customer_staging_buckets_s3_kms.arn
}
# laminar:node:imp-aws-aws_s3_bucket-8:end
