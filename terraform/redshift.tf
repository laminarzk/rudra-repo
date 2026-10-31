# laminar:node:imp-aws-aws_redshift-6:begin
locals {
  golden_entity_source_store_redshift_cfg  = local.cfg.golden_entity_source_store_redshift
  golden_entity_source_store_redshift_name = "${var.project_name}-${var.env}-golden-entity-source-store-redshift"
  golden_entity_source_store_redshift_tags = {
    Component  = "golden-entity-source-store-redshift"
    Owner      = "data-platform-engineering@acme.com"
    CostCenter = "dataoutbound-${var.env}-01"
  }
}

# ---------------------------------------------------------------------------
# Network lookups (VPC and private data subnets are owned by the network layer)
# ---------------------------------------------------------------------------
data "aws_vpc" "golden_entity_source_store_redshift" {
  filter {
    name   = "tag:Name"
    values = [local.golden_entity_source_store_redshift_cfg.vpc_name]
  }
}

data "aws_subnets" "golden_entity_source_store_redshift_private" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.golden_entity_source_store_redshift.id]
  }

  filter {
    name   = "tag:Tier"
    values = [local.golden_entity_source_store_redshift_cfg.subnet_tier]
  }
}

# ---------------------------------------------------------------------------
# KMS customer-managed key
# ---------------------------------------------------------------------------
data "aws_iam_policy_document" "golden_entity_source_store_redshift_kms" {
  # AWS-default account-root administration statement (accepted self-reference).
  statement {
    sid       = "EnableIamUserPermissions"
    effect    = "Allow"
    actions   = ["kms:*"]
    resources = ["*"]

    principals {
      type        = "AWS"
      identifiers = ["arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:root"]
    }
  }

  # Redshift cluster use of the key, restricted to this account and to the Redshift service.
  statement {
    sid    = "AllowRedshiftServiceUseOfKey"
    effect = "Allow"
    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:ReEncrypt*",
      "kms:GenerateDataKey*",
      "kms:DescribeKey",
      "kms:CreateGrant"
    ]
    resources = ["*"]

    principals {
      type        = "AWS"
      identifiers = ["*"]
    }

    condition {
      test     = "StringEquals"
      variable = "kms:CallerAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }

    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["redshift.${data.aws_region.current.name}.amazonaws.com"]
    }
  }
}

resource "aws_kms_key" "golden_entity_source_store_redshift_kms" {
  description             = "CMK encrypting the Golden Entity Source Store Redshift cluster (${local.golden_entity_source_store_redshift_name})"
  enable_key_rotation     = true
  deletion_window_in_days = local.golden_entity_source_store_redshift_cfg.kms_deletion_window_days
  policy                  = data.aws_iam_policy_document.golden_entity_source_store_redshift_kms.json

  tags = local.golden_entity_source_store_redshift_tags
}

resource "aws_kms_alias" "golden_entity_source_store_redshift_kms" {
  name          = "alias/${local.golden_entity_source_store_redshift_name}-key"
  target_key_id = aws_kms_key.golden_entity_source_store_redshift_kms.key_id
}

# ---------------------------------------------------------------------------
# Network: subnet group and least-privilege security group
# ---------------------------------------------------------------------------
resource "aws_redshift_subnet_group" "golden_entity_source_store_redshift_subnet_group" {
  name        = "${local.golden_entity_source_store_redshift_name}-subnets"
  description = "Private data subnets for the Golden Entity Source Store Redshift cluster"
  subnet_ids  = data.aws_subnets.golden_entity_source_store_redshift_private.ids

  tags = local.golden_entity_source_store_redshift_tags
}

resource "aws_security_group" "golden_entity_source_store_redshift_sg" {
  name        = "${local.golden_entity_source_store_redshift_name}-sg"
  description = "Allows JDBC (5439) to Redshift only from the Spark extract and orchestration subnets"
  vpc_id      = data.aws_vpc.golden_entity_source_store_redshift.id

  # No egress block: Terraform removes the default allow-all egress rule.
  ingress {
    description = "Redshift JDBC from EMR Serverless and MWAA subnets"
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = local.golden_entity_source_store_redshift_cfg.ingress_cidr_blocks
  }

  tags = local.golden_entity_source_store_redshift_tags
}

# ---------------------------------------------------------------------------
# Workload management and TLS enforcement
# ---------------------------------------------------------------------------
resource "aws_redshift_parameter_group" "golden_entity_source_store_redshift_params" {
  name        = "${local.golden_entity_source_store_redshift_name}-params"
  family      = "redshift-1.0"
  description = "Enforces TLS and user activity logging for the Golden Entity Source Store"

  parameter {
    name  = "require_ssl"
    value = "true"
  }

  parameter {
    name  = "enable_user_activity_logging"
    value = "true"
  }

  tags = local.golden_entity_source_store_redshift_tags
}

# ---------------------------------------------------------------------------
# Read-only access role assumed by the Export Extract Spark Job (EMR Serverless)
# ---------------------------------------------------------------------------
data "aws_iam_policy_document" "golden_entity_source_store_redshift_trust" {
  statement {
    sid     = "AllowEmrServerlessAssumeReadOnlyRole"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_iam_role" "golden_entity_source_store_redshift_role" {
  name               = "${local.golden_entity_source_store_redshift_name}-role"
  assume_role_policy = data.aws_iam_policy_document.golden_entity_source_store_redshift_trust.json

  tags = local.golden_entity_source_store_redshift_tags
}

data "aws_iam_policy_document" "golden_entity_source_store_redshift_access" {
  statement {
    sid    = "AllowIamAuthenticatedReadConnection"
    effect = "Allow"
    actions = [
      "redshift:GetClusterCredentialsWithIAM"
    ]
    resources = [
      "arn:${data.aws_partition.current.partition}:redshift:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:dbname:${aws_redshift_cluster.golden_entity_source_store_redshift.cluster_identifier}/${local.golden_entity_source_store_redshift_cfg.database_name}"
    ]
  }

  statement {
    sid    = "AllowDescribeSourceCluster"
    effect = "Allow"
    actions = [
      "redshift:DescribeClusters"
    ]
    resources = [
      "arn:${data.aws_partition.current.partition}:redshift:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:cluster:${aws_redshift_cluster.golden_entity_source_store_redshift.cluster_identifier}"
    ]
  }
}

resource "aws_iam_role_policy" "golden_entity_source_store_redshift_policy" {
  name   = "${local.golden_entity_source_store_redshift_name}-policy"
  role   = aws_iam_role.golden_entity_source_store_redshift_role.id
  policy = data.aws_iam_policy_document.golden_entity_source_store_redshift_access.json
}

# ---------------------------------------------------------------------------
# Redshift cluster (Golden Entity Source Store)
# ---------------------------------------------------------------------------
resource "aws_redshift_cluster" "golden_entity_source_store_redshift" {
  cluster_identifier = local.golden_entity_source_store_redshift_name
  database_name      = local.golden_entity_source_store_redshift_cfg.database_name
  node_type          = local.golden_entity_source_store_redshift_cfg.node_type
  number_of_nodes    = local.golden_entity_source_store_redshift_cfg.number_of_nodes
  cluster_type       = local.golden_entity_source_store_redshift_cfg.number_of_nodes > 1 ? "multi-node" : "single-node"

  # Credentials: AWS-managed master secret encrypted with the component CMK (no plaintext in state or code).
  master_username                   = "dataoutbound_admin"
  manage_master_password            = true
  master_password_secret_kms_key_id = aws_kms_key.golden_entity_source_store_redshift_kms.key_id

  port                         = 5439
  encrypted                    = true
  kms_key_id                   = aws_kms_key.golden_entity_source_store_redshift_kms.arn
  publicly_accessible          = false
  enhanced_vpc_routing         = true
  cluster_subnet_group_name    = aws_redshift_subnet_group.golden_entity_source_store_redshift_subnet_group.name
  vpc_security_group_ids       = [aws_security_group.golden_entity_source_store_redshift_sg.id]
  cluster_parameter_group_name = aws_redshift_parameter_group.golden_entity_source_store_redshift_params.name

  allow_version_upgrade               = true
  automated_snapshot_retention_period = local.golden_entity_source_store_redshift_cfg.automated_snapshot_retention_days

  # Deletion protection driven by environment configuration: when enabled, a final snapshot is taken on destroy.
  skip_final_snapshot       = !local.golden_entity_source_store_redshift_cfg.deletion_protection
  final_snapshot_identifier = local.golden_entity_source_store_redshift_cfg.deletion_protection ? "${local.golden_entity_source_store_redshift_name}-final" : null

  tags = local.golden_entity_source_store_redshift_tags
}

resource "aws_redshift_logging" "golden_entity_source_store_redshift_logging" {
  cluster_identifier   = aws_redshift_cluster.golden_entity_source_store_redshift.cluster_identifier
  log_destination_type = "cloudwatch"
  log_exports          = ["connectionlog", "useractivitylog", "userlog"]
}

# ---------------------------------------------------------------------------
# Outputs (consumed by the Export Extract Spark Job edge conn-imp-8)
# ---------------------------------------------------------------------------
output "golden_entity_source_store_redshift_arn" {
  value = aws_redshift_cluster.golden_entity_source_store_redshift.arn
}

output "golden_entity_source_store_redshift_cluster_identifier" {
  value = aws_redshift_cluster.golden_entity_source_store_redshift.cluster_identifier
}

output "golden_entity_source_store_redshift_dns_name" {
  value = aws_redshift_cluster.golden_entity_source_store_redshift.dns_name
}

output "golden_entity_source_store_redshift_port" {
  value = aws_redshift_cluster.golden_entity_source_store_redshift.port
}

output "golden_entity_source_store_redshift_jdbc_connection_url" {
  value = "jdbc:redshift://${aws_redshift_cluster.golden_entity_source_store_redshift.dns_name}:${aws_redshift_cluster.golden_entity_source_store_redshift.port}/${aws_redshift_cluster.golden_entity_source_store_redshift.database_name}?ssl=true"
}

output "golden_entity_source_store_redshift_security_group_id" {
  value = aws_security_group.golden_entity_source_store_redshift_sg.id
}

output "golden_entity_source_store_redshift_role_arn" {
  value = aws_iam_role.golden_entity_source_store_redshift_role.arn
}

output "golden_entity_source_store_redshift_kms_key_arn" {
  value = aws_kms_key.golden_entity_source_store_redshift_kms.arn
}
# laminar:node:imp-aws-aws_redshift-6:end
