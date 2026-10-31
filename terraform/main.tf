# Root module. Resource definitions are grouped by service type:
#   api.tf
#   blob_store.tf
#   emr.tf
#   eventbridge.tf
#   gcs.tf
#   lambda.tf
#   mwaa.tf
#   rds.tf
#   redshift.tf
#   s3.tf
# Per-environment values live in values.yaml and are read through local.cfg.

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Environment = var.env
      Project     = var.project_name
      ManagedBy   = "Terraform"
    }
  }
}

locals {
  env = var.env
  cfg = yamldecode(file("${path.module}/values.yaml"))[var.env]
}

# Shared lookups — declared once here; fragments reference data.<type>.current.
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
data "aws_partition" "current" {}
