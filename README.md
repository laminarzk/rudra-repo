<!-- laminar:generated:start -->
# DataOutbound Product

Generated and verified by the **Laminar Rudra** engine from the approved architecture and design specifications.

## Component Inventory

| Component | Service | Language | Tasks | Generated Paths |
|---|---|---|---|---|
| Audit & Execution Log Store | `aws_s3_bucket` | `-` | `terraform_gen` | `terraform/s3.tf`, `terraform/values.yaml` |
| Child Export DAG (MWAA) | `aws_mwaa_dag` | `python` | `terraform_gen`, `code_gen`, `manifest` | `src/dataoutbound/dags/child_export_dag.py`, `terraform/mwaa.tf`, `terraform/values.yaml` |
| Customer Azure Blob Destination | `blob_store` | `-` | `terraform_gen` | `terraform/blob_store.tf`, `terraform/values.yaml` |
| Customer GCS Destination | `gcs` | `-` | `terraform_gen` | `terraform/gcs.tf`, `terraform/values.yaml` |
| Customer Staging Buckets (S3) | `aws_s3_bucket` | `-` | `terraform_gen` | `terraform/s3.tf`, `terraform/values.yaml` |
| Egress Lambda | `aws_lambda` | `python` | `terraform_gen`, `code_gen`, `manifest` | `pyproject.toml`, `requirements.txt`, `src/dataoutbound/lambdas/egress_lambda.py`, `terraform/lambda.tf`, `terraform/values.yaml` |
| Export Admin API | `standard_api` | `python` | `terraform_gen`, `code_gen`, `manifest` | `pyproject.toml`, `requirements.txt`, `src/dataoutbound/apis/config.yml`, `src/dataoutbound/apis/export_admin_api/app.py`, `src/dataoutbound/apis/export_admin_api/dependencies.py`, `src/dataoutbound/apis/export_admin_api/export_admin_routes.py`, `src/dataoutbound/apis/export_admin_api/export_admin_schemas.py`, `src/dataoutbound/apis/export_admin_api/export_admin_service.py`, `terraform/api.tf`, `terraform/values.yaml` |
| Export Config Store (RDS Postgres) | `aws_rds` | `-` | `terraform_gen` | `terraform/rds.tf`, `terraform/values.yaml` |
| Export Extract Spark Job (EMR Serverless) | `aws_emr_serverless` | `python` | `terraform_gen`, `code_gen`, `manifest` | `pyproject.toml`, `requirements.txt`, `src/dataoutbound/spark_emr/export_extract_spark_job/config/config.yaml`, `src/dataoutbound/spark_emr/export_extract_spark_job/helper/spark_session_helper.py`, `src/dataoutbound/spark_emr/export_extract_spark_job/io/child_export_golden_entity_source_store_reader.py`, `src/dataoutbound/spark_emr/export_extract_spark_job/io/customer_staging_buckets_audit_execution_log_store_writer.py`, `src/dataoutbound/spark_emr/export_extract_spark_job/main.py`, `src/dataoutbound/spark_emr/export_extract_spark_job/processor/export_extract_input_processor.py`, `src/dataoutbound/spark_emr/export_extract_spark_job/processor/export_extract_output_processor.py`, `src/dataoutbound/spark_emr/export_extract_spark_job/transform/export_extract_transformer.py`, `src/dataoutbound/spark_emr/export_extract_spark_job/util/constants.py`, `src/dataoutbound/spark_emr/export_extract_spark_job/util/file_utils.py`, `terraform/emr.tf`, `terraform/values.yaml` |
| Export Orchestrator DAG (MWAA) | `aws_mwaa_dag` | `python` | `terraform_gen`, `code_gen`, `manifest` | `pyproject.toml`, `requirements.txt`, `src/dataoutbound/dags/export_orchestrator_dag.py`, `terraform/mwaa.tf`, `terraform/values.yaml` |
| Export Scheduler | `aws_eventbridge` | `-` | `terraform_gen` | `terraform/eventbridge.tf`, `terraform/values.yaml` |
| Golden Entity Source Store (Redshift) | `aws_redshift` | `-` | `terraform_gen` | `terraform/redshift.tf`, `terraform/values.yaml` |
| Shared foundation | `foundation` | `python` | `code_gen`, `manifest` | `pyproject.toml`, `requirements.txt`, `src/dataoutbound/common/clients/aws/emr/emr_client.py`, `src/dataoutbound/common/clients/aws/eventbridge/eventbridge_client.py`, `src/dataoutbound/common/clients/aws/mwaa/mwaa_client.py`, `src/dataoutbound/common/clients/aws/s3/s3_client.py`, `src/dataoutbound/common/clients/aws/session.py`, `src/dataoutbound/common/clients/aws/sqs/sqs_client.py`, `src/dataoutbound/common/clients/aws/ssm/ssm_client.py`, `src/dataoutbound/common/clients/azure/blob_client.py`, `src/dataoutbound/common/clients/db/db_client.py`, `src/dataoutbound/common/clients/gcp/gcs_client.py`, `src/dataoutbound/common/clients/rest/rest_client.py`, `src/dataoutbound/common/clients/sftp/sftp_client.py`, `src/dataoutbound/common/logging/logger.py`, `src/dataoutbound/common/security/auth.py`, `src/dataoutbound/common/telemetry/auditor.py`, `src/dataoutbound/config/common_config.yaml`, `src/dataoutbound/config/config_loader.py`, `src/dataoutbound/dags/config.yml`, `src/dataoutbound/lambdas/config.yml`, `src/dataoutbound/util/constants.py`, `src/dataoutbound/util/exceptions.py`, `src/dataoutbound/util/idempotency.py`, `src/dataoutbound/util/utils.py` |
| Transform & Encrypt DAG (MWAA) | `aws_mwaa_dag` | `python` | `terraform_gen`, `code_gen`, `manifest` | `pyproject.toml`, `requirements.txt`, `src/dataoutbound/dags/transform_encrypt_dag.py`, `terraform/mwaa.tf`, `terraform/values.yaml` |

## Repository Layout

### Application Code (`src/`)
- `src/dataoutbound/apis/config.yml`
- `src/dataoutbound/apis/export_admin_api/app.py`
- `src/dataoutbound/apis/export_admin_api/dependencies.py`
- `src/dataoutbound/apis/export_admin_api/export_admin_routes.py`
- `src/dataoutbound/apis/export_admin_api/export_admin_schemas.py`
- `src/dataoutbound/apis/export_admin_api/export_admin_service.py`
- `src/dataoutbound/common/clients/aws/emr/emr_client.py`
- `src/dataoutbound/common/clients/aws/eventbridge/eventbridge_client.py`
- `src/dataoutbound/common/clients/aws/mwaa/mwaa_client.py`
- `src/dataoutbound/common/clients/aws/s3/s3_client.py`
- `src/dataoutbound/common/clients/aws/session.py`
- `src/dataoutbound/common/clients/aws/sqs/sqs_client.py`
- `src/dataoutbound/common/clients/aws/ssm/ssm_client.py`
- `src/dataoutbound/common/clients/azure/blob_client.py`
- `src/dataoutbound/common/clients/db/db_client.py`
- `src/dataoutbound/common/clients/gcp/gcs_client.py`
- `src/dataoutbound/common/clients/rest/rest_client.py`
- `src/dataoutbound/common/clients/sftp/sftp_client.py`
- `src/dataoutbound/common/logging/logger.py`
- `src/dataoutbound/common/security/auth.py`
- `src/dataoutbound/common/telemetry/auditor.py`
- `src/dataoutbound/config/common_config.yaml`
- `src/dataoutbound/config/config_loader.py`
- `src/dataoutbound/dags/child_export_dag.py`
- `src/dataoutbound/dags/config.yml`
- `src/dataoutbound/dags/export_orchestrator_dag.py`
- `src/dataoutbound/dags/transform_encrypt_dag.py`
- `src/dataoutbound/lambdas/config.yml`
- `src/dataoutbound/lambdas/egress_lambda.py`
- `src/dataoutbound/spark_emr/export_extract_spark_job/config/config.yaml`
- `src/dataoutbound/spark_emr/export_extract_spark_job/helper/spark_session_helper.py`
- `src/dataoutbound/spark_emr/export_extract_spark_job/io/child_export_golden_entity_source_store_reader.py`
- `src/dataoutbound/spark_emr/export_extract_spark_job/io/customer_staging_buckets_audit_execution_log_store_writer.py`
- `src/dataoutbound/spark_emr/export_extract_spark_job/main.py`
- `src/dataoutbound/spark_emr/export_extract_spark_job/processor/export_extract_input_processor.py`
- `src/dataoutbound/spark_emr/export_extract_spark_job/processor/export_extract_output_processor.py`
- `src/dataoutbound/spark_emr/export_extract_spark_job/transform/export_extract_transformer.py`
- `src/dataoutbound/spark_emr/export_extract_spark_job/util/constants.py`
- `src/dataoutbound/spark_emr/export_extract_spark_job/util/file_utils.py`
- `src/dataoutbound/util/constants.py`
- `src/dataoutbound/util/exceptions.py`
- `src/dataoutbound/util/idempotency.py`
- `src/dataoutbound/util/utils.py`

### Infrastructure (`terraform/`)
- `terraform/api.tf`
- `terraform/blob_store.tf`
- `terraform/emr.tf`
- `terraform/eventbridge.tf`
- `terraform/gcs.tf`
- `terraform/lambda.tf`
- `terraform/main.tf`
- `terraform/mwaa.tf`
- `terraform/rds.tf`
- `terraform/redshift.tf`
- `terraform/s3.tf`
- `terraform/values.yaml`
- `terraform/variables.tf`

## Build, Lint & Test Instructions

### Python
```bash
pip install -r requirements.txt && pytest
ruff check .
```

### Terraform Infrastructure

One root module under `terraform/`: `main.tf` (providers, `local.cfg`), `variables.tf` (`env`), `values.yaml` (per-environment values: dev, staging, prod) and one `<service>.tf` per service type.

```bash
terraform -chdir=terraform init -backend=false && terraform -chdir=terraform validate
terraform -chdir=terraform plan -var env=staging   # values.yaml[staging]
```

## Traceability & Review Artifacts

- **Node & File Manifest:** `.laminar/manifest.json`
- **Principal Delivery Review:** `reports/review/final-review.md`
- **Requirement & Story Traceability:** `TRACEABILITY.md`
<!-- laminar:generated:end -->
