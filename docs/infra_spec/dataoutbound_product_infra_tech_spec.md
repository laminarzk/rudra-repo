# DataOutbound Product - Infrastructure Technical Specification

## Table of Contents

- **1.** Infrastructure Overview & Scope
- **2.** IaaC Tenets & Automation Principles
- **3.** Naming Conventions & Tagging Strategy
  - **3.1** Diagram Components & Physical Names
  - **3.2** Supporting Resources Naming Convention
  - **3.3** Mandatory Tags JSON
- **4.** Compute, Container & Serverless Infrastructure
  - **4.1** Infrastructure Specification: orchestrator
    - **4.1.1** Resource Configuration & Sizing
    - **4.1.2** IAM Role & Policies
    - **4.1.3** Network Configuration
    - **4.1.4** Security Controls & Encryption
    - **4.1.5** Observability & Alerting
    - **4.1.6** Disaster Recovery & Backup
    - **4.1.7** Infrastructure Assumptions
  - **4.2** Infrastructure Specification: egress-lambda
    - **4.2.1** Resource Configuration & Sizing
    - **4.2.2** IAM Role & Policies
    - **4.2.3** Network Configuration
    - **4.2.4** Security Controls & Encryption
    - **4.2.5** Observability & Alerting
    - **4.2.6** Disaster Recovery & Backup
    - **4.2.7** Infrastructure Assumptions
  - **4.3** Infrastructure Specification: child_export_dag
    - **4.3.1** Resource Configuration & Sizing
    - **4.3.2** IAM Role & Policies
    - **4.3.3** Network Configuration
    - **4.3.4** Security Controls & Encryption
    - **4.3.5** Observability & Alerting
    - **4.3.6** Disaster Recovery & Backup
    - **4.3.7** Infrastructure Assumptions
  - **4.4** Infrastructure Specification: do-admin-api
    - **4.4.1** Resource Configuration & Sizing
    - **4.4.2** IAM Role & Policies
    - **4.4.3** Network Configuration
    - **4.4.4** Security Controls & Encryption
    - **4.4.5** Observability & Alerting
    - **4.4.6** Disaster Recovery & Backup
    - **4.4.7** Infrastructure Assumptions
  - **4.5** Infrastructure Specification: emr_do
    - **4.5.1** Resource Configuration & Sizing
    - **4.5.2** IAM Role & Policies
    - **4.5.3** Network Configuration
    - **4.5.4** Security Controls & Encryption
    - **4.5.5** Observability & Alerting
    - **4.5.6** Disaster Recovery & Backup
    - **4.5.7** Infrastructure Assumptions
  - **4.6** Infrastructure Specification: egress dag
    - **4.6.1** Resource Configuration & Sizing
    - **4.6.2** IAM Role & Policies
    - **4.6.3** Network Configuration
    - **4.6.4** Security Controls & Encryption
    - **4.6.5** Observability & Alerting
    - **4.6.6** Disaster Recovery & Backup
    - **4.6.7** Infrastructure Assumptions
  - **4.7** Infrastructure Specification: transform_encrypt_dag
    - **4.7.1** Resource Configuration & Sizing
    - **4.7.2** IAM Role & Policies
    - **4.7.3** Network Configuration
    - **4.7.4** Security Controls & Encryption
    - **4.7.5** Observability & Alerting
    - **4.7.6** Disaster Recovery & Backup
    - **4.7.7** Infrastructure Assumptions
- **5.** Storage, Databases & Caching
  - **5.1** Infrastructure Specification: cofig_db
    - **5.1.1** Resource Configuration & Sizing
    - **5.1.2** IAM Role & Policies
    - **5.1.3** Network Configuration
    - **5.1.4** Security Controls & Encryption
    - **5.1.5** Observability & Alerting
    - **5.1.6** Disaster Recovery & Backup
    - **5.1.7** Infrastructure Assumptions
  - **5.2** Infrastructure Specification: dev
    - **5.2.1** Resource Configuration & Sizing
    - **5.2.2** IAM Role & Policies
    - **5.2.3** Network Configuration
    - **5.2.4** Security Controls & Encryption
    - **5.2.5** Observability & Alerting
    - **5.2.6** Disaster Recovery & Backup
    - **5.2.7** Infrastructure Assumptions
  - **5.3** Infrastructure Specification: staging_bucet_internal
    - **5.3.1** Resource Configuration & Sizing
    - **5.3.2** IAM Role & Policies
    - **5.3.3** Network Configuration
    - **5.3.4** Security Controls & Encryption
    - **5.3.5** Observability & Alerting
    - **5.3.6** Disaster Recovery & Backup
    - **5.3.7** Infrastructure Assumptions
  - **5.4** Infrastructure Specification: audit_bucket
    - **5.4.1** Resource Configuration & Sizing
    - **5.4.2** IAM Role & Policies
    - **5.4.3** Network Configuration
    - **5.4.4** Security Controls & Encryption
    - **5.4.5** Observability & Alerting
    - **5.4.6** Disaster Recovery & Backup
    - **5.4.7** Infrastructure Assumptions
- **6.** Networking, DNS & Content Delivery
  - **6.1** Infrastructure Specification: scheduler
    - **6.1.1** Resource Configuration & Sizing
    - **6.1.2** IAM Role & Policies
    - **6.1.3** Network Configuration
    - **6.1.4** Security Controls & Encryption
    - **6.1.5** Observability & Alerting
    - **6.1.6** Disaster Recovery & Backup
    - **6.1.7** Infrastructure Assumptions
- **7.** Security, Identity & Secrets Management
- **8.** Observability, Monitoring & Alerting
- **9.** Alternatives Considered
- **10.** Appendix: Assumptions & Follow-up Question Resolution
- **11.** Traceability
- **12.** Closing & Disclaimer

* **Version:** 1.0
* **Generation Date:** 2025-02-15
* **Generated By:** Principal Infrastructure and DevOps Architect

---

## 1. Infrastructure Overview & Scope

The DataOutbound Product is an enterprise-grade, scheduled batch outbound data synchronization engine operating within the hospitality data platform domain. It extracts Golden Entity datasets (identity profiles, subscription statuses, enriched campaigns, reactions, and reservation items) from the Redshift source store (`dev`), transforms them into JSON/TXT files, and securely delivers them to customer-owned destinations. 

The architecture is fully provisioned on AWS using infrastructure-as-code (Terraform) across multi-environment workspaces (`dev`, `staging`, `prod`) hosted in the `us-east-1` region. The deployed AWS footprint comprises:
* **Amazon MWAA (Managed Workflows for Apache Airflow)**: Orchestrates pipeline execution via four core DAGs (`orchestrator`, `child_export_dag`, `transform_encrypt_dag`, `egress dag`).
* **Amazon RDS PostgreSQL**: Acts as the Export Config Store (`cofig_db`) for active connection metadata, schedules, and watermark tracking.
* **Amazon Redshift**: The Golden Entity Source Store (`dev`), which is an existing externally owned source and is explicitly excluded from the pipeline-owned resource allowlist per SCRUM-485.
* **EMR Serverless**: Executes the Export Extract Spark Job (`emr_do`) for distributed data extraction and staging.
* **Amazon S3**: Powers Customer Staging Buckets (`staging_bucet_internal`) with automated 7-day lifecycle purging, and the Audit & Execution Log Store (`audit_bucket`).
* **AWS Lambda**: Executes the Egress Lambda (`egress-lambda`) triggered by S3 events to drive multi-protocol delivery.
* **Amazon EventBridge**: Emits scheduled cadence triggers via `scheduler`.
* **External Destinations**: Customer-owned targets including Azure Blob Storage (`Customer Azure Blob Destination`), Google Cloud Storage (`Customer GCS Destination`), secure SFTP endpoints (`customer SFTP server`), and customer S3 buckets (`customer bucket`), all of which are customer-owned and excluded from the pipeline allowlist per SCRUM-485.

---

## 2. IaaC Tenets & Automation Principles

1. **Immutable Infrastructure**: All compute runtimes (MWAA workers, EMR Serverless Spark executors, Lambda functions) are immutable; configuration is injected via environment variables and SSM Parameter Store secrets at runtime.
2. **Single Terraform Root Module**: The infrastructure is structured as a unified Terraform root (`terraform/`) with service-specific `.tf` files (`api.tf`, `blob_store.tf`, `emr.tf`, `eventbridge.tf`, `gcs.tf`, `lambda.tf`, `main.tf`, `mwaa.tf`, `rds.tf`, `redshift.tf`, `s3.tf`) driven by environment overlays in `values.yaml`.
3. **Environment-Driven Deletion Protection**: Stateful resources (RDS PostgreSQL, S3 buckets, KMS keys) enforce deletion protection toggles dynamically read from environment configuration variables (`dev`: disabled for rapid teardown; `staging`/`prod`: enabled).
4. **Least-Privilege Security by Default**: All IAM roles adhere strictly to least-privilege scoping, referencing specific resource ARNs with customer-managed KMS Customer Master Keys (CMKs) for encryption at rest.

---

## 3. Naming Conventions & Tagging Strategy

### 3.1 Diagram Components & Physical Names
| Component Description | Diagram Name (Physical Name) | Terraform Resource Name |
|---|---|---|
| Export Orchestrator DAG (MWAA) | `orchestrator` | `orchestrator_dag_mwaa` |
| Export Admin API | `do-admin-api` | `export_admin_api` |
| Export Scheduler (EventBridge) | `scheduler` | `export_scheduler_eventbridge` |
| Child Export DAG (MWAA) | `child_export_dag` | `child_export_dag_mwaa` |
| Transform & Encrypt DAG (MWAA) | `transform_encrypt_dag` | `transform_encrypt_dag_mwaa` |
| Egress DAG (MWAA) | `egress dag` | `egress_dag_mwaa` |
| Export Extract Spark Job (EMR) | `emr_do` | `export_extract_spark_job_emr` |
| Golden Entity Source Store (Redshift) | `dev` | `golden_entity_source_store_redshift` |
| Export Config Store (RDS) | `cofig_db` | `export_config_store_rds` |
| Customer Staging Buckets (S3) | `staging_bucet_internal` | `customer_staging_buckets_s3` |
| Audit & Execution Log Store (S3) | `audit_bucket` | `audit_execution_log_store_s3` |
| Egress Lambda | `egress-lambda` | `egress_lambda` |
| Customer Azure Blob Destination | `Customer Azure Blob Destination` | `customer_azure_blob_destination_blob_store` |
| Customer GCS Destination | `Customer GCS Destination` | `customer_gcs_destination_gcs` |
| Customer SFTP Server | `customer SFTP server` | `customer_sftp_server` |
| Customer S3 Bucket | `customer bucket` | `customer_bucket_s3` |

### 3.2 Supporting Resources Naming Convention
Supporting resources created by Terraform follow the laminar laminar naming standard: `laminar-<service>-<component>-<env>`.

| Resource Type | Pattern | Example (prod) |
|---|---|---|
| IAM Role | `laminar-{service}-{component}-role-{env}` | `laminar-mwaa-orchestrator-role-prod` |
| CloudWatch Log Group | `/aws/laminar/{service}/{component}/{env}` | `/aws/laminar/mwaa/orchestrator/prod` |
| KMS Key Alias | `alias/laminar-{component}-key-{env}` | `alias/laminar-orchestrator-key-prod` |
| Dead-Letter Queue | `laminar-{component}-dlq-{env}` | `laminar-egress-dlq-prod` |
| Security Group | `laminar-{component}-sg-{env}` | `laminar-mwaa-orchestrator-sg-prod` |

### 3.3 Mandatory Tags JSON
```json
{
  "Environment": "prod",
  "Product": "com.laminar:dataoutbound",
  "ManagedBy": "Terraform",
  "Owner": "data-platform-engineering@acme.com",
  "CostCenter": "dataoutbound-prod-01"
}
```

---

## 4. Compute, Container & Serverless Infrastructure

### 4.1 Infrastructure Specification: orchestrator
#### 4.1.1 Resource Configuration & Sizing
All four DAGs (`orchestrator`, `child_export_dag`, `transform_encrypt_dag`, `egress dag`) run in a **single MWAA environment**; the environment class and worker scaling below are the one authoritative per-environment configuration held in `values.yaml` (dev / staging / prod).

| Parameter | dev / staging | prod | Justification |
|---|---|---|---|
| Service | Amazon MWAA (Apache Airflow 2.8.1) | Amazon MWAA (Apache Airflow 2.8.1) | Managed orchestration control plane for all four DAGs. |
| DAG Name | `orchestrator` | `orchestrator` | Exact user decision from architecture graph. |
| Schedule | `@daily` | `@daily` | Default orchestration schedule cadence. |
| Catchup | `False` | `False` | Prevents unintended backfill execution storms upon DAG activation. |
| Environment Class | `mw1.small` | `mw1.medium` | Cost-effective dev footprint; production-grade capacity for daily/hourly coordination and child DAG fan-out. |
| Min Workers | `1` | `2` | Baseline capacity; two workers in prod for AZ fault tolerance. |
| Max Workers | `2` | `10` | Auto-scaling ceiling to handle concurrent fan-out of child export runs. |
| Schedulers | `1` | `2` | Active-active HA scheduler configuration in prod; single scheduler in lower environments. |
| Max Active Runs | `1` | `1` | Strictly enforces single-run idempotency per schedule window. |

#### 4.1.2 IAM Role & Policies
**Trust Policy (`laminar-mwaa-orchestrator-trust-prod.json`):**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "airflow.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

**Execution Policy (`laminar-mwaa-orchestrator-policy-prod.json`):**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject*",
        "s3:PutObject*",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::staging_bucet_internal",
        "arn:aws:s3:::staging_bucet_internal/*",
        "arn:aws:s3:::audit_bucket",
        "arn:aws:s3:::audit_bucket/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "rds-db:connect"
      ],
      "Resource": "arn:aws:rds-db:us-east-1:123456789012:dbuser:cofig_db/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "airflow:PublishMetrics"
      ],
      "Resource": "arn:aws:airflow:us-east-1:123456789012:environment/orchestrator"
    },
    {
      "Effect": "Allow",
      "Action": [
        "kms:Decrypt",
        "kms:GenerateDataKey"
      ],
      "Resource": "arn:aws:kms:us-east-1:123456789012:key/laminar-orchestrator-key-prod"
    }
  ]
}
```

#### 4.1.3 Network Configuration
* **VPC Placement:** Private Application Subnets (Multi-AZ across `us-east-1a`, `us-east-1b`, `us-east-1c`).
* **Subnet Tier:** Isolated Private Subnets with NAT Gateway egress.
* **Security Group Rules:**

| Direction | Protocol | Port Range | Destination / Source | Purpose |
|---|---|---|---|---|
| Inbound | TCP | 443 | Internal VPC CIDR (`10.0.0.0/16`) | Airflow Webserver / API Access |
| Outbound | TCP | 5432 | `export_config_store_rds` SG | RDS PostgreSQL Metadata Queries |
| Outbound | TCP | 443 | AWS Service Endpoints (S3, KMS, SSM) | SDK calls and artifact synchronization |

#### 4.1.4 Security Controls & Encryption
**KMS CMK Policy (`laminar-orchestrator-key-policy-prod.json`):**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "Enable IAM User Permissions",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::123456789012:root"
      },
      "Action": "kms:*",
      "Resource": "*"
    },
    {
      "Sid": "Allow MWAA Execution Role",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::123456789012:role/laminar-mwaa-orchestrator-role-prod"
      },
      "Action": [
        "kms:Decrypt",
        "kms:GenerateDataKey",
        "kms:DescribeKey"
      ],
      "Resource": "*"
    }
  ]
}
```

#### 4.1.5 Observability & Alerting
| Metric | Namespace | Threshold | Alarm Action |
|---|---|---|---|
| `DAGProcessingTotalDuration` | `AWS/MWAA` | > 300 seconds | SNS Alert Topic (`laminar-alerts-prod`) |
| `WorkerCPUUtilization` | `AWS/MWAA` | > 85% for 15 mins | SNS Alert Topic (`laminar-alerts-prod`) |
| `QueuedTasks` | `AWS/MWAA` | > 50 for 10 mins | SNS Alert Topic (`laminar-alerts-prod`) |

#### 4.1.6 Disaster Recovery & Backup
* **RTO Target:** < 2 hours
* **RPO Target:** 24 hours (DAG code stored in the dedicated MWAA DAGs source bucket with versioning; Airflow metadata DB managed by MWAA with automated backups).
* **Backup Mechanism:** Automated S3 bucket versioning on the DAGs source bucket and MWAA-managed metadata database backups.
* **Note:** The Export Config Store (`cofig_db`) is deployed with `multi_az = False` per the user's diagram decision; the orchestrator's RPO therefore depends on the RDS automated-backup schedule (24-hour RPO) rather than synchronous Multi-AZ replication.

#### 4.1.7 Infrastructure Assumptions
* **Q:** What is the exact Airflow version and sizing tier required for production workloads? / **A:** Airflow version 2.8.1 with `mw1.medium` environment class and auto-scaling between 1 and 10 workers. / **Justification:** Aligns with standard enterprise MWAA footprints capable of processing up to 100,000 records under the 24-hour SLA.
* **Q:** How are MWAA environment variables and secrets secured? / **A:** Injected via AWS Secrets Manager and SSM Parameter Store SecureString references encrypted with the dedicated KMS CMK. / **Justification:** Adheres to enterprise security standards prohibiting plaintext credentials in DAG definitions.
* **Q:** What is the deletion protection policy for lower vs upper environments? / **A:** Driven by environment configuration flags (`dev`: false, `prod`: true). / **Justification:** Satisfies requirement SCRUM-485 for clean teardowns in lower environments while protecting production stateful resources.

---

### 4.2 Infrastructure Specification: egress-lambda
#### 4.2.1 Resource Configuration & Sizing
| Parameter | Value | Justification |
|---|---|---|
| Function Name | `egress-lambda` | Exact user decision from architecture graph. |
| Runtime | `python3.11` | Standard secure Python runtime matching blueprint language and security standards. |
| Handler | `lambda_function.lambda_handler` | Entrypoint convention defined in project blueprint. |
| Memory | `512 MB` | Balanced allocation for handling JSON payload parsing, S3 streaming, and PGP invocation without excessive cost. |
| Timeout | `30 seconds` | Sufficient duration to process staged export file notifications and trigger downstream encryption/transfer DAGs. |
| VPC Enabled | `False` | Executes in AWS Lambda managed service plane; communicates with S3 and MWAA via regional HTTPS APIs. |
| Environment Variables | `{}` | Runtime configuration loaded dynamically via `ConfigLoader` and SSM Parameter Store. |
| Tags | `{}` | Environment-driven tagging applied via Terraform root module. |

#### 4.2.2 IAM Role & Policies
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowS3StagingRead",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::staging_bucet_internal",
        "arn:aws:s3:::staging_bucet_internal/*"
      ]
    },
    {
      "Sid": "AllowMwaaDagTrigger",
      "Effect": "Allow",
      "Action": [
        "airflow:CreateDagRun"
      ],
      "Resource": "arn:aws:airflow:us-east-1:123456789012:environment/egress_dag"
    },
    {
      "Sid": "AllowCloudWatchLogs",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:us-east-1:123456789012:log-group:/aws/lambda/egress-lambda:*"
    },
    {
      "Sid": "AllowSSMParameters",
      "Effect": "Allow",
      "Action": [
        "ssm:GetParameter",
        "ssm:GetParameters"
      ],
      "Resource": "arn:aws:ssm:us-east-1:123456789012:parameter/dataoutbound/*"
    }
  ]
}
```
*Trust Policy (IAM Role `laminar-lambda-egress-role-prod`):*
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

#### 4.2.3 Network Configuration
* **VPC Placement**: AWS Lambda managed execution environment (non-VPC mode as configured via `vpc_enabled = False`).
* **Subnet Tier**: N/A (Lambda managed control plane).
* **Security Group Rules**: Outbound HTTPS (port 443) to AWS S3 and MWAA REST APIs. Inbound traffic is triggered directly via S3 event notifications.

#### 4.2.4 Security Controls & Encryption
* **Data at Rest**: Environment variables and temporary execution storage encrypted using AWS managed keys (`aws/lambda`).
* **Data in Transit**: Enforced TLS 1.3 for all calls to S3, MWAA, and SSM Parameter Store.
* **IAM Least Privilege**: Restricted strictly to S3 read actions on `staging_bucet_internal`, MWAA DAG trigger permissions, and CloudWatch logging.

#### 4.2.5 Observability & Alerting
| Metric | Namespace | Threshold | Alarm Action |
|---|---|---|---|
| `Errors` | `AWS/Lambda` | > 0 over 1 minute period | SNS Topic `laminar-alerts-topic` |
| `Throttles` | `AWS/Lambda` | > 0 over 5 minute period | SNS Topic `laminar-alerts-topic` |
| `Duration` | `AWS/Lambda` | > 25,000ms (83% of timeout) | SNS Topic `laminar-alerts-topic` |

#### 4.2.6 Disaster Recovery & Backup
* **RTO**: < 1 minute (Stateless compute resource recreated automatically on failure).
* **RPO**: 0 (No local persistent state; event-driven invocation replay via S3 notifications or SQS DLQ).
* **Backup Mechanism**: Function code versioning stored in S3 deployment bucket with automated infrastructure provisioning via Terraform.

#### 4.2.7 Infrastructure Assumptions
* Q: What is the exact trigger mechanism for the Egress Lambda? / A: S3 ObjectCreated event notification originating from `staging_bucet_internal`. / Justification: Aligns with architecture specifications and SCRUM-482 technical context.
* Q: How are unhandled runtime exceptions handled? / A: Unhandled exceptions route to an SQS Dead-Letter Queue (DLQ) after 3 retry attempts. / Justification: Adheres to serverless resilience best practices and enterprise logging standards.

---

### 4.3 Infrastructure Specification: child_export_dag
#### 4.3.1 Resource Configuration & Sizing
| Parameter | Value | Justification |
|---|---|---|
| DAG Name | `child_export_dag` | Exact user decision from architecture graph. |
| Schedule | `@daily` | Default schedule inherited from orchestrator and configured per connection metadata. |
| Catchup | `False` | Prevents unintended backfill execution storms upon DAG activation. |
| Max Active Runs | `1` | Ensures sequential execution integrity and prevents resource contention on EMR Serverless. |
| Tags | `{}` | Environment-driven tagging applied via Terraform MWAA module. |

#### 4.3.2 IAM Role & Policies
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowEmrServerlessJobSubmission",
      "Effect": "Allow",
      "Action": [
        "emr-serverless:StartJobRun",
        "emr-serverless:GetJobRun",
        "emr-serverless:CancelJobRun"
      ],
      "Resource": "arn:aws:emr-serverless:us-east-1:123456789012:/applications/emr_do"
    },
    {
      "Sid": "AllowRdsConfigRead",
      "Effect": "Allow",
      "Action": [
        "rds-db:connect"
      ],
      "Resource": "arn:aws:rds-db:us-east-1:123456789012:dbuser:cofig_db/*"
    },
    {
      "Sid": "AllowS3StagingAccess",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::staging_bucet_internal",
        "arn:aws:s3:::staging_bucet_internal/*"
      ]
    }
  ]
}
```
*Trust Policy (IAM Role `laminar-mwaa-child-exec-role`):*
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "airflow.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

#### 4.3.3 Network Configuration
* **VPC Placement**: Private isolated MWAA worker subnets within the enterprise VPC.
* **Subnet Tier**: Private Application Subnets (`10.0.3.0/24`, `10.0.4.0/24`).
* **Security Group Rules**:
  * Inbound: Internal VPC traffic on ephemeral ports.
  * Outbound: TCP 5432 (Postgres) to `cofig_db`, HTTPS (443) to EMR Serverless and S3 endpoints via VPC endpoints.

#### 4.3.4 Security Controls & Encryption
* **Data at Rest**: MWAA metadata encrypted using AWS KMS Customer Managed Key (`arn:aws:kms:us-east-1:123456789012:key/mwaa-cmk`).
* **Data in Transit**: TLS 1.3 enforced for communication with RDS Postgres and EMR Serverless APIs.
* **Access Control**: DAG execution restricted to authorized orchestrator triggers and Airflow RBAC policies.

#### 4.3.5 Observability & Alerting
| Metric | Namespace | Threshold | Alarm Action |
|---|---|---|---|
| `DagProcessingDuration` | `AmazonMWAA` | > 300 seconds | SNS Topic `laminar-alerts-topic` |
| `TaskFailed` | `AmazonMWAA` | >= 1 failure over 1 period | SNS Topic `laminar-alerts-topic` |
| `WorkerCPUUtilization` | `AmazonMWAA` | > 85% for 15 minutes | SNS Topic `laminar-alerts-topic` |

#### 4.3.6 Disaster Recovery & Backup
* **RTO**: < 30 minutes (MWAA environment recovery via automated Terraform deployment and DAG source re-sync).
* **RPO**: 24 hours (Airflow metadata DB managed by MWAA with automated backups; DAG code is version-controlled in Git and synced to the DAGs bucket).
* **Backup Mechanism:** DAG code, requirements and plugins are stored in the **dedicated MWAA DAGs source bucket** (`laminar-mwaa-dags-{env}`, a supporting resource of the shared MWAA environment — not `staging_bucet_internal`, which is purged after exactly 7 days per SCRUM-486 AC#5 and must never hold DAG code). Bucket versioning enabled on the DAGs source bucket.

#### 4.3.7 Infrastructure Assumptions
* Q: How does the Child Export DAG receive connection metadata? / A: Passed dynamically in the DAG run configuration (`dag_run.conf`) generated by the Export Orchestrator DAG. / Justification: Satisfies SCRUM-480 AC#4.
* Q: Where are execution logs aggregated? / A: CloudWatch Log Groups explicitly provisioned under the Audit & Execution Log Store (`audit_bucket` / parent log group hierarchy). / Justification: Complies with audit and logging standards (SCRUM-486).

---

### 4.4 Infrastructure Specification: do-admin-api
#### 4.4.1 Resource Configuration & Sizing
| Parameter | Value | Justification |
|---|---|---|
| API Name | `do-admin-api` | Exact user decision from architecture graph. |
| Protocol | `REST` | Standard HTTP/REST interface for administrative CRUD operations. |
| Authentication | `JWT` | Bearer token validation enforced across all protected administrative endpoints. |
| Compute Runtime | Python 3.11 (AWS Lambda / FastAPI) | Serverless API backend ensuring rapid scaling, zero idle cost, and seamless integration with RDS. |
| Memory / Timeout | `1024 MB` / `15 seconds` | Adequate performance profile for handling JSON request validation and database queries. |

#### 4.4.2 IAM Role & Policies
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowRdsConfigWrite",
      "Effect": "Allow",
      "Action": [
        "rds-db:connect"
      ],
      "Resource": "arn:aws:rds-db:us-east-1:123456789012:dbuser:cofig_db/*"
    },
    {
      "Sid": "AllowDbCredentialRetrieval",
      "Effect": "Allow",
      "Action": [
        "ssm:GetParameter",
        "ssm:GetParameters"
      ],
      "Resource": "arn:aws:ssm:us-east-1:123456789012:parameter/dataoutbound/db/cofig_db/*"
    },
    {
      "Sid": "AllowMwaaOrchestratorTrigger",
      "Effect": "Allow",
      "Action": [
        "airflow:CreateDagRun"
      ],
      "Resource": "arn:aws:airflow:us-east-1:123456789012:environment/orchestrator"
    },
    {
      "Sid": "AllowAuditLogging",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject"
      ],
      "Resource": "arn:aws:s3:::audit_bucket/*"
    }
  ]
}
```
*Trust Policy (IAM Role `laminar-api-role`):*
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

#### 4.4.3 Network Configuration
* **Hosting:** The `do-admin-api` runs as a Lambda function exposed via a **Lambda Function URL** (HTTPS endpoint). No API Gateway is deployed — Amazon API Gateway is outside the SCRUM-485 approved allowlist (MWAA, RDS Postgres, Lambda, EMR Serverless, S3, ECS).
* **VPC Placement:** Lambda functions deployed in private application subnets (`10.0.3.0/24`, `10.0.4.0/24`) to reach `cofig_db`.
* **Security Group Rules:**

| Direction | Protocol | Port Range | Destination / Source | Purpose |
|---|---|---|---|---|
| Inbound | TCP | 443 | Lambda Function URL (managed) | HTTPS API requests from authorized clients |
| Outbound | TCP | 5432 | `cofig_db` SG | RDS PostgreSQL configuration CRUD |
| Outbound | TCP | 443 | MWAA REST API endpoint | Orchestrator DAG trigger / stop |
| Outbound | TCP | 443 | S3 VPC Gateway Endpoint | Audit event writes to `audit_bucket` |

#### 4.4.4 Security Controls & Encryption
* **Authentication & Authorization**: JWT token validation with required `export-admin` role verification.
* **Data at Rest**: Credential fields encrypted using AWS KMS CMK before persisting to RDS (`cofig_db`).
* **Data in Transit**: TLS 1.3 enforced on all incoming API requests and outgoing database connections.

#### 4.4.5 Observability & Alerting
| Metric | Namespace | Threshold | Alarm Action |
|---|---|---|---|
| `Errors` | `AWS/Lambda` (function `do-admin-api`) | >= 5 errors over 5 minutes | SNS Topic `laminar-alerts-topic` |
| `Duration` | `AWS/Lambda` (function `do-admin-api`) | > 12,000ms p99 over 5 minutes | SNS Topic `laminar-alerts-topic` |
| `Throttles` | `AWS/Lambda` (function `do-admin-api`) | > 0 over 5 minutes | SNS Topic `laminar-alerts-topic` |

#### 4.4.6 Disaster Recovery & Backup
* **RTO**: < 15 minutes (Stateless API frontend with multi-AZ serverless architecture).
* **RPO**: 0 (Persistence handled entirely by RDS PostgreSQL backend).
* **Backup Mechanism**: API Gateway stage versioning and Terraform-managed infrastructure definitions.

#### 4.4.7 Infrastructure Assumptions
* Q: How are API requests authenticated? / A: JWT bearer tokens validated against the enterprise Identity Provider (IdP) with role checks implemented in `auth.py`. / Justification: Meets standard API security principles and SCRUM-479 AC#7.
* Q: Where are admin actions audited? / A: Written directly to the Audit & Execution Log Store (`audit_bucket`) and database audit tables within <=5s. / Justification: Satisfies SCRUM-479 AC#4/AC#5 and SCRUM-486.

---

### 4.5 Infrastructure Specification: emr_do

#### 4.5.1 Resource Configuration & Sizing
| Parameter | Value (Dev / Staging) | Value (Prod) | Justification |
|---|---|---|---|
| **Terraform Resource** | `aws_emrserverless_application.export_extract_spark_job_emr` | `aws_emrserverless_application.export_extract_spark_job_emr` | Terraform resource name per project blueprint (`terraform/emr.tf`). |
| **Application Name** | `emr_do` | `emr_do` | Exact physical resource name defined in architecture graph. |
| **Release Label** | `emr-7.0.0` | `emr-7.0.0` | Enterprise standard Spark 3.5 runtime on EMR Serverless 7.x. |
| **Job Type** | `SPARK` | `SPARK` | Optimized for distributed data extraction and transformation workloads. |
| **Maximum Capacity Units** | vCPU: 100, Memory: 400 GB | vCPU: 200, Memory: 800 GB | Bounded capacity to prevent runaway resource consumption while handling up to 100,000 records under 24-hour SLA. |
| **Initial Capacity Workers** | 2 Workers (4 vCPU, 16 GB RAM each) | 4 Workers (8 vCPU, 32 GB RAM each) | Pre-warmed workers to eliminate cold-start latency for scheduled fast-batch extractions. |
| **Auto-Stop Idle Timeout** | 15 minutes | 15 minutes | Automatically scales down idle worker fleets to optimize infrastructure run costs. |

#### 4.5.2 IAM Role & Policies
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "EMRServerlessAssumeRole",
      "Effect": "Allow",
      "Principal": {
        "Service": "emr-serverless.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3StagingAndLogsAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::staging_bucet_internal",
        "arn:aws:s3:::staging_bucet_internal/*",
        "arn:aws:s3:::audit_bucket",
        "arn:aws:s3:::audit_bucket/*"
      ]
    },
    {
      "Sid": "KMSKeyDecryptionEncryption",
      "Effect": "Allow",
      "Action": [
        "kms:Decrypt",
        "kms:Encrypt",
        "kms:GenerateDataKey*",
        "kms:DescribeKey"
      ],
      "Resource": "arn:aws:kms:us-east-1:123456789012:key/laminar-data-cmk-id"
    },
    {
      "Sid": "RedshiftDataConnectionAccess",
      "Effect": "Allow",
      "Action": [
        "redshift-data:ExecuteStatement",
        "redshift-data:DescribeStatement",
        "redshift-data:GetStatementResult",
        "redshift:GetClusterCredentials"
      ],
      "Resource": "*"
    },
    {
      "Sid": "CloudWatchLoggingAccess",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "logs:DescribeLogStreams"
      ],
      "Resource": "arn:aws:logs:us-east-1:123456789012:log-group:/aws/emr-serverless/applications/emr_do:*"
    }
  ]
}
```

#### 4.5.3 Network Configuration
* **VPC Placement:** Private Subnets (`subnet-priv-1`, `subnet-priv-2`) isolated from direct public internet routing.
* **Subnet Tier:** Isolated data processing tier with outbound egress routed exclusively via NAT Gateway for external JDBC/Redshift connectivity.
* **Security Group Rules:**

| Direction | Protocol | Port Range | Destination / Source | Purpose |
|---|---|---|---|---|
| **Egress** | TCP | 443 | `0.0.0.0/0` | Secure TLS connections to AWS S3, Redshift Data API, and KMS endpoints. |
| **Egress** | TCP | 5439 | `rds-redshift-subnet-group` | Direct JDBC communication with Redshift Golden Entity source store. |
| **Ingress** | TCP | All | Self (Security Group) | Internal worker-to-worker shuffle communication during Spark distributed map-reduce stages. |

#### 4.5.4 Security Controls & Encryption
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "EnableEMRServerlessKMSEncryption",
      "Effect": "Allow",
      "Principal": {
        "Service": "emr-serverless.amazonaws.com"
      },
      "Action": [
        "kms:Decrypt",
        "kms:GenerateDataKey"
      ],
      "Resource": "*",
      "Condition": {
        "StringEquals": {
          "kms:CallerAccount": "123456789012"
        }
      }
    }
  ]
}
```
* **Data at Rest:** All intermediate shuffle data, logs, and staged files on EMR Serverless managed storage are encrypted using customer-managed KMS keys (`alias/laminar-data-key`).
* **Data in Transit:** TLS 1.3 enforced for all S3 interactions and Redshift JDBC connections. Interactive shell/exec SSH access is permanently disabled.

#### 4.5.5 Observability & Alerting
| Metric | Namespace | Threshold | Alarm Action |
|---|---|---|---|
| `FailedJobs` | `AWS/EMRServerless` | `>= 1` failure in 5 minutes | Triggers SNS alert topic and logs execution failure event to audit store. |
| `JobRunDuration` | `AWS/EMRServerless` | `> 7200 seconds` (2 hours) | Warning alert for SLA breach risk on large batch extractions. |
| `WorkersAllocated` | `AWS/EMRServerless` | `>= 180` workers (90% capacity) | Capacity saturation warning to anticipate scaling bottlenecks. |

#### 4.5.6 Disaster Recovery & Backup
* **RTO (Recovery Time Objective):** 1 Hour. Re-execution of the failed Spark job is automatically triggered via upstream Child Export DAG retry policies or manual dispatch.
* **RPO (Recovery Point Objective):** 0. Stateless execution environment; state is checkpointed incrementally in RDS watermark tables and persistent data resides safely in S3 and Redshift.
* **Backup Mechanism:** Application code and Spark job recipes are version-controlled in Git; infrastructure configurations are provisioned immutably via Terraform.

#### 4.5.7 Infrastructure Assumptions
* **Q:** What is the partitioning strategy for Spark input and output datasets written to S3?  
  **A:** `year=YYYY/month=MM/day=DD` partition layout.  
  **Justification:** Aligns with AWS EMR Serverless best practices to optimize distributed Spark I/O, prevent S3 request throttling, and minimize scan overhead during egress.
* **Q:** How are EMR Serverless worker execution logs persistently captured for compliance and debugging?  
  **A:** Published to dedicated CloudWatch Log Groups (`/aws/emr-serverless/applications/emr_do`) and backed up to S3 log archival buckets.  
  **Justification:** Satisfies enterprise auditability standards and ensures log retention aligns with the organisation logging standard.
* **Q:** What is the deletion protection mechanism for EMR Serverless application resources across environments?  
  **A:** Deletion protection flags are driven dynamically by environment configuration variables (`values.yaml`), ensuring dev environments can be torn down freely while staging and prod enforce hard deletion guards.  
  **Justification:** Conforms with SCRUM-485 architectural requirements for multi-environment lifecycle management.
### 4.6 Infrastructure Specification: egress dag
#### 4.6.1 Resource Configuration & Sizing
The `egress dag` runs in the **single shared MWAA environment** whose sizing is defined once in §4.1.1 (`mw1.small` dev / `mw1.medium` prod, 1–2 min / 2–10 max workers, 1–2 schedulers) and driven by `values.yaml`. DAG-level settings:

| Parameter | Value | Justification |
|---|---|---|
| DAG Name | `egress dag` | Exact user decision from architecture graph. |
| Schedule | `@daily` | Executes daily per business requirements for batch data distribution. |
| Max Active Runs | `1` | Prevents overlapping concurrent runs to maintain strict idempotency and avoid race conditions. |
| Catchup | `False` | Disables automatic backfilling of missed historical runs to prevent accidental duplicate egress data transfers. |
| Worker Task Concurrency | `2` tasks per worker | Prevents memory exhaustion during GZIP and PGP processing phases (peak RSS stays below 2,048 MB). |

#### 4.6.2 IAM Role & Policies
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "MWAAExecutionTrust",
      "Effect": "Allow",
      "Principal": {
        "Service": [
          "airflow.amazonaws.com",
          "airflow-env.amazonaws.com"
        ]
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3StagingAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::staging_bucet_internal",
        "arn:aws:s3:::staging_bucet_internal/*"
      ]
    },
    {
      "Sid": "S3CustomerBucketPush",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject"
      ],
      "Resource": [
        "arn:aws:s3:::customer bucket",
        "arn:aws:s3:::customer bucket/*"
      ]
    },
    {
      "Sid": "AuditBucketPush",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject"
      ],
      "Resource": "arn:aws:s3:::audit_bucket/*"
    },
    {
      "Sid": "KMSEncryptionAccess",
      "Effect": "Allow",
      "Action": [
        "kms:Decrypt",
        "kms:Encrypt",
        "kms:GenerateDataKey*",
        "kms:DescribeKey"
      ],
      "Resource": [
        "arn:aws:kms:us-east-1:123456789012:key/dataoutbound-kms-key"
      ]
    },
    {
      "Sid": "SSMParameterAccess",
      "Effect": "Allow",
      "Action": [
        "ssm:GetParameter",
        "ssm:GetParameters",
        "ssm:GetParameterHistory"
      ],
      "Resource": [
        "arn:aws:ssm:us-east-1:123456789012:parameter/dataoutbound/*"
      ]
    },
    {
      "Sid": "CloudWatchLogging",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "logs:GetLogEvents",
        "logs:DescribeLogStreams"
      ],
      "Resource": [
        "arn:aws:logs:us-east-1:123456789012:log-group:/aws/laminar/mwaa/egress-dag/*"
      ]
    }
  ]
}
```
*Note:* The `push` connection to the customer bucket is covered by `S3CustomerBucketPush`. Azure Blob, GCS, and SFTP deliveries use credentials retrieved at runtime from SSM Parameter Store (covered by `SSMParameterAccess`); no cloud-to-cloud IAM applies to those customer-owned destinations.

#### 4.6.3 Network Configuration
* **VPC Placement:** Private Subnets across at least two Availability Zones (Isolated subnets with no direct Internet Gateway routing; outbound connectivity via NAT Gateway).
* **Subnet Tier:** Private Data Subnets.
* **Security Group Rules:**

| Direction | Protocol | Port Range | Destination / Source | Purpose |
|---|---|---|---|---|
| Outbound | TCP | 443 | 0.0.0.0/0 (AWS VPC Endpoint / NAT) | HTTPS API calls to AWS services, S3, Azure Blob, and GCS endpoints over TLS 1.3. |
| Outbound | TCP | 22 | Customer SFTP Server IP / CIDR | Secure file transfer (SSH) to external customer SFTP destination. |
| Inbound | TCP | 443 | Self (Security Group) | Internal worker-to-scheduler communication within MWAA cluster. |

#### 4.6.4 Security Controls & Encryption
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "EnableRootAccountPermissions",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::123456789012:root"
      },
      "Action": "kms:*",
      "Resource": "*"
    },
    {
      "Sid": "AllowMWAAAndS3Use",
      "Effect": "Allow",
      "Principal": {
        "Service": [
          "airflow.amazonaws.com",
          "s3.amazonaws.com"
        ]
      },
      "Action": [
        "kms:Encrypt",
        "kms:Decrypt",
        "kms:ReEncrypt*",
        "kms:GenerateDataKey*",
        "kms:DescribeKey"
      ],
      "Resource": "*"
    }
  ]
}
```
* **Data in Transit:** TLS 1.3 enforced for S3 transfers, HTTPS for Azure and GCS, and SSH with strict host-key verification for SFTP. PGP encryption applied prior to transport.
* **Data at Rest:** MWAA environment state and DAG storage encrypted using AWS KMS Customer Managed Key (CMK).

#### 4.6.5 Observability & Alerting
| Metric | Namespace | Threshold | Alarm Action |
|---|---|---|---|
| `DagRunFailed` | `AWS/MWAA` | Sum ≥ 1 over 5 minutes | SNS Alert Topic (PagerDuty / Opsgenie) |
| `TaskFail` | `AWS/MWAA` | Sum ≥ 2 over 5 minutes | SNS Alert Topic |
| `WorkerCPUUtilization` | `AWS/MWAA` | Greater than 85% for 15 minutes | SNS Alert Topic & Auto-scaling trigger |
| `AirflowWorkerQueueLength` | `AWS/MWAA` | Greater than 20 for 10 minutes | SNS Alert Topic |

#### 4.6.6 Disaster Recovery & Backup
* **RTO (Recovery Time Objective):** 4 hours (recovery via Terraform redeployment and metadata restoration).
* **RPO (Recovery Point Objective):** 24 hours (backed by automated daily snapshots of the underlying Amazon Aurora PostgreSQL metadata database managed by MWAA).
* **Backup Mechanism:** Automated MWAA environment snapshots stored in S3 and RDS automated backups.

#### 4.6.7 Infrastructure Assumptions
* **Q:** What is the expected concurrent task execution limit per worker? / **A:** 2 tasks per worker node. / **Justification:** Keeps peak RSS memory well below the 2,048 MB worker limit mandated by SCRUM-483 AC#2.
* **Q:** How are Airflow requirements and plugins packaged and deployed? / **A:** `requirements.txt` and plugin ZIP archives are stored in the dedicated MWAA DAGs source bucket (`laminar-mwaa-dags-{env}`) and synced by MWAA upon environment update. / **Justification:** Standardized MWAA deployment pattern avoiding direct manual worker file modifications.
* **Q:** What is the log retention period for MWAA CloudWatch log groups? / **A:** 90 days — **pending confirmation** of the single organisation logging-standard value before sprint commitment (SCRUM-486). / **Justification:** One consistent retention value is applied to every pipeline log group and the audit store; the exact number must be confirmed by the organisation before implementation.

### 4.7 Infrastructure Specification: transform_encrypt_dag
#### 4.7.1 Resource Configuration & Sizing
The `transform_encrypt_dag` runs in the **single shared MWAA environment** whose sizing is defined once in §4.1.1 and driven by `values.yaml`. DAG-level settings:

| Parameter | Value | Justification |
|---|---|---|
| DAG Name | `transform_encrypt_dag` | Exact user decision from blueprint and requirements. |
| Schedule | `@daily` | Triggered by the egress event workflow. |
| Catchup | `False` | Prevents unneeded historical backfills. |
| Max Active Runs | `1` | Serialises encryption runs per lineage to guarantee fail-closed semantics. |

#### 4.7.2 IAM Role & Policies
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowS3StagingAndAudit",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::staging_bucet_internal",
        "arn:aws:s3:::staging_bucet_internal/*",
        "arn:aws:s3:::audit_bucket",
        "arn:aws:s3:::audit_bucket/*"
      ]
    },
    {
      "Sid": "AllowRdsConfigRead",
      "Effect": "Allow",
      "Action": [
        "rds-db:connect"
      ],
      "Resource": "arn:aws:rds-db:us-east-1:123456789012:dbuser:cofig_db/*"
    },
    {
      "Sid": "AllowDbCredentialRetrieval",
      "Effect": "Allow",
      "Action": [
        "ssm:GetParameter",
        "ssm:GetParameters"
      ],
      "Resource": "arn:aws:ssm:us-east-1:123456789012:parameter/dataoutbound/db/cofig_db/*"
    },
    {
      "Sid": "AllowKMSKeyAccess",
      "Effect": "Allow",
      "Action": [
        "kms:Decrypt",
        "kms:Encrypt",
        "kms:GenerateDataKey"
      ],
      "Resource": "arn:aws:kms:us-east-1:123456789012:key/dataoutbound-kms-key"
    }
  ]
}
```
*Trust Policy:*
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "airflow.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```
*Note:* The DAG reads GZIP/PGP settings and connection JSON from the Export Config Store (`cofig_db`) per SCRUM-483's technical context; DB credentials are retrieved from SSM Parameter Store (`AllowDbCredentialRetrieval`).

#### 4.7.3 Network Configuration
* **VPC Placement:** Private Application Subnets.
* **Security Group Rules:** Outbound HTTPS (443) and internal worker communications.

#### 4.7.4 Security Controls & Encryption
* **Data at Rest & Transit:** Enforced AES-256 / TLS 1.3 with fail-closed PGP encryption.

#### 4.7.5 Observability & Alerting
| Metric | Namespace | Threshold | Alarm Action |
|---|---|---|---|
| `TaskFailed` | `AWS/MWAA` | `>= 1` failure | SNS Alert Topic |

#### 4.7.6 Disaster Recovery & Backup
* **RTO / RPO:** < 2 hours / 0.
* **Backup:** S3 versioning on DAG code bucket.

#### 4.7.7 Infrastructure Assumptions
* Q: What happens on PGP encryption failure? / A: Transfer is aborted immediately, zero unencrypted bytes are passed downstream, and a `TRANSFER_FAILED` event is recorded in the audit store within <=5s. / Justification: Adheres to SCRUM-483 AC#4 and SCRUM-486.

## 5. Storage, Databases & Caching

### 5.1 Infrastructure Specification: cofig_db
#### 5.1.1 Resource Configuration & Sizing
| Parameter | Value | Justification |
|---|---|---|
| Service | Amazon RDS PostgreSQL | Relational database engine for Export Config Store. |
| DB Identifier | `cofig_db` | Exact user decision from architecture graph. |
| Engine / Version | `postgres` / `15.4` | Enterprise standard PostgreSQL engine version. |
| Instance Class | `db.t3.micro` | Lightweight instance class suitable for storing connection metadata and watermark states. |
| Allocated Storage | `20 GB` | Sufficient baseline storage for configuration JSONs and cron metadata with `gp3` storage auto-scaling enabled. |
| Multi-AZ | `False` | Configured per user specifications for dev/test footprint. |

#### 5.1.2 IAM Role & Policies
* RDS Enhanced Monitoring IAM Role (`laminar-rds-monitoring-role`) granting `cloudwatch:PutMetricData`.

#### 5.1.3 Network Configuration
* **VPC Placement:** Isolated Private Database Subnets (`10.0.5.0/24`, `10.0.6.0/24`).
* **Security Group Rules:** Inbound TCP 5432 permitted exclusively from MWAA and API Lambda security groups.

#### 5.1.4 Security Controls & Encryption
* **Encryption at Rest:** Enabled via AWS KMS CMK (`alias/laminar-rds-key`).
* **Transport Encryption:** SSL/TLS enforced (`rds.force_ssl=1`). Credentials stored encrypted.

#### 5.1.5 Observability & Alerting
| Metric | Namespace | Threshold | Alarm Action |
|---|---|---|---|
| `CPUUtilization` | `AWS/RDS` | > 85% for 15 mins | SNS Topic `laminar-alerts-topic` |
| `FreeStorageSpace` | `AWS/RDS` | < 5 GB | SNS Topic `laminar-alerts-topic` |

#### 5.1.6 Disaster Recovery & Backup
* **RTO / RPO:** < 1 hour / 24 hours. Automated backups retained for 30 days.

#### 5.1.7 Infrastructure Assumptions
* Q: How are database credentials stored? / A: Generated via Terraform and stored securely in AWS Secrets Manager, accessed via `ConfigLoader`. / Justification: Adheres to secure-by-default standards.

---

### 5.2 Infrastructure Specification: dev
#### 5.2.1 Resource Configuration & Sizing
| Parameter | Value | Justification |
|---|---|---|
| Service | Amazon Redshift | Enterprise analytical data warehouse acting as Golden Entity Source Store. |
| Cluster Identifier | `golden_data_source` | Exact user decision from architecture graph. |
| Database Name | `dev` | Exact user decision from architecture graph. |
| Node Type | `dc2.large` | Standard dense compute node for fast parallel data scans. |
| Number of Nodes | `2` | Multi-node cluster configuration for distributed query processing. |
| Pipeline Ownership | External / Excluded | Existing externally owned source excluded from pipeline Terraform allowlist per SCRUM-485. |

#### 5.2.2 IAM Role & Policies
* External read-only IAM role assumed by EMR Serverless via Redshift Data API.

#### 5.2.3 Network Configuration
* **VPC Placement:** Private Database Subnets with dedicated Redshift cluster subnet group.

#### 5.2.4 Security Controls & Encryption
* **Encryption:** AES-256 storage encryption enabled via AWS KMS. TLS enforced for all client connections.

#### 5.2.5 Observability & Alerting
| Metric | Namespace | Threshold | Alarm Action |
|---|---|---|---|
| `CPUUtilization` | `AWS/Redshift` | > 90% | SNS Alert Topic |

#### 5.2.6 Disaster Recovery & Backup
* Automated cluster snapshots with 7-day retention.

#### 5.2.7 Infrastructure Assumptions
* Q: Is Redshift managed by this Terraform root module? / A: No, listed as an external data source excluded from allowlist checks per SCRUM-485. / Justification: Conforms strictly to user story scope definitions.

---

### 5.3 Infrastructure Specification: staging_bucet_internal
#### 5.3.1 Resource Configuration & Sizing
| Parameter | Value | Justification |
|---|---|---|
| Service | Amazon S3 | Object storage for customer staging files. |
| Bucket Name | `staging_bucet_internal` | Exact user decision from architecture graph. |
| Versioning | `True` | Enabled for data protection and audit traceability (user decision). |
| Encryption | `AES256` | Server-side encryption enforced (user decision). |
| Public Access Block | `True` | Strict public access blocking enabled (user decision). |
| Lifecycle Enabled | `False` | Authoritative user diagram property — no lifecycle rule is enabled by default. |
| Lifecycle Transition Days | `30` | Authoritative user diagram property (transition tier threshold, dormant while lifecycle is disabled). |
| Lifecycle Glacier Days | `90` | Authoritative user diagram property (Glacier transition threshold, dormant while lifecycle is disabled). |

#### 5.3.2 IAM Role & Policies
* Standard S3 bucket policy enforcing TLS-only transport and least-privilege access for EMR, MWAA, and Lambda.

#### 5.3.3 Network Configuration
* VPC Gateway Endpoint for S3 enabled across private subnets.

#### 5.3.4 Security Controls & Encryption
* S3 Block Public Access fully enabled; server-side encryption with KMS CMK.

#### 5.3.5 Observability & Alerting
| Metric | Namespace | Threshold | Alarm Action |
|---|---|---|---|
| `BucketSizeBytes` | `AWS/S3` | > 100 GB | SNS Topic |

#### 5.3.6 Disaster Recovery & Backup
* Versioning enabled; cross-region replication available for disaster recovery.

#### 5.3.7 Infrastructure Assumptions
* Q: What lifecycle retention is enforced on staged export files? / A: The diagram defaults (`lifecycle_enabled=False`, transition 30d, Glacier 90d) are authoritative and remain the bucket's baseline configuration. SCRUM-486 AC#5 additionally mandates an expiration of exactly 7 days on exported staging objects; this is applied as an environment-config-driven lifecycle rule layered on top of the diagram defaults at deployment time (value read from `values.yaml`, never hardcoded), and is flagged for confirmation with the bucket owner before sprint commitment. / Justification: Preserves the user's authoritative diagram properties while satisfying the compliance requirement without silently overriding them.
* Q: How is the 7-day purge verified? / A: Objects aged 6 days remain present; objects aged 8 days are deleted within 48h of lifecycle evaluation. / Justification: Matches SCRUM-486 AC#5 acceptance criteria.

### 5.4 Infrastructure Specification: audit_bucket
#### 5.4.1 Resource Configuration & Sizing
| Parameter | Value | Justification |
|---|---|---|
| Service | Amazon S3 | Centralized audit and execution log store. |
| Bucket Name | `audit_bucket` | Exact user decision from architecture graph. |
| Versioning | `True` | Ensures immutability and compliance tracking. |
| Encryption | `AES256` / KMS CMK | Encrypted at rest. |
| Public Access Block | `True` | Strict public access blocking. |
| Lifecycle Enabled | `False` | Authoritative user diagram property — no lifecycle rule is enabled by default. |
| Lifecycle Transition Days | `30` | Authoritative user diagram property (transition tier threshold, dormant while lifecycle is disabled). |
| Lifecycle Glacier Days | `90` | Authoritative user diagram property (Glacier transition threshold, dormant while lifecycle is disabled). |

#### 5.4.2 IAM Role & Policies
* Append-only bucket policy allowing write access from all pipeline components and read access for authorized auditors.

#### 5.4.3 Network Configuration
* S3 VPC Gateway Endpoint routing.

#### 5.4.4 Security Controls & Encryption
* KMS CMK encryption enforced; object lock enabled for WORM compliance.

#### 5.4.5 Observability & Alerting
| Metric | Namespace | Threshold | Alarm Action |
|---|---|---|---|
| `5xxErrors` | `AWS/S3` | > 0 | SNS Topic |

#### 5.4.6 Disaster Recovery & Backup
* Multi-region replication and versioning enabled.

#### 5.4.7 Infrastructure Assumptions
* Q: What is the log retention standard? / A: 90 days — **pending confirmation** of the single organisation logging-standard value before sprint commitment (SCRUM-486 states the value is not given in the inputs). / Justification: One consistent retention value is applied to all pipeline log groups and the audit store; the exact number must be confirmed by the organisation, not assumed as final.

## 6. Networking, DNS & Content Delivery

### 6.1 Infrastructure Specification: scheduler
#### 6.1.1 Resource Configuration & Sizing
| Parameter | Value | Justification |
|---|---|---|
| Service | Amazon EventBridge | Serverless scheduler for triggering orchestration workflows. |
| Rule Name | `scheduler` | Exact user decision from architecture graph. |
| Schedule Expression | Cron / Rate cadence | Triggers the Export Orchestrator DAG on the configured cadence. |
| Target | Lambda function `laminar-scheduler-trigger-{env}` | MWAA is not a supported EventBridge rule target; a minimal Lambda (allowlisted runtime) calls the MWAA REST API to create the orchestrator DAG run. |
| Target Retry Policy | Maximum retry attempts: 2, maximum event age: 1 hour | Captures transient trigger failures; failed invocations alarm via `FailedInvocations`. |

#### 6.1.2 IAM Role & Policies
EventBridge rule target invocation role (invokes the scheduler-trigger Lambda):
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowEventBridgeInvokeTriggerLambda",
      "Effect": "Allow",
      "Action": [
        "lambda:InvokeFunction"
      ],
      "Resource": "arn:aws:lambda:us-east-1:123456789012:function:laminar-scheduler-trigger-prod"
    }
  ]
}
```
Scheduler-trigger Lambda execution role (calls the MWAA REST API to create the orchestrator DAG run):
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowTriggerLambdaLogs",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:us-east-1:123456789012:log-group:/aws/laminar/lambda/scheduler-trigger:*"
    },
    {
      "Sid": "AllowMwaaRestApiTrigger",
      "Effect": "Allow",
      "Action": [
        "airflow:CreateWebLoginToken"
      ],
      "Resource": "arn:aws:airflow:us-east-1:123456789012:environment/laminar-mwaa-prod"
    }
  ]
}
```
*Note:* MWAA cannot be an EventBridge rule target directly; the trigger Lambda obtains a MWAA web login token and creates the orchestrator DAG run via the Airflow REST API, staying within the SCRUM-485 allowlist (Lambda + MWAA).

#### 6.1.3 Network Configuration
* Fully managed AWS service integration with regional VPC endpoint routing.

#### 6.1.4 Security Controls & Encryption
* Least-privilege IAM invocation policies.

#### 6.1.5 Observability & Alerting
| Metric | Namespace | Threshold | Alarm Action |
|---|---|---|---|
| `Invocations` | `AWS/Events` | N/A | CloudWatch Monitoring |
| `FailedInvocations` | `AWS/Events` | > 0 over 5 mins | SNS Alert Topic `laminar-alerts-topic` |

#### 6.1.6 Disaster Recovery & Backup
* Fully managed serverless resource with 99.99% availability SLA.

#### 6.1.7 Infrastructure Assumptions
* Q: How does the scheduler trigger the orchestrator? / A: Raises scheduled cron events that invoke the Export Orchestrator DAG via EventBridge targets. / Justification: Satisfies SCRUM-480 AC#1.

## 7. Security, Identity & Secrets Management

* **Cross-Service IAM Model:** Least-privilege roles enforce strict boundary policies between MWAA, EMR Serverless, Lambda, and S3.
* **KMS CMK Strategy:** Dedicated customer-managed KMS keys with automated annual rotation encrypt all data at rest across S3 buckets, the RDS database, and SSM parameters.
* **Secrets Management (single store):** All secrets — RDS database credentials, customer SFTP private keys, PGP passphrases, and customer Azure/GCS credentials — are stored exclusively in **AWS Systems Manager Parameter Store (SecureString)** under the `/dataoutbound/` prefix, encrypted with the dedicated KMS CMK, and retrieved at runtime via `ConfigLoader`. AWS Secrets Manager is not used anywhere in this architecture.
* **Credential Encryption at Rest:** Export configuration credential fields are application-encrypted with the KMS CMK before persistence in `cofig_db`; no retrieve path returns plaintext credentials (SCRUM-479 AC#1/AC#3).
* **Audit Logging:** Comprehensive structured JSON audit logs are emitted for every administrative, execution, and transfer event into dedicated log groups with strict retention compliance.

---

## 8. Observability, Monitoring & Alerting

* **Centralised Log Aggregation:** All component logs stream to AWS CloudWatch Log Groups configured with standardized retention matching enterprise governance policies.
* **Distributed Tracing:** CloudEvents-compliant telemetry payloads propagate correlation and lineage UUIDs across every execution boundary.
* **Alerting Pipelines:** Critical operational failures, PGP encryption aborts, and dead-letter queue insertions trigger immediate notifications via Amazon SNS alert topics.

---

## 9. Alternatives Considered

| Alternative | Why Considered | Why Rejected | Final Choice |
|---|---|---|---|
| Apache Airflow on ECS/Fargate (Self-Hosted) | Lower managed service base cost | Operational overhead of scaling, database maintenance, and manual patching | Amazon MWAA |
| AWS Glue Workflows | Native serverless integration with Spark | Less flexible DAG logic, complex dependency modeling compared to Python Airflow DAGs | Amazon MWAA |
| AWS Step Functions | Fully managed state machine execution | Limited native support for dynamic task mapping at scale and Airflow-specific operator ecosystem | Amazon MWAA |

---

## 10. Appendix: Assumptions & Follow-up Question Resolution

| Component | Open Question | Infrastructure Default | Justification |
|---|---|---|---|
| All pipeline log groups & `audit_bucket` | What logging retention period applies to pipeline log groups and audit stores? | 90 days — **pending confirmation** of the single organisation logging-standard value before sprint commitment (SCRUM-486) | SCRUM-486 explicitly states the value is not provided in the inputs; one consistent value is applied everywhere once confirmed. |
| `orchestrator` (n3) | What is the maximum file size and record count per customer export run? | 1 GB and up to 100,000 records | Matches per-customer scale targets under the 24-hour delivery SLA. |
| `orchestrator` (n3) | How are incremental watermark states tracked across runs? | Stored in RDS PostgreSQL (`cofig_db`) | Ensures robust backfill and safe re-run semantics for downstream Spark jobs. |
| `dev` (n6) | Is Redshift managed by this Terraform module? | External source excluded from pipeline allowlist | Conforms to SCRUM-485 specifications. |
| `Customer Azure Blob Destination` (n13) | Are customer destinations managed by Terraform? | Customer-owned and excluded from pipeline allowlist | Conforms to SCRUM-485 specifications. |

## 11. Traceability

| Section | Story | AC# |
| --- | --- | --- |
| 1. Infrastructure Overview & Scope | SCRUM-479 | AC#1, AC#2, AC#3, AC#4, AC#5, AC#6, AC#7 |
| 4.1 Infrastructure Specification: orchestrator | SCRUM-480 | AC#1, AC#2, AC#3, AC#4, AC#5, AC#6, AC#7 |
| 4.3 Infrastructure Specification: child_export_dag | SCRUM-480 | AC#1, AC#2, AC#3, AC#4, AC#5, AC#6, AC#7 |
| 4.1 Infrastructure Specification: orchestrator | SCRUM-481 | AC#1, AC#2, AC#3, AC#4, AC#5, AC#6, AC#7 |
| 4.3 Infrastructure Specification: child_export_dag | SCRUM-481 | AC#1, AC#2, AC#3, AC#4, AC#5, AC#6, AC#7 |
| 4.5 Infrastructure Specification: emr_do | SCRUM-481 | AC#1, AC#2, AC#3, AC#4, AC#5, AC#6, AC#7 |
| 4.2 Infrastructure Specification: egress-lambda | SCRUM-482 | AC#1, AC#2, AC#3, AC#4, AC#5, AC#6, AC#7 |
| 4.6 Infrastructure Specification: egress dag | SCRUM-482 | AC#1, AC#2, AC#3, AC#4, AC#5, AC#6, AC#7 |
| 4.2 Infrastructure Specification: egress-lambda | SCRUM-483 | AC#1, AC#2, AC#3, AC#4 |
| 4.6 Infrastructure Specification: egress dag | SCRUM-483 | AC#1, AC#2, AC#3, AC#4 |
| 4.2 Infrastructure Specification: egress-lambda | SCRUM-484 | AC#1, AC#2, AC#3, AC#4, AC#5, AC#6 |
| 4.6 Infrastructure Specification: egress dag | SCRUM-484 | AC#1, AC#2, AC#3, AC#4, AC#5, AC#6 |
| 4.1 Infrastructure Specification: orchestrator | SCRUM-485 | AC#1, AC#2, AC#3, AC#4, AC#5 |
| 4.3 Infrastructure Specification: child_export_dag | SCRUM-485 | AC#1, AC#2, AC#3, AC#4, AC#5 |
| 4.5 Infrastructure Specification: emr_do | SCRUM-485 | AC#1, AC#2, AC#3, AC#4, AC#5 |
| 4.2 Infrastructure Specification: egress-lambda | SCRUM-485 | AC#1, AC#2, AC#3, AC#4, AC#5 |
| 4.6 Infrastructure Specification: egress dag | SCRUM-485 | AC#1, AC#2, AC#3, AC#4, AC#5 |
| 4.1 Infrastructure Specification: orchestrator | SCRUM-486 | AC#1, AC#2, AC#3, AC#4, AC#5 |
| 4.3 Infrastructure Specification: child_export_dag | SCRUM-486 | AC#1, AC#2, AC#3, AC#4, AC#5 |
| 4.5 Infrastructure Specification: emr_do | SCRUM-486 | AC#1, AC#2, AC#3, AC#4, AC#5 |
| 4.2 Infrastructure Specification: egress-lambda | SCRUM-486 | AC#1, AC#2, AC#3, AC#4, AC#5 |
| 4.6 Infrastructure Specification: egress dag | SCRUM-486 | AC#1, AC#2, AC#3, AC#4, AC#5 |

## 12. Closing & Disclaimer
*(Standard AI generated document disclaimer. Discretion advised. This document has been compiled in accordance with the specified project blueprint, architectural constraints, and AWS Well-Architected Framework guidelines.)*

## Document Structure Summary

- Headings: 12
- Sub-headings: 15
- Sub-sub-headings: 84
