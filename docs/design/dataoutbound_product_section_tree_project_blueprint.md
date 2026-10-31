# Project Blueprint

**Coordinates:** `com.laminar:dataoutbound-section-tree` · **Language:** `python` (python_src) · **Source root:** `src/dataoutbound_section_tree` · **Tests:** `tests` · **Blueprint hash:** `6d6cd22eb4d2`

Structure versions: `api` v1, `dag` v1, `foundation` v1, `lambda` v1, `spark` v1, `terraform` v1

## Layout

```text
.
├── src
│   └── dataoutbound_section_tree
│       ├── apis
│       │   ├── export_admin_api
│       │   │   ├── app.py
│       │   │   ├── dependencies.py
│       │   │   ├── export_admin_routes.py
│       │   │   ├── export_admin_schemas.py
│       │   │   └── export_admin_service.py
│       │   └── config.yml
│       ├── common
│       │   ├── clients
│       │   │   ├── aws
│       │   │   │   ├── emr
│       │   │   │   │   └── emr_client.py
│       │   │   │   ├── eventbridge
│       │   │   │   │   └── eventbridge_client.py
│       │   │   │   ├── mwaa
│       │   │   │   │   └── mwaa_client.py
│       │   │   │   ├── s3
│       │   │   │   │   └── s3_client.py
│       │   │   │   ├── sqs
│       │   │   │   │   └── sqs_client.py
│       │   │   │   ├── ssm
│       │   │   │   │   └── ssm_client.py
│       │   │   │   └── session.py
│       │   │   ├── azure
│       │   │   │   └── blob_client.py
│       │   │   ├── db
│       │   │   │   └── db_client.py
│       │   │   ├── gcp
│       │   │   │   └── gcs_client.py
│       │   │   ├── rest
│       │   │   │   └── rest_client.py
│       │   │   └── sftp
│       │   │       └── sftp_client.py
│       │   ├── logging
│       │   │   └── logger.py
│       │   ├── security
│       │   │   └── auth.py
│       │   └── telemetry
│       │       └── auditor.py
│       ├── config
│       │   ├── common_config.yaml
│       │   └── config_loader.py
│       ├── dags
│       │   ├── child_export_dag.py
│       │   ├── config.yml
│       │   ├── export_orchestrator_dag.py
│       │   └── transform_encrypt_dag.py
│       ├── lambdas
│       │   ├── config.yml
│       │   └── egress_lambda.py
│       ├── spark_emr
│       │   └── export_extract_spark_job
│       │       ├── config
│       │       │   ├── recipes
│       │       │   │   ├── contact_method_subscription_recipe.json
│       │       │   │   ├── enriched_campaign_reactions_recipe.json
│       │       │   │   ├── enriched_campaigns_recipe.json
│       │       │   │   ├── pii_restricted_profile_recipe.json
│       │       │   │   ├── restaurant_reservation_recipe.json
│       │       │   │   ├── room_stay_reservation_recipe.json
│       │       │   │   ├── spa_treatment_reservation_recipe.json
│       │       │   │   └── tee_time_reservation_recipe.json
│       │       │   └── config.yaml
│       │       ├── helper
│       │       │   └── spark_session_helper.py
│       │       ├── io
│       │       │   ├── child_export_golden_entity_source_store_reader.py
│       │       │   └── customer_staging_buckets_audit_execution_log_store_writer.py
│       │       ├── processor
│       │       │   ├── export_extract_input_processor.py
│       │       │   └── export_extract_output_processor.py
│       │       ├── transform
│       │       │   └── export_extract_transformer.py
│       │       ├── util
│       │       │   ├── constants.py
│       │       │   └── file_utils.py
│       │       └── main.py
│       └── util
│           ├── constants.py
│           ├── exceptions.py
│           ├── idempotency.py
│           └── utils.py
└── terraform
    ├── api.tf
    ├── blob_store.tf
    ├── emr.tf
    ├── eventbridge.tf
    ├── gcs.tf
    ├── lambda.tf
    ├── main.tf
    ├── mwaa.tf
    ├── rds.tf
    ├── redshift.tf
    ├── s3.tf
    ├── values.yaml
    └── variables.tf
```

## Shared foundation

Module type `foundation` · generated as `module_bundle` · root `src/dataoutbound_section_tree`

| Path | Kind | Purpose / responsibility |
|---|---|---|
| `src/dataoutbound_section_tree/common` | folder | Cross-cutting code shared by every component: clients, logging, security, telemetry. Never contains business logic. |
| `src/dataoutbound_section_tree/common/clients` | folder | Thin, typed wrappers over external systems; one sub-package per cloud or protocol. The only place SDK clients are constructed; every method maps SDK errors to util.exceptions and logs through the shared logger. |
| `src/dataoutbound_section_tree/common/clients/aws` | folder | AWS service clients built on boto3 with one shared session factory, retries and structured error mapping. |
| `src/dataoutbound_section_tree/common/clients/aws/emr` | folder | EMR Serverless job submission and status polling. |
| `src/dataoutbound_section_tree/common/clients/aws/emr/emr_client.py` | file | EmrClient: start_job_run / get_job_run / cancel with typed job states. |
| `src/dataoutbound_section_tree/common/clients/aws/eventbridge` | folder | Event publishing to the solution event bus. |
| `src/dataoutbound_section_tree/common/clients/aws/eventbridge/eventbridge_client.py` | file | EventBridgeClient: put_events with CloudEvents-shaped detail and batching. |
| `src/dataoutbound_section_tree/common/clients/aws/mwaa` | folder | Managed Airflow control operations: trigger, inspect and stop DAG runs. |
| `src/dataoutbound_section_tree/common/clients/aws/mwaa/mwaa_client.py` | file | MwaaClient: trigger_dag_run / get_dag_run / cancel via the MWAA CLI token or REST API. |
| `src/dataoutbound_section_tree/common/clients/aws/s3` | folder | Object storage operations (get/put/list/multipart, presigned URLs) for the solution buckets. |
| `src/dataoutbound_section_tree/common/clients/aws/s3/s3_client.py` | file | S3Client: upload/download/list/delete with server-side encryption and typed errors. |
| `src/dataoutbound_section_tree/common/clients/aws/session.py` | file | Single boto3 session/client factory with region, retry and endpoint configuration from ConfigLoader. |
| `src/dataoutbound_section_tree/common/clients/aws/sqs` | folder | Queue send/receive/delete with visibility and DLQ handling. |
| `src/dataoutbound_section_tree/common/clients/aws/sqs/sqs_client.py` | file | SqsClient: send/receive/delete/batch with typed messages. |
| `src/dataoutbound_section_tree/common/clients/aws/ssm` | folder | Parameter Store access for runtime configuration and secrets references. |
| `src/dataoutbound_section_tree/common/clients/aws/ssm/ssm_client.py` | file | SsmClient: get/put parameters by prefix with decryption and caching. |
| `src/dataoutbound_section_tree/common/clients/azure` | folder | Azure service clients (identity, storage) for solutions that read from or deliver to Azure. |
| `src/dataoutbound_section_tree/common/clients/azure/blob_client.py` | file | BlobClient: upload/download/list for Blob Storage / ADLS Gen2 containers with managed identity or SAS. |
| `src/dataoutbound_section_tree/common/clients/db` | folder | Relational database access: connection factory, pooled sessions, parameterised queries and transactions. |
| `src/dataoutbound_section_tree/common/clients/db/db_client.py` | file | DbClient: engine/connection from ConfigLoader + secrets, execute/fetch helpers, transaction context manager. Never builds SQL from untrusted strings. |
| `src/dataoutbound_section_tree/common/clients/gcp` | folder | Google Cloud service clients for solutions that read from or deliver to GCP. |
| `src/dataoutbound_section_tree/common/clients/gcp/gcs_client.py` | file | GcsClient: upload/download/list for Cloud Storage buckets with service-account or workload identity. |
| `src/dataoutbound_section_tree/common/clients/rest` | folder | Outbound HTTP: one client with timeouts, retries with backoff, correlation-id propagation and typed errors. |
| `src/dataoutbound_section_tree/common/clients/rest/rest_client.py` | file | RestClient: get/post/put/delete with timeout, retry, auth header injection and JSON error mapping. |
| `src/dataoutbound_section_tree/common/clients/sftp` | folder | SFTP transfer to and from partner servers with key-based auth and resumable uploads. |
| `src/dataoutbound_section_tree/common/clients/sftp/sftp_client.py` | file | SftpClient: connect (key/password from secrets), upload/download/list with atomic rename on completion. |
| `src/dataoutbound_section_tree/common/logging` | folder | Structured JSON logging shared by every component; the only logger factory in the codebase. |
| `src/dataoutbound_section_tree/common/logging/logger.py` | file | get_logger(name) returning a structured logger with correlation-id and component fields; level from ConfigLoader; never logs PII or secrets. |
| `src/dataoutbound_section_tree/common/security` | folder | Authentication and authorization helpers: token verification, subject extraction, role checks. |
| `src/dataoutbound_section_tree/common/security/auth.py` | file | verify_token / current_subject / require_role helpers over the configured identity provider; raises util.exceptions.AuthError. |
| `src/dataoutbound_section_tree/common/telemetry` | folder | Audit events, metrics and tracing emitted by every component in one shape. |
| `src/dataoutbound_section_tree/common/telemetry/auditor.py` | file | AuditPublisher.publish(event_type, payload) writing CloudEvents-shaped records to the configured sink; never raises into the caller's flow. |
| `src/dataoutbound_section_tree/config` | folder | Runtime configuration: the shared base file and the loader that merges it with per-module config and environment overlays. |
| `src/dataoutbound_section_tree/config/common_config.yaml` | file | Base configuration shared by all modules (logging level, regions, endpoints, retry policy) with dev/staging/prod sections. |
| `src/dataoutbound_section_tree/config/config_loader.py` | file | ConfigLoader: load common_config.yaml + <module>/config.yml, apply the environment section, resolve secret references via common.clients; typed accessors. |
| `src/dataoutbound_section_tree/util` | folder | Pure helpers with no I/O: constants, exceptions, idempotency and small utilities. |
| `src/dataoutbound_section_tree/util/constants.py` | file | Named constants shared across modules (event types, status values, header names). No magic strings elsewhere. |
| `src/dataoutbound_section_tree/util/exceptions.py` | file | Domain exception hierarchy (LaminarError base; ConfigError, ClientError, NotFoundError, ConflictError, AuthError, ValidationError). |
| `src/dataoutbound_section_tree/util/idempotency.py` | file | IdempotencyKey.build(*parts) and retry/backoff helpers used by APIs, lambdas and DAG tasks. |
| `src/dataoutbound_section_tree/util/utils.py` | file | Small pure helpers (time formatting, chunking, safe JSON) used by more than one module. |
| `src/dataoutbound_section_tree/lambdas` | folder | All serverless function handlers of the solution plus their shared configuration. One module per function. |
| `src/dataoutbound_section_tree/lambdas/config.yml` | file | Function configuration per environment (timeouts, batch sizes, destination names, feature flags), loaded through ConfigLoader. |
| `src/dataoutbound_section_tree/dags` | folder | All Airflow / MWAA DAG definitions of the solution plus their shared configuration. Deployed as-is to the DAGs bucket. |
| `src/dataoutbound_section_tree/dags/config.yml` | file | Schedules, retries, SLAs, connection ids and per-DAG parameters per environment, loaded through ConfigLoader. |

## Export Extract Spark Job (EMR Serverless) (`aws_emr_serverless`)

Module type `spark` · generated as `module_bundle` · root `src/dataoutbound_section_tree/spark_emr/export_extract_spark_job`

| Path | Kind | Purpose / responsibility |
|---|---|---|
| `src/dataoutbound_section_tree/spark_emr` | folder | All Spark / EMR job modules of the solution; one sub-package per job component. |
| `src/dataoutbound_section_tree/spark_emr/export_extract_spark_job` | folder | One Spark job: entrypoint, I/O, processors, transforms, configuration and recipes. |
| `src/dataoutbound_section_tree/spark_emr/export_extract_spark_job/config` | folder | Job configuration and declarative recipes. |
| `src/dataoutbound_section_tree/spark_emr/export_extract_spark_job/config/config.yaml` | file | Job configuration per environment: sources, sinks, Spark conf, recipe directory, PII policy references. |
| `src/dataoutbound_section_tree/spark_emr/export_extract_spark_job/config/recipes` | folder (fluid) | Declarative extract/transform recipes (one JSON per product/dataset) interpreted by the processors. |
| `src/dataoutbound_section_tree/spark_emr/export_extract_spark_job/config/recipes/contact_method_subscription_recipe.json` | file | Declarative extract/transform recipe for the Contact Method Subscription Golden Entity, naming source table, projection and output layout. |
| `src/dataoutbound_section_tree/spark_emr/export_extract_spark_job/config/recipes/enriched_campaign_reactions_recipe.json` | file | Declarative extract/transform recipe for the Enriched Campaign Reactions Golden Entity, naming source table, projection and output layout. |
| `src/dataoutbound_section_tree/spark_emr/export_extract_spark_job/config/recipes/enriched_campaigns_recipe.json` | file | Declarative extract/transform recipe for the Enriched Campaigns Golden Entity, naming source table, projection and output layout. |
| `src/dataoutbound_section_tree/spark_emr/export_extract_spark_job/config/recipes/pii_restricted_profile_recipe.json` | file | Declarative extract/transform recipe for the Pii Restricted Profile Golden Entity, declaring the PII masking rules applied to restricted attributes. |
| `src/dataoutbound_section_tree/spark_emr/export_extract_spark_job/config/recipes/restaurant_reservation_recipe.json` | file | Declarative extract/transform recipe for the Restaurant reservation item dataset, naming source table, projection and output layout. |
| `src/dataoutbound_section_tree/spark_emr/export_extract_spark_job/config/recipes/room_stay_reservation_recipe.json` | file | Declarative extract/transform recipe for the Room stay reservation item dataset, naming source table, projection and output layout. |
| `src/dataoutbound_section_tree/spark_emr/export_extract_spark_job/config/recipes/spa_treatment_reservation_recipe.json` | file | Declarative extract/transform recipe for the Spa treatment reservation item dataset, naming source table, projection and output layout. |
| `src/dataoutbound_section_tree/spark_emr/export_extract_spark_job/config/recipes/tee_time_reservation_recipe.json` | file | Declarative extract/transform recipe for the Tee time reservation item dataset, naming source table, projection and output layout. |
| `src/dataoutbound_section_tree/spark_emr/export_extract_spark_job/helper` | folder | Spark runtime helpers (session construction, Spark conf) with no job logic. |
| `src/dataoutbound_section_tree/spark_emr/export_extract_spark_job/helper/spark_session_helper.py` | file | get_spark_session(app_name, config) applying Spark conf, Hive/Glue catalog and S3 committer settings. |
| `src/dataoutbound_section_tree/spark_emr/export_extract_spark_job/io` | folder (fluid) | All reads and writes against external systems, one module per system. No business logic; returns/accepts DataFrames. |
| `src/dataoutbound_section_tree/spark_emr/export_extract_spark_job/io/child_export_golden_entity_source_store_reader.py` | file | JDBC reads (partitioned) and writes against the relational source/sink using credentials from common.clients.db / secrets. |
| `src/dataoutbound_section_tree/spark_emr/export_extract_spark_job/io/customer_staging_buckets_audit_execution_log_store_writer.py` | file | Read/write Parquet/CSV/JSON datasets on S3 with partitioning and manifest writing. |
| `src/dataoutbound_section_tree/spark_emr/export_extract_spark_job/main.py` | file | Job entrypoint: argument parsing, SparkSession via helper, config/recipe loading, orchestration of processors and I/O, exit codes and audit events. |
| `src/dataoutbound_section_tree/spark_emr/export_extract_spark_job/processor` | folder (fluid) | Job step logic: shape inputs, apply business rules (e.g. PII handling, projections), assemble outputs. One processor per concern. |
| `src/dataoutbound_section_tree/spark_emr/export_extract_spark_job/processor/export_extract_input_processor.py` | file | Validate and normalise the input DataFrames (schema enforcement, type casting, filtering by job parameters). |
| `src/dataoutbound_section_tree/spark_emr/export_extract_spark_job/processor/export_extract_output_processor.py` | file | Assemble the final output datasets (ordering, partition columns, manifests, metrics) before I/O writes. |
| `src/dataoutbound_section_tree/spark_emr/export_extract_spark_job/transform` | folder (fluid) | Reusable, stateless DataFrame transformers driven by recipe/config; no I/O. |
| `src/dataoutbound_section_tree/spark_emr/export_extract_spark_job/transform/export_extract_transformer.py` | file | Default column mapping / renaming / derivation transformer applied when a recipe names no specific transformer. |
| `src/dataoutbound_section_tree/spark_emr/export_extract_spark_job/util` | folder | Job-local pure helpers and constants. |
| `src/dataoutbound_section_tree/spark_emr/export_extract_spark_job/util/constants.py` | file | Job constants (column names, partition keys, status values). |
| `src/dataoutbound_section_tree/spark_emr/export_extract_spark_job/util/file_utils.py` | file | Path building, manifest/file naming and small file helpers (no Spark dependency). |

## Egress Lambda (`aws_lambda`)

Module type `lambda` · generated as `single_file` · root `src/dataoutbound_section_tree/lambdas`

| Path | Kind | Purpose / responsibility |
|---|---|---|
| `src/dataoutbound_section_tree/lambdas` | folder | All serverless function handlers of the solution plus their shared configuration. One module per function. |
| `src/dataoutbound_section_tree/lambdas/config.yml` | file | Function configuration per environment (timeouts, batch sizes, destination names, feature flags), loaded through ConfigLoader. |
| `src/dataoutbound_section_tree/lambdas/egress_lambda.py` | file | lambda_handler plus the function's operations: event parsing with the designed schema, client calls through common.clients, error mapping, audit events. |

## Transform & Encrypt DAG (MWAA) (`aws_mwaa_dag`)

Module type `dag` · generated as `single_file` · root `src/dataoutbound_section_tree/dags`

| Path | Kind | Purpose / responsibility |
|---|---|---|
| `src/dataoutbound_section_tree/dags` | folder | All Airflow / MWAA DAG definitions of the solution plus their shared configuration. Deployed as-is to the DAGs bucket. |
| `src/dataoutbound_section_tree/dags/config.yml` | file | Schedules, retries, SLAs, connection ids and per-DAG parameters per environment, loaded through ConfigLoader. |
| `src/dataoutbound_section_tree/dags/transform_encrypt_dag.py` | file | The DAG definition: default_args, schedule from config, tasks in the designed sequence, dependencies, callbacks publishing audit events. |

## Export Orchestrator DAG (MWAA) (`aws_mwaa_dag`)

Module type `dag` · generated as `single_file` · root `src/dataoutbound_section_tree/dags`

| Path | Kind | Purpose / responsibility |
|---|---|---|
| `src/dataoutbound_section_tree/dags` | folder | All Airflow / MWAA DAG definitions of the solution plus their shared configuration. Deployed as-is to the DAGs bucket. |
| `src/dataoutbound_section_tree/dags/config.yml` | file | Schedules, retries, SLAs, connection ids and per-DAG parameters per environment, loaded through ConfigLoader. |
| `src/dataoutbound_section_tree/dags/export_orchestrator_dag.py` | file | The DAG definition: default_args, schedule from config, tasks in the designed sequence, dependencies, callbacks publishing audit events. |

## Child Export DAG (MWAA) (`aws_mwaa_dag`)

Module type `dag` · generated as `single_file` · root `src/dataoutbound_section_tree/dags`

| Path | Kind | Purpose / responsibility |
|---|---|---|
| `src/dataoutbound_section_tree/dags` | folder | All Airflow / MWAA DAG definitions of the solution plus their shared configuration. Deployed as-is to the DAGs bucket. |
| `src/dataoutbound_section_tree/dags/config.yml` | file | Schedules, retries, SLAs, connection ids and per-DAG parameters per environment, loaded through ConfigLoader. |
| `src/dataoutbound_section_tree/dags/child_export_dag.py` | file | The DAG definition: default_args, schedule from config, tasks in the designed sequence, dependencies, callbacks publishing audit events. |

## Export Admin API (`standard_api`)

Module type `api` · generated as `module_bundle` · root `src/dataoutbound_section_tree/apis/export_admin_api`

| Path | Kind | Purpose / responsibility |
|---|---|---|
| `src/dataoutbound_section_tree/apis` | folder | All REST/API modules of the solution; one sub-package per API component plus the shared API configuration. |
| `src/dataoutbound_section_tree/apis/config.yml` | file | API-wide configuration (host, port, CORS, auth issuer, rate limits) per environment, loaded through ConfigLoader. |
| `src/dataoutbound_section_tree/apis/export_admin_api` | folder | One API component: entrypoint, routes, service, schemas and wiring for a single bounded context. |
| `src/dataoutbound_section_tree/apis/export_admin_api/app.py` | file | Application factory: creates the app, registers routers and middleware, wires dependencies, exposes health endpoint. |
| `src/dataoutbound_section_tree/apis/export_admin_api/dependencies.py` | file | Dependency providers: ConfigLoader, AuditPublisher, common.clients instances, current subject (auth); single construction point. |
| `src/dataoutbound_section_tree/apis/export_admin_api/export_admin_routes.py` | file | HTTP route handlers exactly as in the design skeleton (method, path, status codes, auth dependency); delegates to service.py; maps domain exceptions to HTTP errors. |
| `src/dataoutbound_section_tree/apis/export_admin_api/export_admin_schemas.py` | file | Request/response and domain models (pydantic) named as in the design document's Schema Definitions. |
| `src/dataoutbound_section_tree/apis/export_admin_api/export_admin_service.py` | file | Business operations behind the routes: validation, orchestration of clients, idempotency, audit publishing. No HTTP types. |

## Infrastructure (Terraform)

One root module; resources grouped by service type, configured per environment through `values.yaml`.

| Component | Resource name | File |
|---|---|---|
| audit execution log store s3 | `audit_execution_log_store_s3` | `terraform/s3.tf` |
| Child Export DAG (MWAA) | `child_export_dag_mwaa` | `terraform/mwaa.tf` |
| customer azure blob destination blob store | `customer_azure_blob_destination_blob_store` | `terraform/blob_store.tf` |
| customer gcs destination gcs | `customer_gcs_destination_gcs` | `terraform/gcs.tf` |
| customer staging buckets s3 | `customer_staging_buckets_s3` | `terraform/s3.tf` |
| Egress Lambda | `egress_lambda` | `terraform/lambda.tf` |
| Export Admin API | `export_admin_api` | `terraform/api.tf` |
| export config store rds | `export_config_store_rds` | `terraform/rds.tf` |
| Export Extract Spark Job (EMR Serverless) | `export_extract_spark_job_emr` | `terraform/emr.tf` |
| Export Orchestrator DAG (MWAA) | `export_orchestrator_dag_mwaa` | `terraform/mwaa.tf` |
| export scheduler eventbridge | `export_scheduler_eventbridge` | `terraform/eventbridge.tf` |
| golden entity source store redshift | `golden_entity_source_store_redshift` | `terraform/redshift.tf` |
| Transform & Encrypt DAG (MWAA) | `transform_encrypt_dag_mwaa` | `terraform/mwaa.tf` |

## Files named for this architecture

- `src/dataoutbound_section_tree/spark_emr/export_extract_spark_job/config/recipes/contact_method_subscription_recipe.json` (Export Extract Spark Job (EMR Serverless))
- `src/dataoutbound_section_tree/spark_emr/export_extract_spark_job/config/recipes/enriched_campaign_reactions_recipe.json` (Export Extract Spark Job (EMR Serverless))
- `src/dataoutbound_section_tree/spark_emr/export_extract_spark_job/config/recipes/enriched_campaigns_recipe.json` (Export Extract Spark Job (EMR Serverless))
- `src/dataoutbound_section_tree/spark_emr/export_extract_spark_job/config/recipes/pii_restricted_profile_recipe.json` (Export Extract Spark Job (EMR Serverless))
- `src/dataoutbound_section_tree/spark_emr/export_extract_spark_job/config/recipes/restaurant_reservation_recipe.json` (Export Extract Spark Job (EMR Serverless))
- `src/dataoutbound_section_tree/spark_emr/export_extract_spark_job/config/recipes/room_stay_reservation_recipe.json` (Export Extract Spark Job (EMR Serverless))
- `src/dataoutbound_section_tree/spark_emr/export_extract_spark_job/config/recipes/spa_treatment_reservation_recipe.json` (Export Extract Spark Job (EMR Serverless))
- `src/dataoutbound_section_tree/spark_emr/export_extract_spark_job/config/recipes/tee_time_reservation_recipe.json` (Export Extract Spark Job (EMR Serverless))
