# Final Delivery Review — DataOutbound

## Decision: ❌ NOT APPROVED

The codebase cannot be signed off. Cross-component contracts are broken at nearly every interface boundary, and several components cannot execute their primary path at all.

## Critical cross-module defects (must regenerate)

1. **orchestrator → cofig_db schema breach.** `QUERY_ACTIVE_CONNECTIONS` selects `customer_id`, `extra_parameters`, and aliases `connection_name AS target_bucket` — none exist in `export_configurations` (§7.5: connection_name, target_type, dataset, mode, cron_schedule, encrypted_credentials, is_active, timestamps). Every scheduled run raises a SQL error.
2. **do-admin-api → MwaaClient.** `trigger_run()` passes `api_token=`; `MwaaClient.trigger_dag_run()` has no such parameter → `TypeError` on every manual trigger.
3. **child_export_dag → EmrClient.** DAG calls `start_job_run(..., entry_point_uri=, arguments=, client_token=)`; the client's signature is `name/job_driver/configuration_overrides/tags`. EMR submission fails on first use.
4. **transform_encrypt_dag → shared clients.** Calls `BlobClient.upload_blob/download_blob` and `SftpClient.remote_sha256`, none of which exist. Delivery paths for Azure and SHA-256 read-back verification are dead code.
5. **egress-lambda → transform_encrypt_dag conf contract.** Lambda emits `{lineage_uuid, source_s3_reference, idempotency_key}` only. The DAG needs `destination_type`/`destination` (falls back to `'s3'` with an empty destination dict → validation failure) and never receives PGP/GCP/Azure/SFTP settings. No successful end-to-end transfer is possible.
6. **ChildExportDagRunConf three-way disagreement.** §3.5: `connection_id` string, `target_bucket` required. §6.5: `connection_id` integer, no `target_bucket`. API `_connection_json()`: integer id, no `target_bucket`. One concept, three conflicting contracts.
7. **DAG config key nesting.** Both orchestrator (`config['export_orchestrator_dag']`) and child DAG (`config['child_export_dag']`) read top-level keys that dags/config.yml places under `dags.*` → `ConfigError` at DAG parse; the orchestrator DAG never imports. transform_encrypt_dag additionally mis-handles the ConfigLoader object (`isinstance(config, dict)` is False) and silently discards all its settings.
8. **Dataset → recipe naming gap.** DAG forwards `source_dataset` (`identity_profiles` etc.) as `recipe_name`, but the recipe schema (§15) and transformer require entity names like `Unified Identity Profile`; no reconciliation layer exists.
9. **Audit sink identity drift.** AuditPublisher defaults to `audit_bucket`/`audit` prefix; terraform provisions `audit_execution_log_store_s3`; lambda env sets `AUDIT_PREFIX=export-admin-api/`; none of these values is consumed by the publisher.
10. **Egress lambda ignores its own deployment configuration.** Terraform injects `MWAA_ENVIRONMENT_NAME`, `TRANSFORM_DAG_ID` (values.yaml even says `egress_dag` vs code's `transform_encrypt_dag`), `STAGING_PREFIX` — the code reads none of them and its config.yml key lookup (`egress_lambda`) doesn't match the file's `lambda` section.
11. **Build blocker.** `json` is listed as a pip dependency in both pyproject.toml and requirements.txt — not a real package; installs fail.

## Compliance / governance findings

- **Retention breach:** Global assumption #1 requires 365-day retention everywhere; values.yaml sets 30/90/180 days across scheduler, lambda, API, child-DAG, EMR and (dev) audit log groups.
- **Parity report self-reports 5 significant divergences** (missing ChildDagConfig, S3EventRecord, EgressExecutionResult, ExportPackageConfig/TransferDestinationType/TransferResult, EMR processor classes). The shared foundation is *unverified* — no design chapter matched it.
- **Zero-stub violations:** the scheduler trigger Lambda referenced by terraform (`lambda_function.lambda_handler`, `build/export_scheduler_trigger.zip`) has no source in the repo; dead placeholder members exist in DbClient/SftpClient.
- **Positive:** API error-envelope, structured logging/redaction, Fernet credential handling, fail-closed PGP in the DAG, KMS/S3 hardening in terraform, and audit CloudEvent shapes are largely consistent and of good quality.

## Node actions

| Node | Action |
|---|---|
| export_orchestrator_dag_mwaa | regenerate (SQL contract, config keys) |
| child_export_dag_mwaa | regenerate (EmrClient contract, config keys, dataset naming) |
| transform_encrypt_dag_mwaa | regenerate (client method contracts, conf contract, settings loader) |
| egress_lambda | regenerate (env/config wiring, conf payload completeness) |
| export_admin_api | regenerate (MwaaClient api_token call) |
| shared_foundation | flag (unverified vs design; clients need method additions) |
| export_scheduler_eventbridge | flag (trigger Lambda source missing from repo) |

## Sign-off conditions

Re-review requires: (a) single authoritative ChildExportDagRunConf schema adopted on all three sides; (b) shared clients extended to the interfaces consumers actually call; (c) DAG config key paths aligned with dags/config.yml; (d) audit sink name/prefix unified; (e) scheduler trigger Lambda implemented; (f) dependency lists cleaned; (g) retention values raised to the 365-day standard or the design assumption formally amended and re-approved.

## Blueprint coverage

56 of 56 planned file(s) generated (54 required, 2 optional).

✅ Every required file of the blueprint was generated.

2 committed file(s) are outside the blueprint (carried human work or generator additions):

- `pyproject.toml`
- `requirements.txt`

## Verification & flags

⚠️ 8 component(s) need attention before merge:

| Component | Reason |
|---|---|
| Component | flagged by the principal review |
| Component | flagged by the principal review |
| Component | flagged by the principal review |
| Component | component cannot be regenerated: not produced by this run |
| Component | component cannot be regenerated: not produced by this run |
| Component | component cannot be regenerated: not produced by this run |
| Component | component cannot be regenerated: not produced by this run |
| Component | component cannot be regenerated: not produced by this run |
