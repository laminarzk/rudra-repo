# Design ↔ Code Parity Report

Design document version after this run: **v1.0** · significant amendments: **5** · minor amendments: **0** · not verifiable: **1**

⚠️ 1 component(s) could not be compared with the design document — no component chapter matched them (rows marked *unverified*). Their code is NOT confirmed to match the design.

Code was generated to match the binding sections of the approved design document. Where it could not, the document was amended in place (see its Amendment Log) so both stay in sync; significant amendments should be re-reviewed.

| Component | Section | Outcome | Note |
|---|---|---|---|
| Child Export DAG (MWAA) | class_interface | amended-significant | ChildDagConfig: ChildDagConfig → missing |
| Egress Lambda | class_interface | amended-significant | EgressExecutionResult: EgressExecutionResult → missing; S3EventRecord: S3EventRecord → missing |
| Export Admin API | skeleton | amended-significant | POST /{config_id}/stop: auth dependency → no auth dependency |
| Export Extract Spark Job (EMR Serverless) | class_interface | amended-significant | ExportExtractInputProcessor: ExportExtractInputProcessor → missing; ExportExtractOutputProcessor: ExportExtractOutputProcessor → missing; ExportExtractTransformer: ExportExtractTransformer → missing |
| Export Orchestrator DAG (MWAA) | all | conformant |  |
| Shared foundation | all | unverified | no component chapter in the design document matched this component (looked for: shared foundation, __foundation__) |
| Transform & Encrypt DAG (MWAA) | class_interface | amended-significant | ExportPackageConfig: ExportPackageConfig → missing; TransferDestinationType: TransferDestinationType → missing; TransferResult: TransferResult → missing |
