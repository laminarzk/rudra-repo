"""Job constants for the Export Extract Spark Job.

Every literal used by the job (entity names, column names, partition keys,
status values, audit event types and sink defaults) is declared here so that no
magic strings leak into the entrypoint, processors, transformers or I/O
modules.
"""

from __future__ import annotations

from typing import Dict, Final, Tuple

# --------------------------------------------------------------------------
# Module identity / runtime
# --------------------------------------------------------------------------
MODULE_NAME: Final[str] = "spark_emr/export_extract_spark_job"
JOB_APPLICATION_NAME: Final[str] = "emr_do_export_extract"
DEFAULT_ENVIRONMENT: Final[str] = "dev"
DEFAULT_RECIPES_DIR: Final[str] = "config/recipes"
ENVIRONMENT_KEYS: Final[Tuple[str, ...]] = ("default", "dev", "staging", "prod")

EXIT_SUCCESS: Final[int] = 0
EXIT_FAILURE: Final[int] = 1

# --------------------------------------------------------------------------
# Execution modes
# --------------------------------------------------------------------------
EXECUTION_MODE_FULL: Final[str] = "FULL"
EXECUTION_MODE_INCREMENTAL: Final[str] = "INCREMENTAL"
EXECUTION_MODE_ALIASES: Final[Dict[str, str]] = {
    "initial": EXECUTION_MODE_FULL,
    "full": EXECUTION_MODE_FULL,
    "incremental": EXECUTION_MODE_INCREMENTAL,
}

# --------------------------------------------------------------------------
# Golden entities covered by this export pipeline
# --------------------------------------------------------------------------
ENTITY_UNIFIED_IDENTITY_PROFILE: Final[str] = "Unified Identity Profile"
ENTITY_PII_RESTRICTED_PROFILE: Final[str] = "Pii Restricted Profile"
ENTITY_CONTACT_METHOD_SUBSCRIPTION: Final[str] = "Contact Method Subscription"
ENTITY_ENRICHED_CAMPAIGNS: Final[str] = "Enriched Campaigns"
ENTITY_ENRICHED_CAMPAIGN_REACTIONS: Final[str] = "Enriched Campaign Reactions"
ENTITY_RESERVATION_ITEM: Final[str] = "Reservation Item"

ENTITY_TYPES: Final[Tuple[str, ...]] = (
    ENTITY_UNIFIED_IDENTITY_PROFILE,
    ENTITY_PII_RESTRICTED_PROFILE,
    ENTITY_CONTACT_METHOD_SUBSCRIPTION,
    ENTITY_ENRICHED_CAMPAIGNS,
    ENTITY_ENRICHED_CAMPAIGN_REACTIONS,
    ENTITY_RESERVATION_ITEM,
)

RESERVATION_SUBTYPES: Final[Tuple[str, ...]] = (
    "Room stay",
    "Restaurant reservation",
    "Tee time",
    "Spa treatment",
)
RESERVATION_TYPE_COLUMN: Final[str] = "reservation_type"

# --------------------------------------------------------------------------
# Source columns
# --------------------------------------------------------------------------
COLUMN_CREATED_AT: Final[str] = "created_at"
COLUMN_UPDATED_AT: Final[str] = "updated_at"
DEFAULT_INCREMENTAL_COLUMNS: Final[Tuple[str, ...]] = (
    COLUMN_UPDATED_AT,
    COLUMN_CREATED_AT,
)

# --------------------------------------------------------------------------
# Output partitioning
# --------------------------------------------------------------------------
PARTITION_YEAR: Final[str] = "year"
PARTITION_MONTH: Final[str] = "month"
PARTITION_DAY: Final[str] = "day"
DEFAULT_PARTITION_COLUMNS: Final[Tuple[str, ...]] = (
    PARTITION_YEAR,
    PARTITION_MONTH,
    PARTITION_DAY,
)
LINEAGE_PARTITION_PREFIX: Final[str] = "lineage_uuid"

# --------------------------------------------------------------------------
# PII handling rules
# --------------------------------------------------------------------------
PII_ACTION_REDACT: Final[str] = "REDACT"
PII_ACTION_HASH_SHA256: Final[str] = "HASH_SHA256"
PII_ACTION_MASK: Final[str] = "MASK"
PII_ACTIONS: Final[Tuple[str, ...]] = (
    PII_ACTION_REDACT,
    PII_ACTION_HASH_SHA256,
    PII_ACTION_MASK,
)
PII_DEFAULT_MASK_CHAR: Final[str] = "*"
PII_DEFAULT_KEEP_LAST: Final[int] = 4

# --------------------------------------------------------------------------
# Output format / writer defaults
# --------------------------------------------------------------------------
FORMAT_PARQUET: Final[str] = "parquet"
FORMAT_JSON: Final[str] = "json"
FORMAT_CSV: Final[str] = "csv"
SUPPORTED_OUTPUT_FORMATS: Final[Tuple[str, ...]] = (
    FORMAT_PARQUET,
    FORMAT_JSON,
    FORMAT_CSV,
)
SUPPORTED_WRITE_MODES: Final[Tuple[str, ...]] = (
    "append",
    "overwrite",
    "error",
    "ignore",
)
DEFAULT_OUTPUT_FORMAT: Final[str] = FORMAT_PARQUET
DEFAULT_WRITE_MODE: Final[str] = "overwrite"
DEFAULT_MAX_RECORDS_PER_FILE: Final[int] = 100000

# --------------------------------------------------------------------------
# JDBC
# --------------------------------------------------------------------------
JDBC_FORMAT: Final[str] = "jdbc"
DEFAULT_JDBC_FETCH_SIZE: Final[int] = 10000
DEFAULT_JDBC_NUM_PARTITIONS: Final[int] = 8

# --------------------------------------------------------------------------
# Manifest control file
# --------------------------------------------------------------------------
MANIFEST_DIRECTORY: Final[str] = "manifests"
MANIFEST_FILE_PREFIX: Final[str] = "manifest_"

# --------------------------------------------------------------------------
# Audit / telemetry
# --------------------------------------------------------------------------
EVENT_EXECUTION_STARTED: Final[str] = "EXECUTION_STARTED"
EVENT_EXECUTION_COMPLETED: Final[str] = "EXECUTION_COMPLETED"
EVENT_EXECUTION_FAILED: Final[str] = "EXECUTION_FAILED"
SYSTEM_ACTOR: Final[str] = "system:emr_do"
STATUS_STARTED: Final[str] = "STARTED"
STATUS_SUCCESS: Final[str] = "SUCCESS"
STATUS_FAILED: Final[str] = "FAILED"
AUDIT_TIMESTAMP_FORMAT: Final[str] = "%Y-%m-%dT%H:%M:%S.%f"
