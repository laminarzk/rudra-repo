"""Named constants shared across every DataOutbound module.

Nothing in the codebase should hard-code these values; import them instead so
that schemas, logs and audit records stay aligned.
"""

from __future__ import annotations

from typing import Final

# --------------------------------------------------------------------------
# Audit / telemetry event types (see audit_event.json schema)
# --------------------------------------------------------------------------
EVENT_ADMIN_CREATE: Final = "ADMIN_CREATE"
EVENT_ADMIN_UPDATE: Final = "ADMIN_UPDATE"
EVENT_ADMIN_DELETE: Final = "ADMIN_DELETE"
EVENT_ADMIN_MANUAL_TRIGGER: Final = "ADMIN_MANUAL_TRIGGER"
EVENT_EXECUTION_STARTED: Final = "EXECUTION_STARTED"
EVENT_EXECUTION_COMPLETED: Final = "EXECUTION_COMPLETED"
EVENT_EXECUTION_FAILED: Final = "EXECUTION_FAILED"
EVENT_TRANSFER_STARTED: Final = "TRANSFER_STARTED"
EVENT_TRANSFER_COMPLETED: Final = "TRANSFER_COMPLETED"
EVENT_TRANSFER_FAILED: Final = "TRANSFER_FAILED"

ALL_AUDIT_EVENT_TYPES: Final = frozenset(
    {
        EVENT_ADMIN_CREATE,
        EVENT_ADMIN_UPDATE,
        EVENT_ADMIN_DELETE,
        EVENT_ADMIN_MANUAL_TRIGGER,
        EVENT_EXECUTION_STARTED,
        EVENT_EXECUTION_COMPLETED,
        EVENT_EXECUTION_FAILED,
        EVENT_TRANSFER_STARTED,
        EVENT_TRANSFER_COMPLETED,
        EVENT_TRANSFER_FAILED,
    }
)

# --------------------------------------------------------------------------
# CloudEvents (see the Telemetry CloudEvent schema)
# --------------------------------------------------------------------------
CLOUDEVENT_SPEC_VERSION: Final = "1.0"
CLOUDEVENT_CONTENT_TYPE: Final = "application/json"
CLOUDEVENT_SOURCE_PREFIX: Final = "urn:laminar:dataoutbound"
CLOUDEVENT_TYPE_PREFIX: Final = "com.laminar.dataoutbound"

# --------------------------------------------------------------------------
# Status values reused by clients, DAGs and Lambdas
# --------------------------------------------------------------------------
STATUS_SUCCESS: Final = "SUCCESS"
STATUS_FAILED: Final = "FAILED"
STATUS_PENDING: Final = "PENDING"
STATUS_RUNNING: Final = "RUNNING"
STATUS_CANCELLED: Final = "CANCELLED"

# --------------------------------------------------------------------------
# HTTP header names
# --------------------------------------------------------------------------
HEADER_AUTHORIZATION: Final = "Authorization"
HEADER_CORRELATION_ID: Final = "X-Correlation-Id"
HEADER_REQUEST_ID: Final = "X-Request-Id"
HEADER_IDEMPOTENCY_KEY: Final = "Idempotency-Key"
HEADER_CONTENT_TYPE: Final = "Content-Type"

# --------------------------------------------------------------------------
# Supported customer destinations
# --------------------------------------------------------------------------
DESTINATION_S3: Final = "s3"
DESTINATION_AZURE_BLOB: Final = "azure_blob"
DESTINATION_GCS: Final = "gcs"
DESTINATION_SFTP: Final = "sftp"

ALL_DESTINATION_TYPES: Final = frozenset(
    {
        DESTINATION_S3,
        DESTINATION_AZURE_BLOB,
        DESTINATION_GCS,
        DESTINATION_SFTP,
    }
)

# --------------------------------------------------------------------------
# Environment variable names
# --------------------------------------------------------------------------
ENV_DATAOUTBOUND_ENVIRONMENT: Final = "DATAOUTBOUND_ENV"
ENV_DATAOUTBOUND_CONFIG_ROOT: Final = "DATAOUTBOUND_CONFIG_ROOT"
ENV_DATAOUTBOUND_LOG_LEVEL: Final = "DATAOUTBOUND_LOG_LEVEL"
ENV_AWS_REGION: Final = "AWS_REGION"

DEFAULT_ENVIRONMENT: Final = "dev"
DEFAULT_LOG_LEVEL: Final = "INFO"
DEFAULT_AWS_REGION: Final = "us-east-1"

# --------------------------------------------------------------------------
# Secret reference scheme used inside configuration files
# --------------------------------------------------------------------------
SECRET_SCHEME_PREFIXES: Final = ("secret://", "ssm://")
