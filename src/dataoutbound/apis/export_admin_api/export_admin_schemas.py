"""Request, response and domain models for the Export Admin API.

Model names and shapes mirror the High-Level Design Schema Definitions
(Section 10.2 / 10.5). No HTTP concepts leak into this module.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

TargetType = Literal["s3", "azure_blob", "gcs", "sftp"]
DatasetType = Literal[
    "identity_profiles",
    "subscription_statuses",
    "campaigns_reactions",
    "reservation_items",
]
ExecutionMode = Literal["initial", "incremental"]

CRON_FIELD_COUNT = 5
_CRON_FIELD_PATTERN = re.compile(r"^[0-9*,\-/]+$")


def validate_cron_expression(value: str) -> str:
    """Validate a five-field cron expression.

    Args:
        value: Candidate cron expression.

    Returns:
        The unchanged expression when it is well formed.

    Raises:
        ValueError: When the expression is empty, has the wrong number of
            fields, or contains characters that are not valid cron tokens.
    """
    if not isinstance(value, str) or not value.strip():
        raise ValueError("cron_schedule must be a non-empty 5-field cron expression")
    fields = value.split()
    if len(fields) != CRON_FIELD_COUNT:
        raise ValueError(
            "cron_schedule must contain exactly 5 whitespace-separated fields"
        )
    for field in fields:
        if not _CRON_FIELD_PATTERN.match(field):
            raise ValueError(
                f"cron_schedule field '{field}' contains invalid characters"
            )
    return value


class ExportConfigCreateRequest(BaseModel):
    """Payload accepted by ``POST /api/v1/exports``."""

    model_config = ConfigDict(extra="forbid")

    connection_name: str = Field(..., min_length=1, max_length=100)
    target_type: TargetType
    dataset: DatasetType
    mode: ExecutionMode
    cron_schedule: str = Field(..., description="Valid 5-field cron expression")
    credentials: Dict[str, Any] = Field(
        ..., description="Target connection credentials (stored encrypted)"
    )
    is_active: bool = Field(
        default=True, description="Whether the configuration is enabled"
    )

    @field_validator("cron_schedule")
    @classmethod
    def _check_cron_schedule(cls, value: str) -> str:
        return validate_cron_expression(value)


class ExportConfigUpdateRequest(BaseModel):
    """Payload accepted by ``PUT /api/v1/exports/{config_id}``."""

    model_config = ConfigDict(extra="forbid")

    connection_name: Optional[str] = Field(default=None, min_length=1, max_length=100)
    target_type: Optional[TargetType] = None
    dataset: Optional[DatasetType] = None
    mode: Optional[ExecutionMode] = None
    cron_schedule: Optional[str] = None
    credentials: Optional[Dict[str, Any]] = None
    is_active: Optional[bool] = None

    @field_validator("cron_schedule")
    @classmethod
    def _check_cron_schedule(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        return validate_cron_expression(value)


class ExportConfigResponse(BaseModel):
    """Representation of a persisted export configuration.

    Credential fields are never returned in plaintext; ``credentials_masked``
    carries the field names of the stored credentials with their values masked.
    """

    model_config = ConfigDict(extra="ignore")

    id: int
    connection_name: str
    target_type: str
    dataset: str
    mode: str
    cron_schedule: str
    is_active: bool
    created_at: str
    updated_at: str
    credentials_masked: Optional[Dict[str, str]] = None


class ManualTriggerResponse(BaseModel):
    """Result of ``POST /api/v1/exports/{config_id}/trigger``."""

    message: str
    config_id: int
    run_id: str
    execution_id: str


class ManualStopResponse(BaseModel):
    """Result of ``POST /api/v1/exports/{config_id}/stop``."""

    message: str
    config_id: int
    run_id: Optional[str] = None


class ErrorDetail(BaseModel):
    """Single field-level diagnostic inside an error envelope."""

    field: Optional[str] = None
    message: str


class ApiError(BaseModel):
    """Machine readable error payload."""

    code: str
    message: str
    details: List[ErrorDetail] = Field(default_factory=list)


class ErrorEnvelope(BaseModel):
    """Uniform error envelope returned by every failing endpoint."""

    error: ApiError
