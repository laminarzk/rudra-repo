"""Pure helpers for the Export Extract Spark Job.

Contains timestamp formatting/parsing, audit payload assembly, recipe value
resolution, execution-mode normalisation and every staging/manifest path used by
the job. Nothing in this module imports Spark so all helpers stay unit-testable
without a running SparkSession.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from dataoutbound.spark_emr.export_extract_spark_job.util.constants import (
    AUDIT_TIMESTAMP_FORMAT,
    EXECUTION_MODE_ALIASES,
    EXECUTION_MODE_FULL,
    EXECUTION_MODE_INCREMENTAL,
    LINEAGE_PARTITION_PREFIX,
    MANIFEST_DIRECTORY,
    MANIFEST_FILE_PREFIX,
    SYSTEM_ACTOR,
)
from dataoutbound.util.exceptions import ValidationError

_RESERVED_AUDIT_FIELDS: Tuple[str, ...] = (
    "actor_id",
    "timestamp_utc",
    "event_type",
    "execution_id",
)
_PATH_SEGMENT_PATTERN = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.:-]*$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_S3_SCHEMES: Tuple[str, ...] = ("s3", "s3a", "s3n")
_SHA256_CHUNK_SIZE = 1024 * 1024


def utc_now_iso_millis() -> str:
    """Return the current UTC instant in the audit schema format."""
    return format_utc_millis(datetime.now(timezone.utc))


def format_utc_millis(moment: datetime) -> str:
    """Format a datetime as ISO-8601 UTC with millisecond precision and a Z suffix."""
    if not isinstance(moment, datetime):
        raise ValidationError("moment must be a datetime instance")
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    rendered = moment.astimezone(timezone.utc).strftime(AUDIT_TIMESTAMP_FORMAT)
    return f"{rendered[:-3]}Z"


def parse_iso8601_utc(value: Any) -> datetime:
    """Parse an ISO-8601 timestamp (``Z`` or an offset) into an aware UTC datetime."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if value is None:
        raise ValidationError("timestamp value is required")

    text = str(value).strip()
    if not text:
        raise ValidationError("timestamp value must not be empty")
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValidationError(f"Invalid ISO-8601 timestamp: {value!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def build_audit_payload(
    event_type: str,
    execution_id: str,
    actor_id: Optional[str] = None,
    status: Optional[str] = None,
    extra: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a business audit record following the pipeline audit schema."""
    if not isinstance(event_type, str) or not event_type.strip():
        raise ValidationError("event_type is required for an audit record")
    if not isinstance(execution_id, str) or not execution_id.strip():
        raise ValidationError("execution_id is required for an audit record")

    payload: Dict[str, Any] = {
        "actor_id": actor_id or SYSTEM_ACTOR,
        "timestamp_utc": utc_now_iso_millis(),
        "event_type": event_type.strip(),
        "execution_id": execution_id.strip(),
    }
    if status:
        payload["status"] = status

    if extra:
        for key, value in extra.items():
            if key in _RESERVED_AUDIT_FIELDS or value is None:
                continue
            payload[str(key)] = value
    return payload


def get_recipe_value(
    recipe: Optional[Mapping[str, Any]],
    key: str,
    default: Any = None,
) -> Any:
    """Safely read a key from a recipe mapping without raising on None."""
    if not isinstance(recipe, Mapping):
        return default
    value = recipe.get(key, default)
    if value is None:
        return default
    return value


def as_list(value: Any) -> List[Any]:
    """Normalise a scalar or sequence recipe value into a list."""
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, Sequence):
        return list(value)
    return [value]


def normalise_execution_mode(value: Optional[str]) -> str:
    """Resolve the execution mode to ``FULL`` or ``INCREMENTAL``."""
    if value is None:
        return EXECUTION_MODE_FULL
    text = str(value).strip()
    if not text:
        return EXECUTION_MODE_FULL

    upper = text.upper()
    if upper in (EXECUTION_MODE_FULL, EXECUTION_MODE_INCREMENTAL):
        return upper

    alias = EXECUTION_MODE_ALIASES.get(text.lower())
    if alias is None:
        raise ValidationError(f"Unsupported execution mode: {value!r}")
    return alias


def validate_identifier(value: Any, label: str) -> str:
    """Validate that ``value`` is a plain SQL-safe identifier."""
    if not isinstance(value, str):
        raise ValidationError(f"{label} must be a string")
    candidate = value.strip()
    if not _IDENTIFIER_PATTERN.match(candidate):
        raise ValidationError(f"{label} must match [A-Za-z_][A-Za-z0-9_]*: {value!r}")
    return candidate


def sanitize_path_segment(value: Any) -> str:
    """Normalise a value into a safe S3 path segment."""
    if value is None:
        raise ValidationError("path segment value is required")
    candidate = str(value).strip().replace(" ", "_")
    if not candidate or not _PATH_SEGMENT_PATTERN.match(candidate):
        raise ValidationError(f"Unsafe path segment: {value!r}")
    return candidate


def join_uri(base_uri: str, *segments: Any) -> str:
    """Join a base URI with one or more cleanup-safe path segments."""
    if not isinstance(base_uri, str) or not base_uri.strip():
        raise ValidationError("base_uri is required")
    parts = [base_uri.strip().rstrip("/")]
    for segment in segments:
        if segment is None:
            continue
        text = str(segment).strip("/")
        if text:
            parts.append(text)
    return "/".join(parts)


def build_dataset_uri(base_uri: str, dataset: str, lineage_uuid: str) -> str:
    """Build the lineage-partitioned output prefix for one dataset."""
    return join_uri(
        base_uri,
        sanitize_path_segment(dataset),
        f"{LINEAGE_PARTITION_PREFIX}={sanitize_path_segment(lineage_uuid)}",
    )


def manifest_file_name(lineage_uuid: str) -> str:
    """Return the manifest control-file name for a lineage UUID."""
    return f"{MANIFEST_FILE_PREFIX}{sanitize_path_segment(lineage_uuid)}.json"


def build_manifest_uri(base_uri: str, dataset: str, lineage_uuid: str) -> str:
    """Build the manifest object URI for one dataset and lineage UUID."""
    return join_uri(
        base_uri,
        MANIFEST_DIRECTORY,
        sanitize_path_segment(dataset),
        manifest_file_name(lineage_uuid),
    )


def is_s3_uri(uri: Any) -> bool:
    """Return True when the URI targets an S3-compatible filesystem."""
    if not isinstance(uri, str) or not uri.strip():
        return False
    return urlparse(uri.strip()).scheme.lower() in _S3_SCHEMES


def split_s3_uri(uri: str) -> Tuple[str, str]:
    """Split ``s3://bucket/key`` into ``(bucket, key)``."""
    if not is_s3_uri(uri):
        raise ValidationError(f"Not an S3 URI: {uri!r}")
    parsed = urlparse(uri.strip())
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    if not bucket:
        raise ValidationError(f"S3 URI has no bucket: {uri!r}")
    return bucket, key


def sha256_of_file(local_path: str) -> str:
    """Return the hex SHA-256 digest of a local file."""
    digest = hashlib.sha256()
    try:
        with open(local_path, "rb") as handle:
            while True:
                chunk = handle.read(_SHA256_CHUNK_SIZE)
                if not chunk:
                    break
                digest.update(chunk)
    except OSError as exc:
        raise ValidationError(f"Unable to read file '{local_path}': {exc}") from exc
    return digest.hexdigest()
