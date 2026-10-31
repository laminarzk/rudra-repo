"""Egress Lambda: S3 ObjectCreated entrypoint that triggers the Transform & Encrypt DAG.

The handler parses the S3 event notification, recovers the lineage UUID from the
staged object key, and triggers exactly one Transform & Encrypt DAG run per staged
object. Transfer lifecycle audit events are owned by the DAG, so this function only
publishes a TRANSFER_FAILED event when it cannot hand the object to the DAG.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional
from urllib.parse import unquote_plus

from dataoutbound.common.clients.aws.mwaa.mwaa_client import MwaaClient
from dataoutbound.common.logging.logger import get_logger
from dataoutbound.common.telemetry.auditor import AuditPublisher
from dataoutbound.config.config_loader import ConfigLoader
from dataoutbound.util.exceptions import ClientError, ValidationError
from dataoutbound.util.idempotency import IdempotencyKey

logger = get_logger(__name__)

TRANSFORM_ENCRYPT_DAG_ID = "transform_encrypt_dag"
ACTOR_ID = "system:egress_lambda"
LINEAGE_PREFIX = "lineage_uuid="
UUID_VERSION = 4
DEFAULT_MAX_TRIGGER_ATTEMPTS = 3
DEFAULT_BACKOFF_SECONDS = 2.0

_CONFIG: Dict[str, Any] = ConfigLoader.load_config("lambdas")
_RETRY_CONFIG: Mapping[str, Any] = _CONFIG.get("egress_lambda", {}).get("retry", {})
_MAX_TRIGGER_ATTEMPTS: int = int(
    _RETRY_CONFIG.get("max_attempts", DEFAULT_MAX_TRIGGER_ATTEMPTS)
)
_BACKOFF_SECONDS: float = float(
    _RETRY_CONFIG.get("backoff_seconds", DEFAULT_BACKOFF_SECONDS)
)

# Clients and the audit publisher are constructed once per execution environment.
_MWAA_CLIENT = MwaaClient(_CONFIG)
_AUDITOR = AuditPublisher()


@dataclass(frozen=True)
class StagedObjectRef:
    """Reference to a staged export object that must be transformed and delivered.

    Attributes:
        bucket: Name of the staging bucket that emitted the event.
        key: Decoded object key within the bucket.
        lineage_uuid: RFC 4122 v4 lineage UUID embedded in the object key.
    """

    bucket: str
    key: str
    lineage_uuid: str


def _utc_now_ms() -> str:
    """Return the current UTC time in ISO-8601 format with millisecond precision.

    Returns:
        Timestamp such as ``2025-01-31T02:15:00.000Z``.
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _validate_lineage_uuid(candidate: str) -> str:
    """Validate that a candidate string is an RFC 4122 v4 UUID.

    Args:
        candidate: Raw UUID text taken from the object key.

    Returns:
        Canonical lowercase string form of the UUID.

    Raises:
        ValidationError: If the value is not a valid version 4 UUID.
    """
    try:
        parsed = uuid.UUID(candidate)
    except ValueError as exc:
        raise ValidationError(f"Invalid lineage UUID '{candidate}'") from exc
    if parsed.version != UUID_VERSION:
        raise ValidationError(f"Lineage UUID '{candidate}' is not version 4")
    return str(parsed)


def _extract_lineage_uuid(object_key: str) -> str:
    """Recover the lineage UUID embedded in a staged object key.

    Args:
        object_key: Decoded S3 object key, e.g. ``exports/lineage_uuid=<uuid>/part-0.json``.

    Returns:
        Canonical lineage UUID string.

    Raises:
        ValidationError: If no valid lineage UUID segment is present.
    """
    for part in object_key.split("/"):
        if part.startswith(LINEAGE_PREFIX):
            return _validate_lineage_uuid(part[len(LINEAGE_PREFIX) :])
    raise ValidationError(f"Object key '{object_key}' carries no lineage UUID segment")


def _required_string(container: Mapping[str, Any], field: str, index: int) -> str:
    """Return a required non-empty string field from a mapping.

    Args:
        container: Mapping expected to hold the field.
        field: Field name to read.
        index: Record index used in error messages.

    Returns:
        The string value.

    Raises:
        ValidationError: If the field is missing or not a non-empty string.
    """
    value = container.get(field)
    if not isinstance(value, str) or not value:
        raise ValidationError(
            f"Record {index} is missing required string field '{field}'"
        )
    return value


def _parse_s3_event(event: Mapping[str, Any]) -> List[StagedObjectRef]:
    """Parse and validate an S3 ObjectCreated notification into object references.

    Args:
        event: Raw Lambda event payload from S3.

    Returns:
        One StagedObjectRef per record in the event.

    Raises:
        ValidationError: If the event does not match the designed schema.
    """
    records = event.get("Records")
    if not isinstance(records, list) or not records:
        raise ValidationError("S3 event must contain a non-empty 'Records' list")

    refs: List[StagedObjectRef] = []
    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            raise ValidationError(f"Record {index} is not an object")
        s3_info = record.get("s3")
        if not isinstance(s3_info, Mapping):
            raise ValidationError(f"Record {index} has no 's3' section")
        bucket_info = s3_info.get("bucket")
        object_info = s3_info.get("object")
        if not isinstance(bucket_info, Mapping) or not isinstance(object_info, Mapping):
            raise ValidationError(f"Record {index} has no 'bucket' or 'object' section")

        bucket = _required_string(bucket_info, "name", index)
        raw_key = _required_string(object_info, "key", index)
        key = unquote_plus(raw_key)
        refs.append(
            StagedObjectRef(
                bucket=bucket,
                key=key,
                lineage_uuid=_extract_lineage_uuid(key),
            )
        )
    return refs


def _audit_payload(
    lineage_uuid: str,
    event_type: str,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a business audit payload that conforms to the audit schema.

    Args:
        lineage_uuid: Execution identifier for the audit record.
        event_type: One of the enumerated audit event types.
        extra: Optional additional payload fields.

    Returns:
        Dictionary with actor_id, timestamp_utc, event_type and execution_id.
    """
    payload: Dict[str, Any] = {
        "actor_id": ACTOR_ID,
        "timestamp_utc": _utc_now_ms(),
        "event_type": event_type,
        "execution_id": lineage_uuid,
    }
    if extra:
        payload.update(extra)
    return payload


def _trigger_with_retry(conf: Dict[str, Any]) -> None:
    """Trigger the Transform & Encrypt DAG, retrying transient client failures.

    Args:
        conf: DAG run configuration payload.

    Raises:
        ClientError: If every attempt fails.
    """
    last_error: Optional[ClientError] = None
    for attempt in range(1, _MAX_TRIGGER_ATTEMPTS + 1):
        try:
            _MWAA_CLIENT.trigger_dag_run(dag_id=TRANSFORM_ENCRYPT_DAG_ID, conf=conf)
            return
        except ClientError as exc:
            last_error = exc
            logger.warning(
                "DAG trigger attempt failed",
                extra={
                    "attempt": attempt,
                    "max_attempts": _MAX_TRIGGER_ATTEMPTS,
                    "lineage_uuid": conf.get("lineage_uuid"),
                },
            )
            if attempt < _MAX_TRIGGER_ATTEMPTS:
                time.sleep(_BACKOFF_SECONDS * attempt)
    raise ClientError(
        f"Failed to trigger {TRANSFORM_ENCRYPT_DAG_ID} after {_MAX_TRIGGER_ATTEMPTS} attempts"
    ) from last_error


def _trigger_transform_dag(ref: StagedObjectRef) -> None:
    """Trigger exactly one Transform & Encrypt DAG run for a staged object.

    On exhausted retries a TRANSFER_FAILED audit event is published and the
    error is re-raised to the caller.

    Args:
        ref: Staged object reference with its lineage UUID.

    Raises:
        ClientError: If the DAG run could not be triggered.
    """
    idempotency_key = IdempotencyKey.build(ref.lineage_uuid, ref.bucket, ref.key)
    conf: Dict[str, Any] = {
        "lineage_uuid": ref.lineage_uuid,
        "source_s3_reference": {"bucket": ref.bucket, "key": ref.key},
        "idempotency_key": idempotency_key,
    }
    logger.info(
        "Triggering transform & encrypt DAG",
        extra={"lineage_uuid": ref.lineage_uuid, "bucket": ref.bucket, "key": ref.key},
    )
    try:
        _trigger_with_retry(conf)
    except ClientError as exc:
        logger.error(
            "Transform & encrypt DAG trigger failed",
            extra={"lineage_uuid": ref.lineage_uuid, "error": str(exc)},
            exc_info=True,
        )
        _AUDITOR.publish(
            "TRANSFER_FAILED",
            _audit_payload(
                ref.lineage_uuid,
                "TRANSFER_FAILED",
                {"source_key": ref.key, "error": str(exc)},
            ),
        )
        raise
    logger.info(
        "Transform & encrypt DAG triggered",
        extra={"lineage_uuid": ref.lineage_uuid},
    )


def _response(status_code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    """Build an API-Gateway-compatible response envelope.

    Args:
        status_code: HTTP-style status code.
        body: JSON-serialisable response body.

    Returns:
        Dictionary with statusCode, headers and a JSON string body.
    """
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """S3 ObjectCreated entrypoint that triggers one Transform & Encrypt DAG run per object.

    Args:
        event: S3 event notification payload.
        context: Lambda runtime context (may be None in local tests).

    Returns:
        200 when every object was handed to the DAG, 400 for a malformed event,
        500 when one or more DAG triggers failed or an unexpected error occurred.
    """
    request_id = getattr(context, "aws_request_id", None)
    logger.info("Egress Lambda invoked", extra={"request_id": request_id})

    try:
        refs = _parse_s3_event(event)
    except ValidationError as exc:
        logger.error(
            "Rejected malformed S3 event",
            extra={"request_id": request_id, "error": str(exc)},
        )
        return _response(
            400, {"error": {"code": "VALIDATION_ERROR", "message": str(exc)}}
        )

    failed_keys: List[str] = []
    try:
        for ref in refs:
            try:
                _trigger_transform_dag(ref)
            except ClientError:
                failed_keys.append(ref.key)
    except Exception as exc:  # Unexpected failure: map to 500 and log full trace.
        logger.error(
            "Unexpected error in Egress Lambda",
            extra={"request_id": request_id},
            exc_info=True,
        )
        return _response(
            500, {"error": {"code": "INTERNAL_ERROR", "message": str(exc)}}
        )

    if failed_keys:
        logger.error(
            "Egress Lambda completed with failures",
            extra={"request_id": request_id, "failed_count": len(failed_keys)},
        )
        return _response(
            500,
            {
                "error": {
                    "code": "DAG_TRIGGER_FAILED",
                    "message": "One or more DAG triggers failed",
                },
                "failed_keys": failed_keys,
            },
        )

    logger.info(
        "Egress Lambda completed",
        extra={"request_id": request_id, "triggered_count": len(refs)},
    )
    return _response(
        200,
        {"message": "Transform & Encrypt DAG triggered", "triggered_count": len(refs)},
    )
