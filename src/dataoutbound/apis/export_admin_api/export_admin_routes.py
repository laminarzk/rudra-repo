"""HTTP route handlers for the Export Admin API.

Paths, methods, status codes and auth dependencies mirror the High-Level Design
Implementation Code Skeleton (Section 10.3). Routes contain HTTP concerns only;
all business logic lives in ``export_admin_service.py``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status

from dataoutbound.apis.export_admin_api.dependencies import (
    get_audit_publisher,
    get_export_admin_service,
    require_export_admin,
)
from dataoutbound.apis.export_admin_api.export_admin_schemas import (
    ExportConfigCreateRequest,
    ExportConfigResponse,
    ExportConfigUpdateRequest,
    ManualStopResponse,
    ManualTriggerResponse,
)
from dataoutbound.apis.export_admin_api.export_admin_service import (
    ExportAdminService,
)
from dataoutbound.common.logging.logger import get_logger
from dataoutbound.common.telemetry.auditor import AuditPublisher
from dataoutbound.util.exceptions import (
    AuthError,
    ClientError,
    ConflictError,
    NotFoundError,
    ValidationError,
)

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/exports", tags=["Export Admin API"])

_ISO_MILLIS = "%Y-%m-%dT%H:%M:%S.%f"


def _utc_now_ms() -> str:
    """Return the current UTC time as ISO-8601 with millisecond precision."""
    return datetime.now(timezone.utc).strftime(_ISO_MILLIS)[:-3] + "Z"


def _actor_id(subject: Dict[str, Any]) -> str:
    """Extract the authenticated actor identifier from the token subject."""
    return str(subject.get("sub") or subject.get("username") or "unknown")


def _http_error(
    status_code: int,
    code: str,
    message: str,
    details: Optional[list] = None,
) -> HTTPException:
    """Build an ``HTTPException`` carrying the standard error envelope."""
    return HTTPException(
        status_code=status_code,
        detail={
            "error": {
                "code": code,
                "message": message,
                "details": details or [],
            }
        },
    )


def _publish_audit(
    auditor: AuditPublisher,
    event_type: str,
    actor_id: str,
    execution_id: Optional[str] = None,
    **extra: Any,
) -> None:
    """Emit a schema-conformant business audit event."""
    payload: Dict[str, Any] = {
        "actor_id": actor_id,
        "timestamp_utc": _utc_now_ms(),
        "event_type": event_type,
        "execution_id": execution_id or str(uuid4_safe()),
    }
    payload.update(extra)
    try:
        auditor.publish(event_type, payload)
    except Exception:
        # AuditPublisher never raises into the caller's flow.
        logger.error(
            "Failed to publish audit event",
            exc_info=True,
            extra={"event_type": event_type, "actor_id": actor_id},
        )


def uuid4_safe() -> str:
    """Return a fresh RFC 4122 v4 UUID string for admin audit correlation."""
    import uuid

    return str(uuid.uuid4())


@router.post(
    "",
    response_model=ExportConfigResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_export_config(
    payload: ExportConfigCreateRequest,
    service: ExportAdminService = Depends(get_export_admin_service),
    auditor: AuditPublisher = Depends(get_audit_publisher),
    subject: Dict[str, Any] = Depends(require_export_admin),
) -> ExportConfigResponse:
    """Create a new customer export configuration."""
    actor_id = _actor_id(subject)
    logger.info(
        "create_export_config entry",
        extra={"actor_id": actor_id, "connection_name": payload.connection_name},
    )
    try:
        result = service.create_configuration(payload, actor_id=actor_id)
    except ValidationError as exc:
        logger.warning(
            "create_export_config rejected",
            extra={"actor_id": actor_id, "error": str(exc)},
        )
        raise _http_error(
            status.HTTP_400_BAD_REQUEST, "VALIDATION_ERROR", str(exc)
        ) from exc
    except ConflictError as exc:
        raise _http_error(status.HTTP_409_CONFLICT, "CONFLICT", str(exc)) from exc
    except ClientError as exc:
        logger.error(
            "create_export_config upstream failure",
            exc_info=True,
            extra={"actor_id": actor_id},
        )
        raise _http_error(
            status.HTTP_502_BAD_GATEWAY, "UPSTREAM_ERROR", str(exc)
        ) from exc
    except AuthError as exc:
        raise _http_error(status.HTTP_403_FORBIDDEN, "FORBIDDEN", str(exc)) from exc

    _publish_audit(
        auditor,
        "ADMIN_CREATE",
        actor_id,
        config_id=result.id,
        connection_name=result.connection_name,
    )
    logger.info(
        "create_export_config success",
        extra={"actor_id": actor_id, "config_id": result.id},
    )
    return result


@router.get(
    "/{config_id}",
    response_model=ExportConfigResponse,
    status_code=status.HTTP_200_OK,
)
def get_export_config(
    config_id: int,
    service: ExportAdminService = Depends(get_export_admin_service),
    subject: Dict[str, Any] = Depends(require_export_admin),
) -> ExportConfigResponse:
    """Retrieve an export configuration by id with credentials masked."""
    actor_id = _actor_id(subject)
    logger.info(
        "get_export_config entry",
        extra={"actor_id": actor_id, "config_id": config_id},
    )
    try:
        result = service.get_configuration(config_id)
    except NotFoundError as exc:
        raise _http_error(status.HTTP_404_NOT_FOUND, "NOT_FOUND", str(exc)) from exc
    except ClientError as exc:
        logger.error(
            "get_export_config upstream failure",
            exc_info=True,
            extra={"actor_id": actor_id, "config_id": config_id},
        )
        raise _http_error(
            status.HTTP_502_BAD_GATEWAY, "UPSTREAM_ERROR", str(exc)
        ) from exc

    logger.info(
        "get_export_config success",
        extra={"actor_id": actor_id, "config_id": config_id},
    )
    return result


@router.put(
    "/{config_id}",
    response_model=ExportConfigResponse,
    status_code=status.HTTP_200_OK,
)
def update_export_config(
    config_id: int,
    payload: ExportConfigUpdateRequest,
    service: ExportAdminService = Depends(get_export_admin_service),
    auditor: AuditPublisher = Depends(get_audit_publisher),
    subject: Dict[str, Any] = Depends(require_export_admin),
) -> ExportConfigResponse:
    """Update an existing export configuration."""
    actor_id = _actor_id(subject)
    logger.info(
        "update_export_config entry",
        extra={"actor_id": actor_id, "config_id": config_id},
    )
    try:
        result = service.update_configuration(config_id, payload, actor_id=actor_id)
    except NotFoundError as exc:
        raise _http_error(status.HTTP_404_NOT_FOUND, "NOT_FOUND", str(exc)) from exc
    except ValidationError as exc:
        raise _http_error(
            status.HTTP_400_BAD_REQUEST, "VALIDATION_ERROR", str(exc)
        ) from exc
    except ConflictError as exc:
        raise _http_error(status.HTTP_409_CONFLICT, "CONFLICT", str(exc)) from exc
    except ClientError as exc:
        logger.error(
            "update_export_config upstream failure",
            exc_info=True,
            extra={"actor_id": actor_id, "config_id": config_id},
        )
        raise _http_error(
            status.HTTP_502_BAD_GATEWAY, "UPSTREAM_ERROR", str(exc)
        ) from exc

    _publish_audit(
        auditor,
        "ADMIN_UPDATE",
        actor_id,
        config_id=config_id,
    )
    logger.info(
        "update_export_config success",
        extra={"actor_id": actor_id, "config_id": config_id},
    )
    return result


@router.delete("/{config_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_export_config(
    config_id: int,
    service: ExportAdminService = Depends(get_export_admin_service),
    auditor: AuditPublisher = Depends(get_audit_publisher),
    subject: Dict[str, Any] = Depends(require_export_admin),
) -> None:
    """Delete an export configuration by id."""
    actor_id = _actor_id(subject)
    logger.info(
        "delete_export_config entry",
        extra={"actor_id": actor_id, "config_id": config_id},
    )
    try:
        deleted = service.delete_configuration(config_id)
    except ClientError as exc:
        logger.error(
            "delete_export_config upstream failure",
            exc_info=True,
            extra={"actor_id": actor_id, "config_id": config_id},
        )
        raise _http_error(
            status.HTTP_502_BAD_GATEWAY, "UPSTREAM_ERROR", str(exc)
        ) from exc

    if not deleted:
        raise _http_error(
            status.HTTP_404_NOT_FOUND,
            "NOT_FOUND",
            f"Export configuration {config_id} was not found",
        )

    _publish_audit(
        auditor,
        "ADMIN_DELETE",
        actor_id,
        config_id=config_id,
    )
    logger.info(
        "delete_export_config success",
        extra={"actor_id": actor_id, "config_id": config_id},
    )
    return None


@router.post(
    "/{config_id}/trigger",
    response_model=ManualTriggerResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
def manual_trigger_export(
    config_id: int,
    service: ExportAdminService = Depends(get_export_admin_service),
    auditor: AuditPublisher = Depends(get_audit_publisher),
    subject: Dict[str, Any] = Depends(require_export_admin),
) -> ManualTriggerResponse:
    """Manually trigger an Export Orchestrator run for a configuration."""
    actor_id = _actor_id(subject)
    logger.info(
        "manual_trigger_export entry",
        extra={"actor_id": actor_id, "config_id": config_id},
    )
    try:
        result = service.trigger_run(config_id, actor_id=actor_id)
    except NotFoundError as exc:
        raise _http_error(status.HTTP_404_NOT_FOUND, "NOT_FOUND", str(exc)) from exc
    except ValidationError as exc:
        raise _http_error(
            status.HTTP_400_BAD_REQUEST, "VALIDATION_ERROR", str(exc)
        ) from exc
    except ClientError as exc:
        logger.error(
            "manual_trigger_export upstream failure",
            exc_info=True,
            extra={"actor_id": actor_id, "config_id": config_id},
        )
        raise _http_error(
            status.HTTP_502_BAD_GATEWAY, "UPSTREAM_ERROR", str(exc)
        ) from exc

    _publish_audit(
        auditor,
        "ADMIN_MANUAL_TRIGGER",
        actor_id,
        execution_id=result.get("execution_id"),
        config_id=config_id,
        run_id=result.get("run_id"),
    )
    logger.info(
        "manual_trigger_export success",
        extra={
            "actor_id": actor_id,
            "config_id": config_id,
            "run_id": result.get("run_id"),
            "execution_id": result.get("execution_id"),
        },
    )
    return ManualTriggerResponse(
        message="Export run triggered successfully",
        config_id=config_id,
        run_id=str(result.get("run_id")),
        execution_id=str(result.get("execution_id")),
    )


@router.post(
    "/{config_id}/stop",
    response_model=ManualStopResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
def manual_stop_export(
    config_id: int,
    run_id: Optional[str] = Query(
        default=None,
        description="Optional explicit orchestrator run id to terminate",
    ),
    service: ExportAdminService = Depends(get_export_admin_service),
    subject: Dict[str, Any] = Depends(require_export_admin),
) -> ManualStopResponse:
    """Request termination of a running export execution."""
    actor_id = _actor_id(subject)
    logger.info(
        "manual_stop_export entry",
        extra={"actor_id": actor_id, "config_id": config_id, "run_id": run_id},
    )
    try:
        result = service.stop_run(config_id, actor_id=actor_id, run_id=run_id)
    except NotFoundError as exc:
        raise _http_error(status.HTTP_404_NOT_FOUND, "NOT_FOUND", str(exc)) from exc
    except ValidationError as exc:
        raise _http_error(
            status.HTTP_400_BAD_REQUEST, "VALIDATION_ERROR", str(exc)
        ) from exc
    except ClientError as exc:
        logger.error(
            "manual_stop_export upstream failure",
            exc_info=True,
            extra={"actor_id": actor_id, "config_id": config_id},
        )
        raise _http_error(
            status.HTTP_502_BAD_GATEWAY, "UPSTREAM_ERROR", str(exc)
        ) from exc

    logger.info(
        "manual_stop_export success",
        extra={
            "actor_id": actor_id,
            "config_id": config_id,
            "run_id": result.get("run_id"),
        },
    )
    return ManualStopResponse(
        message="Export run termination requested",
        config_id=config_id,
        run_id=result.get("run_id"),
    )
