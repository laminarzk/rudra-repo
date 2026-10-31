"""CloudEvents-shaped audit publisher that never breaks the caller's flow."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional

from dataoutbound.common.logging.logger import get_logger
from dataoutbound.config.config_loader import ConfigLoader
from dataoutbound.util.constants import (
    ALL_AUDIT_EVENT_TYPES,
    CLOUDEVENT_CONTENT_TYPE,
    CLOUDEVENT_SOURCE_PREFIX,
    CLOUDEVENT_SPEC_VERSION,
    CLOUDEVENT_TYPE_PREFIX,
)
from dataoutbound.util.utils import safe_json_dumps, utc_now_millis

logger = get_logger(__name__)

DEFAULT_ACTOR_ID = "system:dataoutbound"


@dataclass
class AuditRecord:
    """Normalised audit record emitted for every meaningful pipeline action."""

    event_type: str
    execution_id: str
    actor_id: str = DEFAULT_ACTOR_ID
    timestamp_utc: str = field(default_factory=utc_now_millis)
    payload: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Return the flat business audit event representation."""
        document: Dict[str, Any] = {
            "actor_id": self.actor_id,
            "timestamp_utc": self.timestamp_utc,
            "event_type": self.event_type,
            "execution_id": self.execution_id,
        }
        if self.payload:
            document["payload"] = self.payload
        return document

    def to_cloud_event(self) -> Dict[str, Any]:
        """Return the CloudEvents 1.0 envelope for this record."""
        return {
            "specversion": CLOUDEVENT_SPEC_VERSION,
            "id": str(uuid.uuid4()),
            "source": f"{CLOUDEVENT_SOURCE_PREFIX}:{self.actor_id}",
            "type": f"{CLOUDEVENT_TYPE_PREFIX}.{self.event_type.lower()}",
            "time": self.timestamp_utc,
            "datacontenttype": CLOUDEVENT_CONTENT_TYPE,
            "subject": self.execution_id,
            "data": self.to_dict(),
        }


class AuditPublisher:
    """Write audit records to the configured sink.

    Publication is best-effort by design: a failure to write an audit record
    must never abort an export, so every error path is logged and swallowed.
    """

    def __init__(
        self,
        config: Optional[ConfigLoader] = None,
        *,
        sink: Any = None,
        actor_id: str = DEFAULT_ACTOR_ID,
    ) -> None:
        self._config = config or ConfigLoader.load_config("common")
        self._sink = sink
        self._actor_id = actor_id

    # -- sink wiring -------------------------------------------------------
    def _resolve_sink(self) -> Any:
        if self._sink is not None:
            return self._sink
        from dataoutbound.common.clients.aws.s3.s3_client import S3Client

        self._sink = S3Client(self._config)
        return self._sink

    def _resolve_bucket(self) -> str:
        return self._config.get_str("audit.bucket_name", "audit_bucket")

    def _resolve_prefix(self) -> str:
        return self._config.get_str("audit.prefix", "audit").strip("/")

    def _build_key(self, record: AuditRecord) -> str:
        prefix = self._resolve_prefix()
        timestamp = record.timestamp_utc.replace(":", "").replace("-", "")
        date_segment = record.timestamp_utc[:10]
        return (
            f"{prefix}/dt={date_segment}/{record.event_type.lower()}/"
            f"{timestamp}_{record.execution_id}.json"
        )

    # -- publishing --------------------------------------------------------
    def publish(
        self,
        event_type: str,
        payload: Optional[Mapping[str, Any]] = None,
    ) -> Optional[str]:
        """Publish an audit event and return the object key when it was written.

        The method never raises: validation problems and transport failures are
        logged at error level so that callers can treat auditing as fire and
        forget while still surfacing problems to operators.
        """
        try:
            normalised_payload = self._normalise_payload(payload)
            execution_id = str(
                normalised_payload.get("execution_id")
                or normalised_payload.get("lineage_uuid")
                or uuid.uuid4()
            )
            actor_id = str(normalised_payload.get("actor_id") or self._actor_id)

            record = AuditRecord(
                event_type=event_type,
                execution_id=execution_id,
                actor_id=actor_id,
                payload=normalised_payload,
            )

            if event_type not in ALL_AUDIT_EVENT_TYPES:
                logger.warning(
                    "Publishing audit event outside the declared schema",
                    extra={"event_type": event_type, "execution_id": execution_id},
                )

            sink = self._resolve_sink()
            key = self._build_key(record)
            sink.upload_json(
                self._resolve_bucket(),
                key,
                record.to_cloud_event(),
            )
            logger.info(
                "Audit event published",
                extra={
                    "event_type": event_type,
                    "execution_id": execution_id,
                    "audit_key": key,
                },
            )
            return key
        except Exception as exc:
            logger.error(
                "Failed to publish audit event",
                extra={"event_type": event_type, "error": str(exc)},
                exc_info=True,
            )
            return None

    @staticmethod
    def _normalise_payload(payload: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
        if not payload:
            return {}
        if not isinstance(payload, Mapping):
            return {"value": safe_json_dumps(payload)}
        return {str(key): value for key, value in payload.items()}

    @staticmethod
    def build_record(
        event_type: str,
        execution_id: str,
        *,
        actor_id: str = DEFAULT_ACTOR_ID,
        payload: Optional[Mapping[str, Any]] = None,
    ) -> AuditRecord:
        """Build an audit record without publishing it (used by tests)."""
        return AuditRecord(
            event_type=event_type,
            execution_id=execution_id,
            actor_id=actor_id,
            payload=dict(payload or {}),
        )
