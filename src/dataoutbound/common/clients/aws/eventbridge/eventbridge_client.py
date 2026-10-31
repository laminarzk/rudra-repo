"""EventBridge publisher emitting CloudEvents-shaped detail payloads."""

from __future__ import annotations

import uuid
from typing import Any, Dict, Iterable, List, Optional

from botocore.exceptions import BotoCoreError
from botocore.exceptions import ClientError as BotoClientError

from dataoutbound.common.clients.aws.session import get_client
from dataoutbound.common.logging.logger import get_logger
from dataoutbound.config.config_loader import ConfigLoader
from dataoutbound.util.constants import (
    CLOUDEVENT_CONTENT_TYPE,
    CLOUDEVENT_SPEC_VERSION,
    CLOUDEVENT_SOURCE_PREFIX,
    CLOUDEVENT_TYPE_PREFIX,
)
from dataoutbound.util.exceptions import ClientError
from dataoutbound.util.utils import chunked, safe_json_dumps, utc_now_millis

logger = get_logger(__name__)

CLOUDEVENT_MAX_BATCH = 10
DEFAULT_EVENT_BUS_NAME = "default"


class EventBridgeClient:
    """Publish CloudEvents-shaped events onto the solution event bus."""

    def __init__(
        self,
        config: Optional[ConfigLoader] = None,
        *,
        client: Any = None,
        default_event_bus_name: str = DEFAULT_EVENT_BUS_NAME,
    ) -> None:
        self._config = config or ConfigLoader.load_config("common")
        self._client = client
        self._default_bus_name = default_event_bus_name

    @property
    def client(self) -> Any:
        if self._client is None:
            self._client = get_client("events", config=self._config)
        return self._client

    # -- CloudEvent construction ------------------------------------------
    @staticmethod
    def build_cloud_event(
        *,
        source: str,
        detail_type: str,
        data: Dict[str, Any],
        subject: Optional[str] = None,
        event_id: Optional[str] = None,
        time: Optional[str] = None,
        event_source_prefix: str = CLOUDEVENT_SOURCE_PREFIX,
        event_type_prefix: str = CLOUDEVENT_TYPE_PREFIX,
    ) -> Dict[str, Any]:
        """Return a CloudEvents 1.0 envelope for the supplied ``data``."""
        envelope: Dict[str, Any] = {
            "specversion": CLOUDEVENT_SPEC_VERSION,
            "id": event_id or str(uuid.uuid4()),
            "source": f"{event_source_prefix}:{source}"
            if source
            else event_source_prefix,
            "type": detail_type
            if "." in detail_type
            else f"{event_type_prefix}.{detail_type.lower()}",
            "time": time or utc_now_millis(),
            "datacontenttype": CLOUDEVENT_CONTENT_TYPE,
            "data": data,
        }
        if subject:
            envelope["subject"] = subject
        return envelope

    # -- publishing --------------------------------------------------------
    def put_events(
        self,
        entries: Iterable[Dict[str, Any]],
        *,
        event_bus_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Send raw ``PutEvents`` entries and fail when any entry is rejected."""
        payload = list(entries)
        if not payload:
            return {"FailedEntryCount": 0, "Entries": []}

        bus_name = event_bus_name or self._default_bus_name
        normalised: List[Dict[str, Any]] = []
        for entry in payload:
            item = dict(entry)
            item.setdefault("EventBusName", bus_name)
            if isinstance(item.get("Detail"), (dict, list)):
                item["Detail"] = safe_json_dumps(item["Detail"])
            normalised.append(item)

        try:
            response = self.client.put_events(Entries=normalised)
        except BotoClientError as exc:
            raise ClientError(f"EventBridge put_events failed: {exc}") from exc
        except BotoCoreError as exc:
            raise ClientError(f"EventBridge put_events failed: {exc}") from exc

        failed_count = int(response.get("FailedEntryCount", 0))
        if failed_count:
            logger.error(
                "EventBridge rejected entries",
                extra={
                    "failed_count": failed_count,
                    "event_bus_name": bus_name,
                    "entries": response.get("Entries", []),
                },
            )
            raise ClientError(
                f"EventBridge rejected {failed_count} of {len(normalised)} entries"
            )
        return response

    def put_cloud_event(
        self,
        event: Dict[str, Any],
        *,
        event_bus_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Publish a single CloudEvents envelope."""
        results = self.put_cloud_events([event], event_bus_name=event_bus_name)
        return results[0] if results else {"FailedEntryCount": 0, "Entries": []}

    def put_cloud_events(
        self,
        events: Iterable[Dict[str, Any]],
        *,
        event_bus_name: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Publish many CloudEvents envelopes in batches of ten."""
        entries: List[Dict[str, Any]] = []
        for event in events:
            entries.append(
                {
                    "Source": str(event.get("source", CLOUDEVENT_SOURCE_PREFIX)),
                    "DetailType": str(
                        event.get("type", "com.laminar.dataoutbound.event")
                    ),
                    "Detail": safe_json_dumps(event),
                }
            )

        results: List[Dict[str, Any]] = []
        for batch in chunked(entries, CLOUDEVENT_MAX_BATCH):
            results.append(self.put_events(batch, event_bus_name=event_bus_name))
        return results
