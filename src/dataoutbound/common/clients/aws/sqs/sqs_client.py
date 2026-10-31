"""Typed SQS client for send / receive / delete and batch operations."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional

from botocore.exceptions import BotoCoreError
from botocore.exceptions import ClientError as BotoClientError

from dataoutbound.common.clients.aws.session import get_client
from dataoutbound.common.logging.logger import get_logger
from dataoutbound.config.config_loader import ConfigLoader
from dataoutbound.util.exceptions import ClientError
from dataoutbound.util.utils import chunked, safe_json_dumps

logger = get_logger(__name__)

MAX_BATCH_SIZE = 10
DEFAULT_WAIT_SECONDS = 0
DEFAULT_VISIBILITY_TIMEOUT = 30


@dataclass
class SqsMessage:
    """A single dequeued message with its receipt handle for acknowledgement."""

    message_id: str
    body: Any
    receipt_handle: Optional[str] = None
    attributes: Dict[str, str] = field(default_factory=dict)
    message_attributes: Dict[str, Any] = field(default_factory=dict)


def _serialize_body(body: Any) -> str:
    if isinstance(body, str):
        return body
    return safe_json_dumps(body)


class SqsClient:
    """Send, receive and acknowledge messages on solution queues."""

    def __init__(
        self,
        config: Optional[ConfigLoader] = None,
        *,
        client: Any = None,
    ) -> None:
        self._config = config or ConfigLoader.load_config("common")
        self._client = client

    @property
    def client(self) -> Any:
        if self._client is None:
            self._client = get_client("sqs", config=self._config)
        return self._client

    # -- sends -------------------------------------------------------------
    def send_message(
        self,
        queue_url: str,
        body: Any,
        *,
        delay_seconds: int = 0,
        message_attributes: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Send a single message and return its ``MessageId``."""
        try:
            response = self.client.send_message(
                QueueUrl=queue_url,
                MessageBody=_serialize_body(body),
                DelaySeconds=int(delay_seconds),
                MessageAttributes=message_attributes or {},
            )
        except BotoClientError as exc:
            raise ClientError(f"SQS send_message failed: {exc}") from exc
        except BotoCoreError as exc:
            raise ClientError(f"SQS send_message failed: {exc}") from exc
        return str(response.get("MessageId", ""))

    def send_batch(self, queue_url: str, bodies: Iterable[Any]) -> List[str]:
        """Send up to many messages in batches of ten, returning their ids."""
        payloads = [_serialize_body(body) for body in bodies]
        if not payloads:
            return []

        message_ids: List[str] = []
        for batch in chunked(payloads, MAX_BATCH_SIZE):
            entries = [
                {"Id": str(index), "MessageBody": payload}
                for index, payload in enumerate(batch)
            ]
            try:
                response = self.client.send_message_batch(
                    QueueUrl=queue_url,
                    Entries=entries,
                )
            except BotoClientError as exc:
                raise ClientError(f"SQS send_message_batch failed: {exc}") from exc
            except BotoCoreError as exc:
                raise ClientError(f"SQS send_message_batch failed: {exc}") from exc

            for successful in response.get("Successful", []):
                message_ids.append(str(successful.get("MessageId", "")))

            failed = response.get("Failed", [])
            if failed:
                logger.error(
                    "SQS batch partially failed",
                    extra={"failed_count": len(failed), "queue_url": queue_url},
                )
                raise ClientError(
                    f"SQS send_message_batch failed for {len(failed)} entries"
                )
        return message_ids

    # -- receives ----------------------------------------------------------
    def receive_messages(
        self,
        queue_url: str,
        *,
        max_messages: int = MAX_BATCH_SIZE,
        wait_seconds: int = DEFAULT_WAIT_SECONDS,
        visibility_timeout: Optional[int] = None,
        attribute_names: Optional[Iterable[str]] = None,
    ) -> List[SqsMessage]:
        """Long-poll for up to ``max_messages`` messages from the queue."""
        kwargs: Dict[str, Any] = {
            "QueueUrl": queue_url,
            "MaxNumberOfMessages": min(int(max_messages), MAX_BATCH_SIZE),
            "WaitTimeSeconds": int(wait_seconds),
            "AttributeNames": list(attribute_names or ["All"]),
            "MessageAttributeNames": ["All"],
        }
        if visibility_timeout is not None:
            kwargs["VisibilityTimeout"] = int(visibility_timeout)

        try:
            response = self.client.receive_message(**kwargs)
        except BotoClientError as exc:
            raise ClientError(f"SQS receive_message failed: {exc}") from exc
        except BotoCoreError as exc:
            raise ClientError(f"SQS receive_message failed: {exc}") from exc

        messages: List[SqsMessage] = []
        for raw in response.get("Messages", []):
            body = raw.get("Body", "")
            try:
                parsed_body: Any = json.loads(body)
            except (TypeError, ValueError):
                parsed_body = body
            messages.append(
                SqsMessage(
                    message_id=str(raw.get("MessageId", "")),
                    body=parsed_body,
                    receipt_handle=raw.get("ReceiptHandle"),
                    attributes=dict(raw.get("Attributes", {})),
                    message_attributes=dict(raw.get("MessageAttributes", {})),
                )
            )
        return messages

    # -- acknowledgements --------------------------------------------------
    def delete_message(self, queue_url: str, receipt_handle: str) -> None:
        """Delete a single message by its receipt handle."""
        try:
            self.client.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle,
            )
        except BotoClientError as exc:
            raise ClientError(f"SQS delete_message failed: {exc}") from exc
        except BotoCoreError as exc:
            raise ClientError(f"SQS delete_message failed: {exc}") from exc

    def delete_batch(self, queue_url: str, receipt_handles: Iterable[str]) -> None:
        """Delete many messages in batches of ten."""
        handles = [handle for handle in receipt_handles if handle]
        if not handles:
            return

        for batch in chunked(handles, MAX_BATCH_SIZE):
            entries = [
                {"Id": str(index), "ReceiptHandle": handle}
                for index, handle in enumerate(batch)
            ]
            try:
                response = self.client.delete_message_batch(
                    QueueUrl=queue_url,
                    Entries=entries,
                )
            except BotoClientError as exc:
                raise ClientError(f"SQS delete_message_batch failed: {exc}") from exc
            except BotoCoreError as exc:
                raise ClientError(f"SQS delete_message_batch failed: {exc}") from exc

            failed = response.get("Failed", [])
            if failed:
                logger.error(
                    "SQS delete batch partially failed",
                    extra={"failed_count": len(failed), "queue_url": queue_url},
                )
                raise ClientError(
                    f"SQS delete_message_batch failed for {len(failed)} entries"
                )
