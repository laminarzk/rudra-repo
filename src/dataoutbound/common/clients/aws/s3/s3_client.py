"""Typed S3 client with server-side encryption and mapped errors."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from botocore.exceptions import BotoCoreError
from botocore.exceptions import ClientError as BotoClientError

from dataoutbound.common.clients.aws.session import get_client
from dataoutbound.common.logging.logger import get_logger
from dataoutbound.config.config_loader import ConfigLoader
from dataoutbound.util.exceptions import ClientError, NotFoundError

logger = get_logger(__name__)

DEFAULT_SERVER_SIDE_ENCRYPTION = "AES256"
DEFAULT_PRESIGN_EXPIRY_SECONDS = 3600
_MISSING_CODES = frozenset({"NoSuchKey", "NoSuchBucket", "404", "NotFound"})


def _is_missing(exc: BotoClientError) -> bool:
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return False
    code = str(response.get("Error", {}).get("Code", ""))
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return code in _MISSING_CODES or status == 404


class S3Client:
    """Upload, download, list and delete objects in solution buckets."""

    def __init__(
        self,
        config: Optional[ConfigLoader] = None,
        *,
        client: Any = None,
        server_side_encryption: str = DEFAULT_SERVER_SIDE_ENCRYPTION,
    ) -> None:
        self._config = config or ConfigLoader.load_config("common")
        self._client = client
        self._sse = server_side_encryption

    @property
    def client(self) -> Any:
        if self._client is None:
            self._client = get_client("s3", config=self._config)
        return self._client

    # -- helpers -----------------------------------------------------------
    def _extra_args(
        self,
        *,
        sse: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        content_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        args: Dict[str, Any] = {"ServerSideEncryption": sse or self._sse}
        if metadata:
            args["Metadata"] = {str(k): str(v) for k, v in metadata.items()}
        if content_type:
            args["ContentType"] = content_type
        return args

    # -- uploads -----------------------------------------------------------
    def upload_file(
        self,
        bucket: str,
        key: str,
        local_path: str,
        *,
        sse: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        content_type: Optional[str] = None,
    ) -> str:
        """Upload a local file and return its ETag."""
        path = Path(local_path)
        if not path.is_file():
            raise NotFoundError(f"Local file not found: {local_path}")

        extra_args = self._extra_args(
            sse=sse,
            metadata=metadata,
            content_type=content_type,
        )
        try:
            self.client.upload_file(str(path), bucket, key, ExtraArgs=extra_args)
            head = self.client.head_object(Bucket=bucket, Key=key)
        except BotoClientError as exc:
            raise ClientError(
                f"S3 upload failed for s3://{bucket}/{key}: {exc}"
            ) from exc
        except BotoCoreError as exc:
            raise ClientError(
                f"S3 upload failed for s3://{bucket}/{key}: {exc}"
            ) from exc

        etag = str(head.get("ETag", "")).strip('"')
        logger.debug("S3 upload complete", extra={"bucket": bucket, "key": key})
        return etag

    def upload_bytes(
        self,
        bucket: str,
        key: str,
        data: bytes,
        *,
        sse: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        content_type: Optional[str] = None,
    ) -> str:
        """Upload raw bytes and return the resulting ETag."""
        try:
            response = self.client.put_object(
                Bucket=bucket,
                Key=key,
                Body=data,
                **self._extra_args(
                    sse=sse,
                    metadata=metadata,
                    content_type=content_type,
                ),
            )
        except BotoClientError as exc:
            raise ClientError(
                f"S3 put_object failed for s3://{bucket}/{key}: {exc}"
            ) from exc
        except BotoCoreError as exc:
            raise ClientError(
                f"S3 put_object failed for s3://{bucket}/{key}: {exc}"
            ) from exc
        return str(response.get("ETag", "")).strip('"')

    def upload_json(
        self,
        bucket: str,
        key: str,
        data: Any,
        *,
        sse: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Serialise ``data`` to JSON and upload it as an object."""
        payload = json.dumps(data, default=str).encode("utf-8")
        return self.upload_bytes(
            bucket,
            key,
            payload,
            sse=sse,
            metadata=metadata,
            content_type="application/json",
        )

    # -- downloads ---------------------------------------------------------
    def download_file(self, bucket: str, key: str, local_path: str) -> None:
        """Download an object to ``local_path``, creating parent directories."""
        path = Path(local_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.client.download_file(bucket, key, str(path))
        except BotoClientError as exc:
            if _is_missing(exc):
                raise NotFoundError(
                    f"S3 object not found: s3://{bucket}/{key}"
                ) from exc
            raise ClientError(
                f"S3 download failed for s3://{bucket}/{key}: {exc}"
            ) from exc
        except BotoCoreError as exc:
            raise ClientError(
                f"S3 download failed for s3://{bucket}/{key}: {exc}"
            ) from exc
        logger.debug("S3 download complete", extra={"bucket": bucket, "key": key})

    def download_bytes(self, bucket: str, key: str) -> bytes:
        """Return the body of an object as bytes."""
        try:
            response = self.client.get_object(Bucket=bucket, Key=key)
            body = response["Body"].read()
        except BotoClientError as exc:
            if _is_missing(exc):
                raise NotFoundError(
                    f"S3 object not found: s3://{bucket}/{key}"
                ) from exc
            raise ClientError(
                f"S3 get_object failed for s3://{bucket}/{key}: {exc}"
            ) from exc
        except BotoCoreError as exc:
            raise ClientError(
                f"S3 get_object failed for s3://{bucket}/{key}: {exc}"
            ) from exc
        if isinstance(body, bytes):
            return body
        return bytes(body)

    def download_json(self, bucket: str, key: str, *, default: Any = None) -> Any:
        """Download an object and decode it as JSON, never raising on decode."""
        raw = self.download_bytes(bucket, key)
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            logger.warning(
                "S3 object is not valid JSON",
                extra={"bucket": bucket, "key": key},
            )
            return default

    # -- listing / lifecycle ----------------------------------------------
    def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        *,
        max_keys: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Return object metadata for every key below ``prefix``."""
        collected: List[Dict[str, Any]] = []
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            pagination: Dict[str, Any] = {}
            if max_keys:
                pagination["PageSize"] = int(max_keys)
            for page in paginator.paginate(
                Bucket=bucket,
                Prefix=prefix,
                PaginationConfig=pagination,
            ):
                for entry in page.get("Contents", []):
                    collected.append(
                        {
                            "key": str(entry.get("Key")),
                            "size": int(entry.get("Size", 0)),
                            "etag": str(entry.get("ETag", "")).strip('"'),
                            "last_modified": entry.get("LastModified"),
                        }
                    )
        except BotoClientError as exc:
            raise ClientError(
                f"S3 list_objects failed for s3://{bucket}/{prefix}: {exc}"
            ) from exc
        except BotoCoreError as exc:
            raise ClientError(
                f"S3 list_objects failed for s3://{bucket}/{prefix}: {exc}"
            ) from exc
        return collected

    def object_exists(self, bucket: str, key: str) -> bool:
        """Return ``True`` when the object exists and is readable."""
        try:
            self.client.head_object(Bucket=bucket, Key=key)
            return True
        except BotoClientError as exc:
            if _is_missing(exc):
                return False
            raise ClientError(
                f"S3 head_object failed for s3://{bucket}/{key}: {exc}"
            ) from exc
        except BotoCoreError as exc:
            raise ClientError(
                f"S3 head_object failed for s3://{bucket}/{key}: {exc}"
            ) from exc

    def delete_object(self, bucket: str, key: str) -> None:
        """Delete a single object; missing objects are treated as success."""
        try:
            self.client.delete_object(Bucket=bucket, Key=key)
        except BotoClientError as exc:
            raise ClientError(
                f"S3 delete_object failed for s3://{bucket}/{key}: {exc}"
            ) from exc
        except BotoCoreError as exc:
            raise ClientError(
                f"S3 delete_object failed for s3://{bucket}/{key}: {exc}"
            ) from exc

    def generate_presigned_url(
        self,
        bucket: str,
        key: str,
        *,
        expires_in: int = DEFAULT_PRESIGN_EXPIRY_SECONDS,
        method: str = "get_object",
    ) -> str:
        """Return a time-limited presigned URL for the given object."""
        try:
            return str(
                self.client.generate_presigned_url(
                    ClientMethod=method,
                    Params={"Bucket": bucket, "Key": key},
                    ExpiresIn=int(expires_in),
                )
            )
        except (BotoClientError, BotoCoreError) as exc:
            raise ClientError(f"Failed to presign s3://{bucket}/{key}: {exc}") from exc
