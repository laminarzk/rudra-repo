"""Typed Google Cloud Storage client for customer export delivery."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from dataoutbound.common.logging.logger import get_logger
from dataoutbound.config.config_loader import ConfigLoader
from dataoutbound.util.exceptions import ClientError, ConfigError, NotFoundError

logger = get_logger(__name__)


def _import_storage_sdk() -> Any:
    """Import the GCS SDK lazily so non-GCP deployments need not install it."""
    try:
        from google.cloud import storage
    except ImportError as exc:
        raise ConfigError("google-cloud-storage is required to use GcsClient") from exc
    return storage


class GcsClient:
    """Upload, download and list objects in customer Cloud Storage buckets.

    Authentication uses Application Default Credentials, which covers both
    service-account keys mounted into the runtime and Workload Identity
    federation when running on GCP or in a federated environment.
    """

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
            storage = _import_storage_sdk()
            try:
                self._client = storage.Client()
            except Exception as exc:
                raise ClientError(f"Failed to create GCS client: {exc}") from exc
        return self._client

    def _bucket(self, bucket_name: str) -> Any:
        try:
            return self.client.bucket(bucket_name)
        except Exception as exc:
            raise ClientError(
                f"Failed to resolve GCS bucket '{bucket_name}': {exc}"
            ) from exc

    # -- uploads -----------------------------------------------------------
    def upload_bytes(
        self,
        bucket_name: str,
        destination_blob_name: str,
        data: bytes,
        *,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> str:
        """Upload raw bytes and return the object generation number."""
        try:
            blob = self._bucket(bucket_name).blob(destination_blob_name)
            if content_type:
                blob.content_type = content_type
            if metadata:
                blob.metadata = {str(k): str(v) for k, v in metadata.items()}
            blob.upload_from_string(data)
        except Exception as exc:
            raise ClientError(
                f"GCS upload failed for gs://{bucket_name}/{destination_blob_name}: {exc}"
            ) from exc
        logger.debug(
            "GCS upload complete",
            extra={"bucket": bucket_name, "object": destination_blob_name},
        )
        return str(getattr(blob, "generation", "") or "")

    def upload_file(
        self,
        bucket_name: str,
        destination_blob_name: str,
        source_file_name: str,
        *,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> str:
        """Upload a local file and return the object generation number."""
        path = Path(source_file_name)
        if not path.is_file():
            raise NotFoundError(f"Local file not found: {source_file_name}")
        with path.open("rb") as handle:
            return self.upload_bytes(
                bucket_name,
                destination_blob_name,
                handle.read(),
                content_type=content_type,
                metadata=metadata,
            )

    def upload_json(
        self,
        bucket_name: str,
        destination_blob_name: str,
        data: Any,
        *,
        metadata: Optional[Dict[str, str]] = None,
    ) -> str:
        """Serialise ``data`` as JSON and upload it as an object."""
        payload = json.dumps(data, default=str).encode("utf-8")
        return self.upload_bytes(
            bucket_name,
            destination_blob_name,
            payload,
            content_type="application/json",
            metadata=metadata,
        )

    # -- downloads ---------------------------------------------------------
    def download_bytes(self, bucket_name: str, source_blob_name: str) -> bytes:
        """Return the object body as bytes, raising when it is missing."""
        try:
            blob = self._bucket(bucket_name).blob(source_blob_name)
            if not blob.exists():
                raise NotFoundError(
                    f"GCS object not found: gs://{bucket_name}/{source_blob_name}"
                )
            return bytes(blob.download_as_bytes())
        except NotFoundError:
            raise
        except Exception as exc:
            raise ClientError(
                f"GCS download failed for gs://{bucket_name}/{source_blob_name}: {exc}"
            ) from exc

    def download_file(
        self,
        bucket_name: str,
        source_blob_name: str,
        destination_file_name: str,
    ) -> None:
        """Download an object to ``destination_file_name``."""
        payload = self.download_bytes(bucket_name, source_blob_name)
        path = Path(destination_file_name)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as handle:
            handle.write(payload)

    # -- listing -----------------------------------------------------------
    def list_objects(
        self,
        bucket_name: str,
        prefix: str = "",
    ) -> List[Dict[str, Any]]:
        """Return object metadata for every object below ``prefix``."""
        try:
            collected: List[Dict[str, Any]] = []
            for blob in self.client.list_blobs(bucket_name, prefix=prefix or None):
                collected.append(
                    {
                        "name": str(getattr(blob, "name", "")),
                        "size": int(getattr(blob, "size", 0) or 0),
                        "generation": str(getattr(blob, "generation", "") or ""),
                        "crc32c": str(getattr(blob, "crc32c", "") or ""),
                    }
                )
            return collected
        except Exception as exc:
            raise ClientError(
                f"GCS list failed for gs://{bucket_name}/{prefix}: {exc}"
            ) from exc

    def object_exists(self, bucket_name: str, blob_name: str) -> bool:
        """Return ``True`` when the object exists and is readable."""
        try:
            return bool(self._bucket(bucket_name).blob(blob_name).exists())
        except Exception as exc:
            raise ClientError(
                f"GCS head failed for gs://{bucket_name}/{blob_name}: {exc}"
            ) from exc
