"""Typed Azure Blob Storage / ADLS Gen2 client for export delivery."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from dataoutbound.common.logging.logger import get_logger
from dataoutbound.config.config_loader import ConfigLoader
from dataoutbound.util.exceptions import ClientError, ConfigError, NotFoundError

logger = get_logger(__name__)


def _import_blob_sdk() -> Any:
    """Import the Azure SDK lazily so AWS-only deployments need not install it."""
    try:
        from azure.storage.blob import BlobServiceClient
    except ImportError as exc:
        raise ConfigError("azure-storage-blob is required to use BlobClient") from exc
    return BlobServiceClient


class BlobClient:
    """Upload, download and list blobs in customer containers.

    Credentials are resolved from configuration: either a connection string
    backed by a managed identity or a shared access signature. Account keys are
    never accepted directly so that the solution stays on Entra ID RBAC.
    """

    def __init__(
        self,
        config: Optional[ConfigLoader] = None,
        *,
        account_url: Optional[str] = None,
        credential: Any = None,
        service_client: Any = None,
    ) -> None:
        self._config = config or ConfigLoader.load_config("common")
        self._service_client = service_client
        self._account_url = account_url or self._config.get_str(
            "azure.blob.account_url"
        )
        self._credential = credential

    @property
    def service_client(self) -> Any:
        if self._service_client is None:
            if not self._account_url:
                raise ConfigError(
                    "Missing required configuration value: azure.blob.account_url"
                )
            blob_service_client = _import_blob_sdk()
            try:
                if self._credential is not None:
                    self._service_client = blob_service_client(
                        account_url=self._account_url,
                        credential=self._credential,
                    )
                else:
                    from azure.identity import DefaultAzureCredential

                    self._service_client = blob_service_client(
                        account_url=self._account_url,
                        credential=DefaultAzureCredential(),
                    )
            except Exception as exc:
                raise ClientError(
                    f"Failed to create Azure Blob service client: {exc}"
                ) from exc
        return self._service_client

    # -- uploads -----------------------------------------------------------
    def upload_bytes(
        self,
        container: str,
        blob_name: str,
        data: bytes,
        *,
        metadata: Optional[Dict[str, str]] = None,
        overwrite: bool = True,
    ) -> str:
        """Upload raw bytes and return the resulting ETag."""
        try:
            blob = self.service_client.get_blob_client(
                container=container,
                blob=blob_name,
            )
            response = blob.upload_blob(
                data,
                overwrite=overwrite,
                metadata=metadata,
            )
        except Exception as exc:
            raise ClientError(
                f"Azure Blob upload failed for {container}/{blob_name}: {exc}"
            ) from exc
        etag = str(
            (response or {}).get("etag", "")
            if isinstance(response, dict)
            else getattr(response, "etag", "")
        )
        logger.debug(
            "Azure Blob upload complete",
            extra={"container": container, "blob_name": blob_name},
        )
        return etag

    def upload_file(
        self,
        container: str,
        blob_name: str,
        local_path: str,
        *,
        metadata: Optional[Dict[str, str]] = None,
        overwrite: bool = True,
    ) -> str:
        """Upload a local file and return the resulting ETag."""
        path = Path(local_path)
        if not path.is_file():
            raise NotFoundError(f"Local file not found: {local_path}")
        with path.open("rb") as handle:
            return self.upload_bytes(
                container,
                blob_name,
                handle.read(),
                metadata=metadata,
                overwrite=overwrite,
            )

    def upload_json(
        self,
        container: str,
        blob_name: str,
        data: Any,
        *,
        metadata: Optional[Dict[str, str]] = None,
        overwrite: bool = True,
    ) -> str:
        """Serialise ``data`` as JSON and upload it as a blob."""
        payload = json.dumps(data, default=str).encode("utf-8")
        return self.upload_bytes(
            container,
            blob_name,
            payload,
            metadata=metadata,
            overwrite=overwrite,
        )

    # -- downloads ---------------------------------------------------------
    def download_bytes(self, container: str, blob_name: str) -> bytes:
        """Return the blob body as bytes, raising when it is missing."""
        try:
            blob = self.service_client.get_blob_client(
                container=container,
                blob=blob_name,
            )
            if not blob.exists():
                raise NotFoundError(f"Azure Blob not found: {container}/{blob_name}")
            stream = blob.download_blob()
            return bytes(stream.readall())
        except NotFoundError:
            raise
        except Exception as exc:
            raise ClientError(
                f"Azure Blob download failed for {container}/{blob_name}: {exc}"
            ) from exc

    def download_file(self, container: str, blob_name: str, local_path: str) -> None:
        """Download a blob to ``local_path``, creating parent directories."""
        payload = self.download_bytes(container, blob_name)
        path = Path(local_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as handle:
            handle.write(payload)

    # -- listing -----------------------------------------------------------
    def list_blobs(self, container: str, prefix: str = "") -> List[Dict[str, Any]]:
        """Return blob metadata for every blob below ``prefix``."""
        try:
            container_client = self.service_client.get_container_client(container)
            collected: List[Dict[str, Any]] = []
            for blob in container_client.list_blobs(name_starts_with=prefix or None):
                collected.append(
                    {
                        "name": str(getattr(blob, "name", "")),
                        "size": int(getattr(blob, "size", 0) or 0),
                        "etag": str(getattr(blob, "etag", "")),
                    }
                )
            return collected
        except Exception as exc:
            raise ClientError(
                f"Azure Blob list failed for {container}/{prefix}: {exc}"
            ) from exc
