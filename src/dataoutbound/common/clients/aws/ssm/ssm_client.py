"""Typed AWS Systems Manager Parameter Store client with TTL caching."""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, Optional

from botocore.exceptions import BotoCoreError
from botocore.exceptions import ClientError as BotoClientError

from dataoutbound.common.clients.aws.session import get_client
from dataoutbound.common.logging.logger import get_logger
from dataoutbound.config.config_loader import ConfigLoader
from dataoutbound.util.exceptions import ClientError, NotFoundError

logger = get_logger(__name__)

DEFAULT_CACHE_TTL_SECONDS = 300
_PARAMETER_PAGE_SIZE = 10
_NOT_FOUND_CODE = "ParameterNotFound"


class _CacheEntry:
    __slots__ = ("value", "expires_at")

    def __init__(self, value: str, expires_at: float) -> None:
        self.value = value
        self.expires_at = expires_at


def _error_code(exc: Exception) -> str:
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        error = response.get("Error")
        if isinstance(error, dict):
            return str(error.get("Code", ""))
    return ""


class SsmClient:
    """Read and write Parameter Store values by name or prefix."""

    def __init__(
        self,
        config: Optional[ConfigLoader] = None,
        *,
        client: Any = None,
        cache_ttl_seconds: int = DEFAULT_CACHE_TTL_SECONDS,
    ) -> None:
        self._config = config or ConfigLoader.load_config("common")
        self._client = client
        self._cache: Dict[str, _CacheEntry] = {}
        self._cache_ttl = max(0, int(cache_ttl_seconds))
        self._lock = threading.Lock()

    @property
    def client(self) -> Any:
        if self._client is None:
            self._client = get_client("ssm", config=self._config)
        return self._client

    # -- cache -------------------------------------------------------------
    def _get_cached(self, key: str) -> Optional[str]:
        if self._cache_ttl <= 0:
            return None
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return None
            if entry.expires_at < time.monotonic():
                self._cache.pop(key, None)
                return None
            return entry.value

    def _set_cached(self, key: str, value: str) -> None:
        if self._cache_ttl <= 0:
            return
        with self._lock:
            self._cache[key] = _CacheEntry(
                value=value,
                expires_at=time.monotonic() + self._cache_ttl,
            )

    def clear_cache(self) -> None:
        """Invalidate every cached parameter value."""
        with self._lock:
            self._cache.clear()

    # -- reads -------------------------------------------------------------
    def get_parameter(
        self,
        name: str,
        *,
        decrypt: bool = True,
        use_cache: bool = True,
    ) -> str:
        """Return the value of a single parameter, raising if it is missing."""
        if not name:
            raise ClientError("SSM parameter name must be a non-empty string")

        cache_key = f"{name}::{int(decrypt)}"
        if use_cache:
            cached = self._get_cached(cache_key)
            if cached is not None:
                return cached

        try:
            response = self.client.get_parameter(Name=name, WithDecryption=decrypt)
            value = str(response["Parameter"]["Value"])
        except BotoClientError as exc:
            if _error_code(exc) == _NOT_FOUND_CODE:
                raise NotFoundError(f"SSM parameter not found: {name}") from exc
            raise ClientError(f"Failed to read SSM parameter '{name}': {exc}") from exc
        except BotoCoreError as exc:
            raise ClientError(f"Failed to read SSM parameter '{name}': {exc}") from exc

        if use_cache:
            self._set_cached(cache_key, value)
        return value

    def get_parameter_or_none(
        self,
        name: str,
        *,
        decrypt: bool = True,
    ) -> Optional[str]:
        """Return the parameter value or ``None`` when it does not exist."""
        try:
            return self.get_parameter(name, decrypt=decrypt)
        except NotFoundError:
            return None

    def get_parameters_by_prefix(
        self,
        prefix: str,
        *,
        decrypt: bool = True,
    ) -> Dict[str, str]:
        """Return every parameter below ``prefix`` keyed by full name."""
        if not prefix:
            raise ClientError("SSM parameter prefix must be a non-empty string")

        collected: Dict[str, str] = {}
        token: Optional[str] = None
        while True:
            kwargs: Dict[str, Any] = {
                "Path": prefix,
                "WithDecryption": decrypt,
                "Recursive": True,
                "MaxResults": _PARAMETER_PAGE_SIZE,
            }
            if token:
                kwargs["NextToken"] = token
            try:
                response = self.client.get_parameters_by_path(**kwargs)
            except BotoCoreError as exc:
                raise ClientError(
                    f"Failed to read SSM parameters under '{prefix}': {exc}"
                ) from exc
            except BotoClientError as exc:
                raise ClientError(
                    f"Failed to read SSM parameters under '{prefix}': {exc}"
                ) from exc

            for parameter in response.get("Parameters", []):
                collected[str(parameter["Name"])] = str(parameter["Value"])

            token = response.get("NextToken")
            if not token:
                break
        return collected

    # -- writes ------------------------------------------------------------
    def put_parameter(
        self,
        name: str,
        value: str,
        *,
        parameter_type: str = "SecureString",
        overwrite: bool = True,
    ) -> None:
        """Create or update a parameter and invalidate the local cache."""
        if not name:
            raise ClientError("SSM parameter name must be a non-empty string")
        try:
            self.client.put_parameter(
                Name=name,
                Value=value,
                Type=parameter_type,
                Overwrite=overwrite,
            )
        except BotoCoreError as exc:
            raise ClientError(f"Failed to write SSM parameter '{name}': {exc}") from exc
        except BotoClientError as exc:
            raise ClientError(f"Failed to write SSM parameter '{name}': {exc}") from exc
        self.clear_cache()
