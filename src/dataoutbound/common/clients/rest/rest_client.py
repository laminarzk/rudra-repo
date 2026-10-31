"""Outbound HTTP client with retries, timeouts and typed error mapping."""

from __future__ import annotations

import time
from typing import Any, Callable, Dict, Mapping, Optional

import requests

from dataoutbound.common.logging.logger import get_correlation_id, get_logger
from dataoutbound.config.config_loader import ConfigLoader
from dataoutbound.util.constants import HEADER_CORRELATION_ID
from dataoutbound.util.exceptions import (
    AuthError,
    ClientError,
    NotFoundError,
    ValidationError,
)
from dataoutbound.util.utils import safe_json_loads

logger = get_logger(__name__)

DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_BACKOFF_SECONDS = 0.5
RETRY_STATUS_CODES = frozenset({408, 425, 429, 500, 502, 503, 504})
_BAD_REQUEST_STATUS_CODES = frozenset({400, 409, 422})
_AUTH_STATUS_CODES = frozenset({401, 403})


class RestClient:
    """HTTP client used by adapters such as the MWAA control plane."""

    def __init__(
        self,
        config: Optional[ConfigLoader] = None,
        *,
        base_url: str = "",
        default_headers: Optional[Mapping[str, str]] = None,
        session: Optional[requests.Session] = None,
        timeout: Optional[float] = None,
        auth_provider: Optional[Callable[[], Mapping[str, str]]] = None,
        max_attempts: Optional[int] = None,
        backoff_seconds: float = DEFAULT_BACKOFF_SECONDS,
    ) -> None:
        self._config = config or ConfigLoader.load_config("common")
        self._base_url = (base_url or "").rstrip("/")
        self._default_headers = dict(default_headers or {})
        self._session = session
        self._auth_provider = auth_provider
        self._backoff_seconds = float(backoff_seconds)

        resolved_timeout = timeout
        if resolved_timeout is None:
            resolved_timeout = float(
                self._config.get("timeouts.read_seconds", DEFAULT_TIMEOUT_SECONDS)
            )
        self._timeout = float(resolved_timeout)

        resolved_attempts = max_attempts
        if resolved_attempts is None:
            resolved_attempts = int(
                self._config.get("retry.max_attempts", DEFAULT_MAX_ATTEMPTS)
            )
        self._max_attempts = max(1, int(resolved_attempts))

    # -- helpers -----------------------------------------------------------
    @property
    def session(self) -> requests.Session:
        if self._session is None:
            self._session = requests.Session()
        return self._session

    def _url(self, path: str) -> str:
        if not path:
            return self._base_url
        if path.startswith(("http://", "https://")):
            return path
        if not self._base_url:
            return path
        return f"{self._base_url}/{path.lstrip('/')}"

    def _headers(self, extra: Optional[Mapping[str, str]] = None) -> Dict[str, str]:
        headers: Dict[str, str] = {"Accept": "application/json"}
        headers.update(self._default_headers)

        if self._auth_provider is not None:
            try:
                headers.update(dict(self._auth_provider()))
            except Exception as exc:
                logger.error(
                    "Auth provider failed while building request headers",
                    extra={"error": str(exc)},
                )

        correlation_id = get_correlation_id()
        if correlation_id:
            headers.setdefault(HEADER_CORRELATION_ID, str(correlation_id))

        if extra:
            headers.update(dict(extra))
        return headers

    def _sleep(self, attempt: int) -> None:
        delay = self._backoff_seconds * (2 ** (attempt - 1))
        time.sleep(max(0.0, float(delay)))

    def _handle_response(
        self,
        method: str,
        url: str,
        response: requests.Response,
    ) -> Any:
        if response.status_code >= 400:
            detail = safe_json_loads(response.text, default=response.text)
            message = f"{method.upper()} {url} returned HTTP {response.status_code}"
            if response.status_code == 404:
                raise NotFoundError(message, details=[detail])
            if response.status_code in _BAD_REQUEST_STATUS_CODES:
                raise ValidationError(message, details=[detail])
            if response.status_code in _AUTH_STATUS_CODES:
                raise AuthError(message, details=[detail])
            raise ClientError(message, details=[detail])

        if not response.content:
            return None

        content_type = str(response.headers.get("Content-Type", "")).lower()
        if "json" in content_type:
            parsed = safe_json_loads(response.text, default=None)
            if parsed is None:
                raise ClientError(
                    f"{method.upper()} {url} returned a malformed JSON body"
                )
            return parsed
        return response.text

    # -- core request ------------------------------------------------------
    def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        json: Any = None,
        data: Any = None,
        headers: Optional[Mapping[str, str]] = None,
        timeout: Optional[float] = None,
    ) -> Any:
        """Perform an HTTP request with retry, timeout and typed errors."""
        url = self._url(path)
        attempt = 0
        while True:
            attempt += 1
            try:
                response = self.session.request(
                    method=method.upper(),
                    url=url,
                    params=params,
                    json=json,
                    data=data,
                    headers=self._headers(headers),
                    timeout=timeout or self._timeout,
                )
            except requests.RequestException as exc:
                if attempt >= self._max_attempts:
                    raise ClientError(
                        f"{method.upper()} {url} failed after {attempt} attempts: {exc}"
                    ) from exc
                logger.warning(
                    "HTTP request failed, retrying",
                    extra={
                        "method": method.upper(),
                        "url": url,
                        "attempt": attempt,
                        "error": str(exc),
                    },
                )
                self._sleep(attempt)
                continue

            if (
                response.status_code in RETRY_STATUS_CODES
                and attempt < self._max_attempts
            ):
                logger.warning(
                    "HTTP request returned a retryable status",
                    extra={
                        "method": method.upper(),
                        "url": url,
                        "attempt": attempt,
                        "status_code": response.status_code,
                    },
                )
                self._sleep(attempt)
                continue

            return self._handle_response(method, url, response)

    # -- verbs -------------------------------------------------------------
    def get(
        self,
        path: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
        timeout: Optional[float] = None,
    ) -> Any:
        """Issue a GET request and return the decoded body."""
        return self.request(
            "GET", path, params=params, headers=headers, timeout=timeout
        )

    def post(
        self,
        path: str,
        *,
        json: Any = None,
        data: Any = None,
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
        timeout: Optional[float] = None,
    ) -> Any:
        """Issue a POST request and return the decoded body."""
        return self.request(
            "POST",
            path,
            json=json,
            data=data,
            params=params,
            headers=headers,
            timeout=timeout,
        )

    def put(
        self,
        path: str,
        *,
        json: Any = None,
        data: Any = None,
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
        timeout: Optional[float] = None,
    ) -> Any:
        """Issue a PUT request and return the decoded body."""
        return self.request(
            "PUT",
            path,
            json=json,
            data=data,
            params=params,
            headers=headers,
            timeout=timeout,
        )

    def patch(
        self,
        path: str,
        *,
        json: Any = None,
        data: Any = None,
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
        timeout: Optional[float] = None,
    ) -> Any:
        """Issue a PATCH request and return the decoded body."""
        return self.request(
            "PATCH",
            path,
            json=json,
            data=data,
            params=params,
            headers=headers,
            timeout=timeout,
        )

    def delete(
        self,
        path: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
        timeout: Optional[float] = None,
    ) -> Any:
        """Issue a DELETE request and return the decoded body."""
        return self.request(
            "DELETE", path, params=params, headers=headers, timeout=timeout
        )

    def close(self) -> None:
        """Close the underlying ``requests`` session when one was created."""
        if self._session is not None:
            self._session.close()
            self._session = None
