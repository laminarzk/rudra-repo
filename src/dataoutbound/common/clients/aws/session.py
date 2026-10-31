"""Single boto3 session / client factory used by every AWS client wrapper."""

from __future__ import annotations

import os
import threading
from typing import Any, Optional

import boto3
from botocore.config import Config as BotoConfig

from dataoutbound.common.logging.logger import get_logger
from dataoutbound.config.config_loader import ConfigLoader
from dataoutbound.util.constants import DEFAULT_AWS_REGION, ENV_AWS_REGION
from dataoutbound.util.exceptions import ClientError

logger = get_logger(__name__)

_SESSION_LOCK = threading.Lock()
_CACHED_SESSION: Optional[boto3.Session] = None
_CACHED_REGION: Optional[str] = None


def _resolve_region(config: Optional[ConfigLoader], region: Optional[str]) -> str:
    if region:
        return region
    if config is not None:
        configured = config.get("aws.region")
        if configured:
            return str(configured)
    return os.environ.get(ENV_AWS_REGION) or DEFAULT_AWS_REGION


def _resolve_botocore_config(
    config: Optional[ConfigLoader],
    override: Optional[BotoConfig],
) -> BotoConfig:
    if override is not None:
        return override

    retries: dict = {"max_attempts": 5, "mode": "standard"}
    connect_timeout = 5.0
    read_timeout = 30.0

    if config is not None:
        retries = {
            "max_attempts": int(config.get("aws.retry.max_attempts", 5)),
            "mode": str(config.get("aws.retry.mode", "standard")),
        }
        connect_timeout = float(config.get("timeouts.connect_seconds", 5))
        read_timeout = float(config.get("timeouts.read_seconds", 30))

    return BotoConfig(
        retries=retries,
        connect_timeout=connect_timeout,
        read_timeout=read_timeout,
    )


def get_session(
    config: Optional[ConfigLoader] = None,
    *,
    region: Optional[str] = None,
) -> boto3.Session:
    """Return a cached boto3 session bound to the resolved region."""
    global _CACHED_SESSION, _CACHED_REGION

    resolved_region = _resolve_region(config, region)
    with _SESSION_LOCK:
        if _CACHED_SESSION is None or _CACHED_REGION != resolved_region:
            _CACHED_SESSION = boto3.Session(region_name=resolved_region)
            _CACHED_REGION = resolved_region
        return _CACHED_SESSION


def get_client(
    service_name: str,
    *,
    config: Optional[ConfigLoader] = None,
    region: Optional[str] = None,
    endpoint_url: Optional[str] = None,
    botocore_config: Optional[BotoConfig] = None,
    session: Optional[boto3.Session] = None,
) -> Any:
    """Create a boto3 client with retry, timeout and endpoint configuration."""
    try:
        active_session = session or get_session(config, region=region)
        resolved_endpoint = endpoint_url
        if resolved_endpoint is None and config is not None:
            configured = config.get(f"aws.endpoints.{service_name}")
            if configured:
                resolved_endpoint = str(configured)

        kwargs: dict = {
            "config": _resolve_botocore_config(config, botocore_config),
        }
        if resolved_endpoint:
            kwargs["endpoint_url"] = resolved_endpoint
        if region:
            kwargs["region_name"] = region

        return active_session.client(service_name, **kwargs)
    except ClientError:
        raise
    except Exception as exc:
        raise ClientError(
            f"Failed to create boto3 client for service '{service_name}': {exc}"
        ) from exc


def get_resource(
    service_name: str,
    *,
    config: Optional[ConfigLoader] = None,
    region: Optional[str] = None,
    endpoint_url: Optional[str] = None,
    botocore_config: Optional[BotoConfig] = None,
    session: Optional[boto3.Session] = None,
) -> Any:
    """Create a boto3 resource with the same defaults as :func:`get_client`."""
    try:
        active_session = session or get_session(config, region=region)
        resolved_endpoint = endpoint_url
        if resolved_endpoint is None and config is not None:
            configured = config.get(f"aws.endpoints.{service_name}")
            if configured:
                resolved_endpoint = str(configured)

        kwargs: dict = {
            "config": _resolve_botocore_config(config, botocore_config),
        }
        if resolved_endpoint:
            kwargs["endpoint_url"] = resolved_endpoint
        if region:
            kwargs["region_name"] = region

        return active_session.resource(service_name, **kwargs)
    except ClientError:
        raise
    except Exception as exc:
        raise ClientError(
            f"Failed to create boto3 resource for service '{service_name}': {exc}"
        ) from exc


def reset_session_cache() -> None:
    """Drop the cached session; primarily used by tests and DAG re-entry."""
    global _CACHED_SESSION, _CACHED_REGION
    with _SESSION_LOCK:
        _CACHED_SESSION = None
        _CACHED_REGION = None
        logger.debug("boto3 session cache reset")
