"""Structured JSON logger factory shared by every DataOutbound component."""

from __future__ import annotations

import json
import logging
import os
import sys
import threading
from contextvars import ContextVar
from typing import Any, Dict, Optional

from dataoutbound.util.constants import (
    DEFAULT_LOG_LEVEL,
    ENV_DATAOUTBOUND_LOG_LEVEL,
)
from dataoutbound.util.utils import utc_now_millis

_CORRELATION_ID: ContextVar[Optional[str]] = ContextVar(
    "dataoutbound_correlation_id", default=None
)
_COMPONENT: ContextVar[Optional[str]] = ContextVar(
    "dataoutbound_component", default=None
)

_REDACTED_KEYS = frozenset(
    {
        "password",
        "passwd",
        "secret",
        "token",
        "authorization",
        "api_key",
        "apikey",
        "credentials",
        "client_secret",
        "private_key",
        "pgp_key",
        "sas_token",
        "connection_string",
    }
)

_RESERVED_FIELDS = frozenset(
    logging.LogRecord("", 0, "", 0, "", None, None).__dict__.keys()
) | {"component", "correlation_id", "message", "asctime"}

_LOGGER_LOCK = threading.Lock()
_CONFIGURED_LOGGERS: Dict[str, logging.Logger] = {}


def _coerce_level(value: Any) -> int:
    if isinstance(value, int):
        return value
    name = str(value or DEFAULT_LOG_LEVEL).upper()
    resolved = logging.getLevelName(name)
    if isinstance(resolved, int):
        return resolved
    return logging.INFO


def bind_correlation_id(correlation_id: Optional[str]) -> None:
    """Attach a correlation id to every log line in the current context."""
    _CORRELATION_ID.set(correlation_id)


def get_correlation_id() -> Optional[str]:
    """Return the correlation id currently bound to this context, if any."""
    return _CORRELATION_ID.get()


def bind_component(component: Optional[str]) -> None:
    """Attach a logical component name to every log line in this context."""
    _COMPONENT.set(component)


def _redact(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): (
                "***REDACTED***"
                if str(key).lower() in _REDACTED_KEYS
                else _redact(item)
            )
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_redact(item) for item in value]
    return value


class StructuredJsonFormatter(logging.Formatter):
    """Render log records as single-line JSON objects.

    Extra keyword arguments passed via ``logger.info(..., extra={...})`` are
    merged into the payload, with well-known secret keys redacted so that PII
    and credentials never reach the log sink.
    """

    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "timestamp": utc_now_millis(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        component = _COMPONENT.get() or getattr(record, "component", None)
        if component:
            payload["component"] = component

        correlation_id = _CORRELATION_ID.get() or getattr(
            record, "correlation_id", None
        )
        if correlation_id:
            payload["correlation_id"] = correlation_id

        for key, value in record.__dict__.items():
            if key in _RESERVED_FIELDS:
                continue
            payload[key] = _redact(value)

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, separators=(",", ":"))


def _resolve_level_name() -> str:
    override = os.environ.get(ENV_DATAOUTBOUND_LOG_LEVEL)
    if override:
        return override
    return DEFAULT_LOG_LEVEL


def get_logger(name: str, *, component: Optional[str] = None) -> logging.Logger:
    """Return a structured logger that emits single-line JSON records."""
    with _LOGGER_LOCK:
        logger = _CONFIGURED_LOGGERS.get(name)
        if logger is None:
            logger = logging.getLogger(name)
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(StructuredJsonFormatter())
            logger.handlers = [handler]
            logger.propagate = False
            logger.setLevel(_coerce_level(_resolve_level_name()))
            _CONFIGURED_LOGGERS[name] = logger

    if component:
        adapter = logging.LoggerAdapter(logger, {"component": component})
        return adapter  # type: ignore[return-value]
    return logger


def set_level(level: Any) -> None:
    """Update the level on every logger produced by :func:`get_logger`."""
    resolved = _coerce_level(level)
    with _LOGGER_LOCK:
        for logger in _CONFIGURED_LOGGERS.values():
            logger.setLevel(resolved)
