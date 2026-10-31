"""Idempotency keys and retry helpers reused by APIs, Lambdas and DAG tasks."""

from __future__ import annotations

import hashlib
import json
import random
import time
from functools import wraps
from typing import Any, Callable, Optional, Tuple, Type, TypeVar

from dataoutbound.common.logging.logger import get_logger

logger = get_logger(__name__)

T = TypeVar("T")

_SEPARATOR = "|"


def _normalize(part: Any) -> str:
    if part is None:
        return ""
    if isinstance(part, (dict, list, tuple, set)):
        try:
            return json.dumps(part, sort_keys=True, default=str, separators=(",", ":"))
        except (TypeError, ValueError):
            return repr(part)
    return str(part)


class IdempotencyKey:
    """Deterministic key builder for idempotent operations.

    Identical inputs always produce identical keys, which lets downstream
    systems deduplicate re-triggered work (Airflow backfills, Lambda retries,
    API replays) without a central coordination store.
    """

    @staticmethod
    def build(*parts: Any) -> str:
        """Return a stable SHA-256 hex digest derived from ``parts``."""
        normalized = _SEPARATOR.join(_normalize(part) for part in parts)
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

    @staticmethod
    def namespaced(prefix: str, *parts: Any) -> str:
        """Return ``<prefix>:<digest>`` for readable, collision-free keys."""
        return f"{prefix}:{IdempotencyKey.build(*parts)}"


def retry(
    *,
    max_attempts: int = 3,
    base_delay: float = 0.5,
    max_delay: float = 30.0,
    backoff_factor: float = 2.0,
    jitter: float = 0.1,
    retry_on: Tuple[Type[BaseException], ...] = (Exception,),
    give_up_on: Tuple[Type[BaseException], ...] = (),
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator applying exponential backoff with jitter to ``func``.

    Exceptions listed in ``give_up_on`` (for example fail-closed validation
    errors) propagate immediately, while ``retry_on`` failures are retried
    until ``max_attempts`` is exhausted.
    """
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            attempt = 0
            while True:
                attempt += 1
                try:
                    return func(*args, **kwargs)
                except give_up_on:
                    raise
                except retry_on as exc:
                    if attempt >= max_attempts:
                        logger.error(
                            "Retry attempts exhausted",
                            extra={
                                "function": getattr(func, "__qualname__", str(func)),
                                "attempts": attempt,
                                "error": str(exc),
                            },
                        )
                        raise
                    delay = min(
                        max_delay, base_delay * (backoff_factor ** (attempt - 1))
                    )
                    if jitter:
                        delay += random.uniform(0.0, delay * jitter)
                    logger.warning(
                        "Transient failure, retrying",
                        extra={
                            "function": getattr(func, "__qualname__", str(func)),
                            "attempt": attempt,
                            "delay_seconds": round(delay, 3),
                            "error": str(exc),
                        },
                    )
                    time.sleep(delay)

        return wrapper

    return decorator


def compute_backoff_delay(
    attempt: int,
    *,
    base_delay: float = 0.5,
    max_delay: float = 30.0,
    backoff_factor: float = 2.0,
    jitter: float = 0.1,
) -> float:
    """Return the delay (seconds) for a 1-based ``attempt`` number."""
    if attempt < 1:
        raise ValueError("attempt must be a 1-based positive integer")
    delay: float = min(max_delay, base_delay * (backoff_factor ** (attempt - 1)))
    if jitter:
        delay += random.uniform(0.0, delay * jitter)
    return delay


def optional_key(value: Optional[str]) -> Optional[str]:
    """Normalise an optional idempotency key supplied by a caller."""
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None
