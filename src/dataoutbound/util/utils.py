"""Small pure helpers shared by more than one DataOutbound module."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional, Sequence, TypeVar

T = TypeVar("T")

_UTC_MILLIS_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"


def utc_now() -> datetime:
    """Return a timezone-aware ``datetime`` in UTC."""
    return datetime.now(timezone.utc)


def to_millis_string(value: datetime) -> str:
    """Format a datetime as ``YYYY-MM-DDTHH:MM:SS.sssZ``.

    Naive datetimes are treated as UTC so that audit payloads always carry an
    unambiguous timestamp, matching the ``audit_event.json`` schema pattern.
    """
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    value = value.astimezone(timezone.utc)
    return value.strftime(_UTC_MILLIS_FORMAT)[:-3] + "Z"


def utc_now_millis() -> str:
    """Return the current UTC time as ``YYYY-MM-DDTHH:MM:SS.sssZ``."""
    return to_millis_string(utc_now())


def parse_iso8601(value: str) -> datetime:
    """Parse an ISO-8601 string, tolerating a trailing ``Z`` suffix."""
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def chunked(items: Sequence[T], size: int) -> Iterator[List[T]]:
    """Yield successive lists containing at most ``size`` items each."""
    if size <= 0:
        raise ValueError("size must be a positive integer")
    for start in range(0, len(items), size):
        yield list(items[start : start + size])


def safe_json_loads(data: Any, default: Any = None) -> Any:
    """Best-effort JSON decode returning ``default`` rather than raising."""
    if data is None:
        return default
    if isinstance(data, (bytes, bytearray)):
        try:
            data = data.decode("utf-8")
        except UnicodeDecodeError:
            return default
    try:
        return json.loads(data)
    except (TypeError, ValueError):
        return default


def safe_json_dumps(value: Any, default: str = "{}") -> str:
    """Best-effort JSON encode of an arbitrary Python value."""
    try:
        return json.dumps(value, default=str, separators=(",", ":"))
    except (TypeError, ValueError):
        return default


def deep_merge(base: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge ``overrides`` into ``base`` and return a new dict."""
    merged: Dict[str, Any] = dict(base)
    for key, value in overrides.items():
        existing = merged.get(key)
        if isinstance(existing, dict) and isinstance(value, dict):
            merged[key] = deep_merge(existing, value)
        else:
            merged[key] = value
    return merged


def get_path(data: Any, path: str, default: Any = None) -> Any:
    """Look up a dotted path (``a.b.c``) inside nested dictionaries."""
    if not path:
        return data
    node: Any = data
    for part in path.split("."):
        if not isinstance(node, dict):
            return default
        if part not in node:
            return default
        node = node[part]
    return node


def mask_secret(value: Optional[str], *, visible: int = 4) -> str:
    """Return a redacted representation of a secret that is safe to log."""
    if not value:
        return ""
    if len(value) <= visible:
        return "*" * len(value)
    return "*" * (len(value) - visible) + value[-visible:]


def ensure_dict(value: Any) -> Dict[str, Any]:
    """Return ``value`` when it is a dict, otherwise an empty dict."""
    return value if isinstance(value, dict) else {}


def ensure_list(value: Any) -> List[Any]:
    """Return ``value`` as a list, wrapping scalars when required."""
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]
