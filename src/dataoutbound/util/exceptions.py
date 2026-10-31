"""Domain exception hierarchy shared by every DataOutbound component."""

from __future__ import annotations

from typing import Any, Iterable, List, Optional


class LaminarError(Exception):
    """Base class for every domain error raised by the DataOutbound platform.

    Carries a stable machine-readable ``code`` alongside the human readable
    message so that transports (HTTP, Lambda responses, Airflow callbacks) can
    map failures onto their own error envelopes without string matching.
    """

    default_code: str = "LAMINAR_ERROR"

    def __init__(
        self,
        message: str,
        *,
        code: Optional[str] = None,
        details: Optional[Iterable[Any]] = None,
    ) -> None:
        super().__init__(message)
        self.message: str = str(message)
        self.code: str = code or self.default_code
        self.details: List[Any] = list(details) if details else []

    def to_dict(self) -> dict:
        """Return the canonical error envelope representation."""
        return {
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }

    def __str__(self) -> str:
        return self.message


class ConfigError(LaminarError):
    """Raised when configuration cannot be read, parsed or resolved."""

    default_code = "CONFIG_ERROR"


class ClientError(LaminarError):
    """Raised when an external system call fails after mapping SDK errors."""

    default_code = "CLIENT_ERROR"


class NotFoundError(LaminarError):
    """Raised when a requested resource does not exist in an external system."""

    default_code = "NOT_FOUND"


class ConflictError(LaminarError):
    """Raised when a write conflicts with the current state of a resource."""

    default_code = "CONFLICT"


class AuthError(LaminarError):
    """Raised when authentication or authorization checks fail."""

    default_code = "UNAUTHORIZED"


class ValidationError(LaminarError):
    """Raised when a payload fails schema or business validation."""

    default_code = "VALIDATION_ERROR"
