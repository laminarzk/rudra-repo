"""Typed configuration loader merging base, module and environment overlays."""

from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import yaml

from dataoutbound.common.logging.logger import get_logger
from dataoutbound.util.constants import (
    DEFAULT_ENVIRONMENT,
    ENV_DATAOUTBOUND_CONFIG_ROOT,
    ENV_DATAOUTBOUND_ENVIRONMENT,
    SECRET_SCHEME_PREFIXES,
)
from dataoutbound.util.exceptions import ConfigError
from dataoutbound.util.utils import deep_merge, get_path, mask_secret

logger = get_logger(__name__)

COMMON_CONFIG_FILENAME = "common_config.yaml"
MODULE_CONFIG_FILENAME = "config.yml"
_ENVIRONMENTS = ("dev", "staging", "prod")
_CACHE_LOCK = threading.Lock()
_CACHE: Dict[str, "ConfigLoader"] = {}


def _default_config_root() -> Path:
    """Return the configuration root, honouring the environment override."""
    override = os.environ.get(ENV_DATAOUTBOUND_CONFIG_ROOT)
    if override:
        return Path(override).expanduser().resolve()
    return Path(__file__).resolve().parent


def _read_yaml(path: Path) -> Dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        with path.open("r", encoding="utf-8") as handle:
            parsed = yaml.safe_load(handle) or {}
    except OSError as exc:
        raise ConfigError(f"Failed to read configuration file '{path}': {exc}") from exc
    except yaml.YAMLError as exc:
        raise ConfigError(
            f"Invalid YAML in configuration file '{path}': {exc}"
        ) from exc
    if not isinstance(parsed, dict):
        raise ConfigError(f"Configuration file '{path}' must contain a mapping")
    return parsed


def _extract_environment_overlay(
    document: Dict[str, Any],
    environment: str,
) -> Dict[str, Any]:
    """Return the environment section, keeping unknown keys out of the base."""
    overlay: Dict[str, Any] = {}
    for name in _ENVIRONMENTS:
        section = document.pop(name, None)
        if isinstance(section, dict) and name == environment:
            overlay = section
    return overlay


class ConfigLoader:
    """Merged, environment-aware configuration accessor.

    A loader combines ``common_config.yaml`` with the module's ``config.yml``
    and then applies the environment overlay. Values are read through dotted
    paths; secret references (``secret://`` / ``ssm://``) are resolved lazily so
    that reading unrelated configuration never triggers an SSM call.
    """

    def __init__(
        self,
        data: Optional[Dict[str, Any]] = None,
        *,
        environment: Optional[str] = None,
        module: Optional[str] = None,
        config_root: Optional[Path] = None,
    ) -> None:
        self.environment = (
            environment
            or os.environ.get(ENV_DATAOUTBOUND_ENVIRONMENT)
            or DEFAULT_ENVIRONMENT
        )
        self.module = module
        self.config_root = Path(config_root) if config_root else _default_config_root()
        self._data: Dict[str, Any] = data if data is not None else {}
        self._secret_client: Any = None
        self._resolved_cache: Dict[str, Any] = {}

    # -- construction ------------------------------------------------------
    @classmethod
    def load_config(
        cls,
        module: Optional[str] = None,
        *,
        environment: Optional[str] = None,
        config_root: Optional[Path] = None,
        use_cache: bool = True,
    ) -> "ConfigLoader":
        """Load and merge configuration for ``module`` (``common`` by default)."""
        resolved_environment = (
            environment
            or os.environ.get(ENV_DATAOUTBOUND_ENVIRONMENT)
            or DEFAULT_ENVIRONMENT
        )
        resolved_root = Path(config_root) if config_root else _default_config_root()
        cache_key = f"{resolved_root}|{module or 'common'}|{resolved_environment}"

        if use_cache:
            with _CACHE_LOCK:
                cached = _CACHE.get(cache_key)
                if cached is not None:
                    return cached

        common_path = resolved_root / COMMON_CONFIG_FILENAME
        common_document = _read_yaml(common_path)
        common_overlay = _extract_environment_overlay(
            common_document, resolved_environment
        )
        merged = deep_merge(_strip_metadata(common_document), common_overlay)

        if module:
            module_path = resolved_root / module / MODULE_CONFIG_FILENAME
            module_document = _read_yaml(module_path)
            module_overlay = _extract_environment_overlay(
                module_document, resolved_environment
            )
            merged = deep_merge(merged, _strip_metadata(module_document))
            merged = deep_merge(merged, module_overlay)

        loader = cls(
            merged,
            environment=resolved_environment,
            module=module,
            config_root=resolved_root,
        )
        if use_cache:
            with _CACHE_LOCK:
                _CACHE[cache_key] = loader
        return loader

    @classmethod
    def clear_cache(cls) -> None:
        """Drop every cached loader; primarily used by tests."""
        with _CACHE_LOCK:
            _CACHE.clear()

    # -- reading -----------------------------------------------------------
    def get(self, path: str, default: Any = None) -> Any:
        """Return the value at ``path`` or ``default`` when it is missing."""
        try:
            return get_path(self._data, path, default)
        except (TypeError, AttributeError):
            return default

    def get_str(self, path: str, default: str = "") -> str:
        """Return a string value, coercing non-string scalars."""
        value = self.get(path, default)
        if value is None:
            return default
        return str(value)

    def get_int(self, path: str, default: int = 0) -> int:
        """Return an integer value, falling back on the default when invalid."""
        value = self.get(path, default)
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def get_float(self, path: str, default: float = 0.0) -> float:
        """Return a float value, falling back on the default when invalid."""
        value = self.get(path, default)
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def get_bool(self, path: str, default: bool = False) -> bool:
        """Return a boolean value, interpreting common truthy strings."""
        value = self.get(path, default)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        if isinstance(value, (int, float)):
            return bool(value)
        return default

    def get_list(self, path: str, default: Optional[Iterable[Any]] = None) -> List[Any]:
        """Return a list value, wrapping scalars and never returning ``None``."""
        value = self.get(path, None)
        if value is None:
            return list(default) if default is not None else []
        if isinstance(value, list):
            return value
        if isinstance(value, tuple):
            return list(value)
        return [value]

    def get_dict(
        self,
        path: str,
        default: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Return a mapping value, falling back on an empty dict when invalid."""
        value = self.get(path, None)
        if isinstance(value, dict):
            return value
        return dict(default) if default else {}

    def require(self, path: str) -> Any:
        """Return the value at ``path``, raising when it is missing or empty."""
        value = self.get(path, None)
        if value is None or value == "":
            raise ConfigError(f"Missing required configuration value: {path}")
        return value

    # -- secret resolution -------------------------------------------------
    def _resolve_secret(self, reference: str) -> str:
        cached = self._resolved_cache.get(reference)
        if cached is not None:
            return cached

        if self._secret_client is None:
            from dataoutbound.common.clients.aws.ssm.ssm_client import SsmClient

            self._secret_client = SsmClient(self)

        name = reference
        for prefix in SECRET_SCHEME_PREFIXES:
            if reference.startswith(prefix):
                name = reference[len(prefix) :]
                break
        value = self._secret_client.get_parameter(name)
        self._resolved_cache[reference] = value
        return value

    def get_secret(self, path: str, default: Optional[str] = None) -> Optional[str]:
        """Resolve the secret reference stored at ``path`` via SSM.

        The referenced value is never logged; only a masked form is written at
        debug level so that operators can confirm which key was read.
        """
        reference = self.get(path, None)
        if reference is None:
            return default
        if not isinstance(reference, str):
            raise ConfigError(f"Secret reference at '{path}' must be a string")
        if not reference.startswith(SECRET_SCHEME_PREFIXES):
            return reference
        try:
            resolved = self._resolve_secret(reference)
        except ConfigError:
            raise
        except Exception as exc:
            raise ConfigError(
                f"Failed to resolve secret reference at '{path}': {exc}"
            ) from exc
        logger.debug(
            "Resolved secret reference",
            extra={"path": path, "value": mask_secret(resolved)},
        )
        return resolved

    # -- introspection -----------------------------------------------------
    def as_dict(self) -> Dict[str, Any]:
        """Return a defensive copy of the merged configuration."""
        return deep_merge(self._data, {})


def _strip_metadata(document: Dict[str, Any]) -> Dict[str, Any]:
    """Remove documentation-only keys that must never be read at runtime."""
    return {
        key: value
        for key, value in document.items()
        if key not in _ENVIRONMENTS and not str(key).startswith("x-")
    }


def load_config(
    module: Optional[str] = None,
    *,
    environment: Optional[str] = None,
    config_root: Optional[Path] = None,
) -> ConfigLoader:
    """Module-level convenience wrapper around :meth:`ConfigLoader.load_config`."""
    return ConfigLoader.load_config(
        module,
        environment=environment,
        config_root=config_root,
    )
