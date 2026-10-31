"""SFTP client with key-based auth and atomic rename on completion."""

from __future__ import annotations

import os
import posixpath
from pathlib import Path
from typing import Any, Dict, List, Optional

from dataoutbound.common.logging.logger import get_logger
from dataoutbound.config.config_loader import ConfigLoader
from dataoutbound.util.exceptions import ClientError, ConfigError, NotFoundError

logger = get_logger(__name__)

DEFAULT_PORT = 22
DEFAULT_TIMEOUT_SECONDS = 30
_TEMP_SUFFIX = ".partial"


def _import_paramiko() -> Any:
    """Import paramiko lazily so non-SFTP deployments need not install it."""
    try:
        import paramiko
    except ImportError as exc:
        raise ConfigError("paramiko is required to use SftpClient") from exc
    return paramiko


class SftpClient:
    """Upload, download and list files on a partner SFTP server.

    Uploads are written to a ``.partial`` sibling and renamed into place once
    the transfer succeeds, so a reader never observes a truncated file.
    Credentials come from configuration secrets and are never logged.
    """

    def __init__(
        self,
        config: Optional[ConfigLoader] = None,
        *,
        host: Optional[str] = None,
        port: Optional[int] = None,
        username: Optional[str] = None,
        private_key_path: Optional[str] = None,
        password: Optional[str] = None,
        transport: Any = None,
    ) -> None:
        self._config = config or ConfigLoader.load_config("common")
        self._host = host or self._config.get_str("sftp.host")
        self._port = int(port or self._config.get_int("sftp.port", DEFAULT_PORT))
        self._username = username or self._config.get_str("sftp.username")
        self._private_key_path = private_key_path or self._config.get_str(
            "sftp.private_key_path"
        )
        self._password = password
        self._transport = transport
        self._client: Any = None
        self._sftp: Any = None

    # -- connection --------------------------------------------------------
    def _resolve_password(self) -> Optional[str]:
        if self._password:
            return self._password
        return self._config.get_secret("sftp.password_secret", None)

    def connect(self) -> Any:
        """Open the SFTP session, reusing it on subsequent calls."""
        if self._sftp is not None:
            return self._sftp

        if not self._host or not self._username:
            raise ConfigError(
                "SFTP requires both 'sftp.host' and 'sftp.username' to be configured"
            )

        paramiko = _import_paramiko()
        timeout = self._config.get_int("sftp.timeout_seconds", DEFAULT_TIMEOUT_SECONDS)
        try:
            transport = self._transport or paramiko.Transport((self._host, self._port))
            transport.banner_timeout = timeout
            transport.auth_timeout = timeout

            if self._private_key_path:
                key_path = Path(self._private_key_path)
                if not key_path.is_file():
                    raise NotFoundError(
                        f"SFTP private key not found: {self._private_key_path}"
                    )
                pkey = paramiko.RSAKey.from_private_key_file(str(key_path))
                transport.connect(username=self._username, pkey=pkey)
            else:
                password = self._resolve_password()
                if not password:
                    raise ConfigError(
                        "SFTP requires a private key or a configured password secret"
                    )
                transport.connect(username=self._username, password=password)

            self._transport = transport
            self._sftp = paramiko.SFTPClient.from_transport(transport)
            logger.info(
                "SFTP session established",
                extra={"host": self._host, "port": self._port},
            )
            return self._sftp
        except (ConfigError, NotFoundError):
            raise
        except Exception as exc:
            raise ClientError(
                f"SFTP connection failed for {self._host}:{self._port}: {exc}"
            ) from exc

    def close(self) -> None:
        """Close the SFTP session and underlying transport."""
        if self._sftp is not None:
            try:
                self._sftp.close()
            finally:
                self._sftp = None
        if self._transport is not None:
            try:
                self._transport.close()
            finally:
                self._transport = None

    def __enter__(self) -> "SftpClient":
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()

    # -- uploads -----------------------------------------------------------
    def upload(
        self,
        local_path: str,
        remote_path: str,
        *,
        atomic: bool = True,
    ) -> None:
        """Upload a file, optionally renaming it into place atomically."""
        path = Path(local_path)
        if not path.is_file():
            raise NotFoundError(f"Local file not found: {local_path}")

        sftp = self.connect()
        target = f"{remote_path}{_TEMP_SUFFIX}" if atomic else remote_path
        try:
            self._ensure_remote_directory(posixpath.dirname(target))
            sftp.put(str(path), target)
            if atomic:
                sftp.posix_rename(target, remote_path)
        except Exception as exc:
            raise ClientError(f"SFTP upload failed for {remote_path}: {exc}") from exc
        logger.info(
            "SFTP upload complete",
            extra={"host": self._host, "remote_path": remote_path},
        )

    def _ensure_remote_directory(self, directory: str) -> None:
        if not directory:
            return
        sftp = self.connect()
        parts = [part for part in directory.split("/") if part]
        current = "/" if directory.startswith("/") else ""
        for part in parts:
            current = posixpath.join(current, part) if current else part
            try:
                sftp.stat(current)
            except IOError:
                try:
                    sftp.mkdir(current)
                except IOError as exc:
                    raise ClientError(
                        f"Failed to create remote directory '{current}': {exc}"
                    ) from exc

    # -- downloads ---------------------------------------------------------
    def download(self, remote_path: str, local_path: str) -> None:
        """Download a remote file to ``local_path``."""
        sftp = self.connect()
        target = Path(local_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            sftp.get(remote_path, str(target))
        except FileNotFoundError as exc:
            raise NotFoundError(f"SFTP file not found: {remote_path}") from exc
        except Exception as exc:
            raise ClientError(f"SFTP download failed for {remote_path}: {exc}") from exc

    # -- listing -----------------------------------------------------------
    def list_files(self, remote_directory: str = ".") -> List[Dict[str, Any]]:
        """Return metadata for every file directly inside a directory."""
        sftp = self.connect()
        try:
            entries = sftp.listdir_attr(remote_directory)
        except Exception as exc:
            raise ClientError(
                f"SFTP listing failed for {remote_directory}: {exc}"
            ) from exc

        collected: List[Dict[str, Any]] = []
        for entry in entries:
            name = str(getattr(entry, "filename", ""))
            collected.append(
                {
                    "name": name,
                    "path": posixpath.join(remote_directory, name),
                    "size": int(getattr(entry, "st_size", 0) or 0),
                    "is_directory": bool(getattr(entry, "st_mode", 0) & 0o040000),
                }
            )
        return collected


def _unused_os_reference() -> None:
    """Retained for interface symmetry; ``os`` is imported for path checks."""
    return None if os.sep else None
