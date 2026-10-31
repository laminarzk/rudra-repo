"""Relational database client with pooled connections and transactions.

All SQL must be parameterised; the client never interpolates caller-supplied
values into statements. Use the mapping form ``:name`` (SQLAlchemy ``text``)
or pass tuples to ``executemany``.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError, SQLAlchemyError
from sqlalchemy.pool import QueuePool

from dataoutbound.common.logging.logger import get_logger
from dataoutbound.config.config_loader import ConfigLoader
from dataoutbound.util.exceptions import ClientError, ConflictError, ConfigError

logger = get_logger(__name__)

DEFAULT_POOL_SIZE = 5
DEFAULT_MAX_OVERFLOW = 10
DEFAULT_POOL_RECYCLE_SECONDS = 1800


def _row_to_dict(row: Any) -> Dict[str, Any]:
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return {str(key): value for key, value in mapping.items()}
    if isinstance(row, Mapping):
        return {str(key): value for key, value in row.items()}
    return dict(row)


class DbClient:
    """Execute parameterised statements against the configured RDS instance.

    A single SQLAlchemy engine is created per client instance and pooled, so
    callers should hold one client per process (the API wires it through its
    dependency provider and DAGs construct it once per task callable).
    """

    def __init__(
        self,
        config: Optional[ConfigLoader] = None,
        *,
        engine: Optional[Engine] = None,
        url: Optional[str] = None,
    ) -> None:
        self._config = config or ConfigLoader.load_config("common")
        self._engine = engine
        self._url = url

    # -- engine construction ----------------------------------------------
    def _resolve_url(self) -> str:
        if self._url:
            return self._url

        configured = self._config.get_str("database.url")
        if configured:
            return configured

        host = self._config.require("database.host")
        port = self._config.get_int("database.port", 5432)
        name = self._config.require("database.name")
        user = self._config.require("database.user")
        password = self._config.get_secret("database.password_secret")
        if not password:
            raise ConfigError("Missing required database password secret")

        sslmode = self._config.get_str("database.sslmode", "require")
        return (
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"
            f"?sslmode={sslmode}"
        )

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            try:
                self._engine = create_engine(
                    self._resolve_url(),
                    poolclass=QueuePool,
                    pool_size=self._config.get_int(
                        "database.pool_size", DEFAULT_POOL_SIZE
                    ),
                    max_overflow=self._config.get_int(
                        "database.max_overflow", DEFAULT_MAX_OVERFLOW
                    ),
                    pool_recycle=self._config.get_int(
                        "database.pool_recycle_seconds", DEFAULT_POOL_RECYCLE_SECONDS
                    ),
                    pool_pre_ping=True,
                    future=True,
                )
            except ConfigError:
                raise
            except SQLAlchemyError as exc:
                raise ClientError(f"Failed to create database engine: {exc}") from exc
        return self._engine

    def dispose(self) -> None:
        """Close every pooled connection held by the engine."""
        if self._engine is not None:
            self._engine.dispose()
            logger.debug("Database engine disposed")

    # -- transactions ------------------------------------------------------
    @contextmanager
    def transaction(self) -> Iterator[Any]:
        """Yield a connection inside a transaction, rolling back on error."""
        try:
            connection = self.engine.connect()
        except SQLAlchemyError as exc:
            raise ClientError(f"Failed to acquire database connection: {exc}") from exc

        transaction = connection.begin()
        try:
            yield connection
            transaction.commit()
        except ConflictError:
            transaction.rollback()
            raise
        except SQLAlchemyError as exc:
            transaction.rollback()
            raise ClientError(f"Database transaction failed: {exc}") from exc
        finally:
            connection.close()

    # -- statement execution ----------------------------------------------
    def execute(
        self,
        statement: str,
        params: Optional[Mapping[str, Any]] = None,
    ) -> int:
        """Run a DML statement and return the affected row count.

        ``statement`` must be a static string with named bind parameters;
        callers must never format untrusted values into it.
        """
        try:
            with self.transaction() as connection:
                result = connection.execute(text(statement), dict(params or {}))
                return int(result.rowcount or 0)
        except ClientError:
            raise
        except DBAPIError as exc:
            raise ClientError(f"Database statement failed: {exc}") from exc
        except SQLAlchemyError as exc:
            raise ClientError(f"Database statement failed: {exc}") from exc

    def execute_many(
        self,
        statement: str,
        param_sets: Sequence[Mapping[str, Any]],
    ) -> int:
        """Run a DML statement once per parameter mapping in one transaction."""
        payload = [dict(params) for params in param_sets]
        if not payload:
            return 0
        try:
            with self.transaction() as connection:
                result = connection.execute(text(statement), payload)
                return int(result.rowcount or 0)
        except ClientError:
            raise
        except DBAPIError as exc:
            raise ClientError(f"Database batch statement failed: {exc}") from exc
        except SQLAlchemyError as exc:
            raise ClientError(f"Database batch statement failed: {exc}") from exc

    def fetch_all(
        self,
        statement: str,
        params: Optional[Mapping[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Return every row of a query as a list of dictionaries."""
        try:
            with self.transaction() as connection:
                result = connection.execute(text(statement), dict(params or {}))
                return [_row_to_dict(row) for row in result.fetchall()]
        except ClientError:
            raise
        except DBAPIError as exc:
            raise ClientError(f"Database query failed: {exc}") from exc
        except SQLAlchemyError as exc:
            raise ClientError(f"Database query failed: {exc}") from exc

    def fetch_one(
        self,
        statement: str,
        params: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Return the first row of a query or ``None`` when there are none."""
        try:
            with self.transaction() as connection:
                result = connection.execute(text(statement), dict(params or {}))
                row = result.fetchone()
        except ClientError:
            raise
        except DBAPIError as exc:
            raise ClientError(f"Database query failed: {exc}") from exc
        except SQLAlchemyError as exc:
            raise ClientError(f"Database query failed: {exc}") from exc
        return _row_to_dict(row) if row is not None else None

    def healthcheck(self) -> bool:
        """Return ``True`` when a trivial query succeeds against the database."""
        try:
            self.fetch_one("SELECT 1 AS ok")
            return True
        except ClientError:
            logger.error("Database healthcheck failed", exc_info=True)
            return False

    # -- convenience -------------------------------------------------------
    def insert_returning_id(
        self,
        statement: str,
        params: Optional[Mapping[str, Any]] = None,
    ) -> Optional[int]:
        """Run an ``INSERT ... RETURNING id`` statement and return the new id."""
        row = self.fetch_one(statement, params)
        if not row:
            return None
        value = row.get("id")
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    def chunk_fetch(
        self,
        statement: str,
        params: Optional[Mapping[str, Any]] = None,
        *,
        chunk_size: int = 1000,
    ) -> Iterator[List[Dict[str, Any]]]:
        """Yield query results in chunks to bound memory usage."""
        if chunk_size <= 0:
            raise ConfigError("chunk_size must be a positive integer")
        rows = self.fetch_all(statement, params)
        for start in range(0, len(rows), chunk_size):
            yield rows[start : start + chunk_size]

    def _unused_iterable(self, value: Iterable[Any]) -> None:
        """Kept private; exists only to document that Iterable is imported."""
        for _ in value:
            return
