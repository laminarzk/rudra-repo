"""Business operations behind the Export Admin API routes.

Contains no HTTP types: routes translate the domain exceptions raised here into
HTTP status codes and the standard error envelope.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional

from cryptography.fernet import Fernet, InvalidToken

from dataoutbound.apis.export_admin_api.export_admin_schemas import (
    ExportConfigCreateRequest,
    ExportConfigResponse,
    ExportConfigUpdateRequest,
)
from dataoutbound.common.clients.aws.mwaa.mwaa_client import MwaaClient
from dataoutbound.common.clients.db.db_client import DbClient
from dataoutbound.common.logging.logger import get_logger
from dataoutbound.common.telemetry.auditor import AuditPublisher
from dataoutbound.util.exceptions import (
    ClientError,
    ConfigError,
    ConflictError,
    NotFoundError,
    ValidationError,
)

logger = get_logger(__name__)

ORCHESTRATOR_DAG_ID = "orchestrator"
_ISO_MILLIS = "%Y-%m-%dT%H:%M:%S.%f"


class CredentialCipher:
    """Symmetric protection for stored target credentials.

    Credentials are serialised to canonical JSON and encrypted with Fernet. The
    resulting token is the only representation that ever reaches the Export
    Config Store; plaintext is never returned to callers.
    """

    MASK = "********"

    def __init__(self, key_material: Optional[str]) -> None:
        if not key_material:
            logger.warning(
                "No credential encryption key configured; using an ephemeral key",
                extra={"component": "export_admin_api"},
            )
            key_material = Fernet.generate_key().decode("utf-8")
        try:
            self._fernet = Fernet(key_material.encode("utf-8"))
        except (ValueError, TypeError) as exc:
            raise ConfigError(f"Invalid credential encryption key: {exc}") from exc

    def encrypt(self, credentials: Mapping[str, Any]) -> str:
        """Serialise and encrypt a credential mapping into an opaque token."""
        serialized = json.dumps(
            dict(credentials), sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return self._fernet.encrypt(serialized).decode("utf-8")

    def decrypt(self, token: str) -> Dict[str, Any]:
        """Decrypt a stored credential token back into a mapping."""
        try:
            raw = self._fernet.decrypt(token.encode("utf-8"))
        except (InvalidToken, ValueError, TypeError) as exc:
            raise ClientError("Stored credential value could not be decrypted") from exc
        return json.loads(raw.decode("utf-8"))

    def masked(self, token: Optional[str]) -> Dict[str, str]:
        """Return the credential field names with every value masked."""
        if not token:
            return {}
        try:
            fields = self.decrypt(token)
        except ClientError:
            return {"credentials": self.MASK}
        return {name: self.MASK for name in fields.keys()}


def _first_row(rows: Any) -> Optional[Dict[str, Any]]:
    """Return the first row of a query result as a mapping when present."""
    if not rows:
        return None
    first = rows[0]
    if isinstance(first, Mapping):
        return dict(first)
    return None


def _is_unique_violation(exc: Exception) -> bool:
    """Best-effort detection of a database unique-constraint violation."""
    message = str(exc).lower()
    return "unique" in message or "duplicate key" in message


def _format_utc(value: Any) -> str:
    """Render a timestamp as ISO-8601 UTC with millisecond precision and ``Z``."""
    if isinstance(value, datetime):
        moment = value
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=timezone.utc)
        moment = moment.astimezone(timezone.utc)
        return moment.strftime(_ISO_MILLIS)[:-3] + "Z"
    if isinstance(value, str):
        return value
    return str(value)


class ExportAdminService:
    """CRUD and run-control operations over exported customer configurations."""

    TABLE = "export_configurations"
    _SELECT_COLUMNS = (
        "id, connection_name, target_type, dataset, mode, cron_schedule, "
        "encrypted_credentials, is_active, created_at, updated_at"
    )

    def __init__(
        self,
        db_client: DbClient,
        mwaa_client: MwaaClient,
        audit_publisher: AuditPublisher,
        config: Optional[Mapping[str, Any]] = None,
    ) -> None:
        self._db = db_client
        self._mwaa = mwaa_client
        self._audit = audit_publisher
        self._config: Dict[str, Any] = dict(config or {})
        self._cipher = CredentialCipher(self._config.get("credential_encryption_key"))
        self._orchestrator_dag_id = str(
            self._config.get("orchestrator_dag_id", ORCHESTRATOR_DAG_ID)
        )

    # -- CRUD -----------------------------------------------------------------

    def create_configuration(
        self,
        payload: ExportConfigCreateRequest,
        actor_id: str,
    ) -> ExportConfigResponse:
        """Persist a new export configuration with encrypted credentials."""
        logger.info(
            "Persisting export configuration",
            extra={
                "actor_id": actor_id,
                "connection_name": payload.connection_name,
                "target_type": payload.target_type,
                "dataset": payload.dataset,
            },
        )
        encrypted = self._cipher.encrypt(payload.credentials)
        now = datetime.now(timezone.utc)
        params: Dict[str, Any] = {
            "connection_name": payload.connection_name,
            "target_type": payload.target_type,
            "dataset": payload.dataset,
            "mode": payload.mode,
            "cron_schedule": payload.cron_schedule,
            "encrypted_credentials": encrypted,
            "is_active": bool(payload.is_active),
            "created_at": now,
            "updated_at": now,
        }
        sql = (
            f"INSERT INTO {self.TABLE} "
            "(connection_name, target_type, dataset, mode, cron_schedule, "
            "encrypted_credentials, is_active, created_at, updated_at) "
            "VALUES "
            "(%(connection_name)s, %(target_type)s, %(dataset)s, %(mode)s, "
            "%(cron_schedule)s, %(encrypted_credentials)s, %(is_active)s, "
            "%(created_at)s, %(updated_at)s) "
            f"RETURNING {self._SELECT_COLUMNS}"
        )
        try:
            rows = self._db.execute_query(sql, params)
        except Exception as exc:
            if _is_unique_violation(exc):
                raise ConflictError(
                    f"connection_name '{payload.connection_name}' already exists"
                ) from exc
            raise ClientError(f"Failed to persist export configuration: {exc}") from exc

        row = _first_row(rows)
        if row is None:
            raise ClientError("Export configuration insert returned no row")
        return self._to_response(row)

    def get_configuration(self, config_id: int) -> ExportConfigResponse:
        """Return a single configuration with credentials masked."""
        row = self._load_row(config_id)
        return self._to_response(row)

    def update_configuration(
        self,
        config_id: int,
        payload: ExportConfigUpdateRequest,
        actor_id: str,
    ) -> ExportConfigResponse:
        """Apply a partial update to an existing configuration."""
        self._load_row(config_id)

        updates: Dict[str, Any] = {}
        if payload.connection_name is not None:
            updates["connection_name"] = payload.connection_name
        if payload.target_type is not None:
            updates["target_type"] = payload.target_type
        if payload.dataset is not None:
            updates["dataset"] = payload.dataset
        if payload.mode is not None:
            updates["mode"] = payload.mode
        if payload.cron_schedule is not None:
            updates["cron_schedule"] = payload.cron_schedule
        if payload.credentials is not None:
            updates["encrypted_credentials"] = self._cipher.encrypt(payload.credentials)
        if payload.is_active is not None:
            updates["is_active"] = bool(payload.is_active)

        if not updates:
            raise ValidationError("At least one field must be provided for update")

        updates["updated_at"] = datetime.now(timezone.utc)
        assignments = ", ".join(f"{column} = %({column})s" for column in updates)
        params: Dict[str, Any] = {**updates, "config_id": config_id}
        sql = (
            f"UPDATE {self.TABLE} SET {assignments} "
            "WHERE id = %(config_id)s "
            f"RETURNING {self._SELECT_COLUMNS}"
        )

        logger.info(
            "Updating export configuration",
            extra={"actor_id": actor_id, "config_id": config_id},
        )
        try:
            rows = self._db.execute_query(sql, params)
        except Exception as exc:
            if _is_unique_violation(exc):
                raise ConflictError(
                    "connection_name already exists on another configuration"
                ) from exc
            raise ClientError(f"Failed to update export configuration: {exc}") from exc

        row = _first_row(rows)
        if row is None:
            raise NotFoundError(f"Export configuration {config_id} was not found")
        return self._to_response(row)

    def delete_configuration(self, config_id: int) -> bool:
        """Delete a configuration; returns ``True`` when a row was removed."""
        sql = f"DELETE FROM {self.TABLE} WHERE id = %(config_id)s RETURNING id"
        try:
            rows = self._db.execute_query(sql, {"config_id": config_id})
        except Exception as exc:
            raise ClientError(f"Failed to delete export configuration: {exc}") from exc
        return _first_row(rows) is not None

    # -- Run control ----------------------------------------------------------

    def trigger_run(self, config_id: int, actor_id: str) -> Dict[str, str]:
        """Trigger a manual Export Orchestrator run for one configuration."""
        row = self._load_row(config_id)
        if not bool(row.get("is_active", False)):
            raise ValidationError(f"Export configuration {config_id} is not active")

        execution_id = str(uuid.uuid4())
        trigger_payload: Dict[str, Any] = {
            "lineage_uuid": execution_id,
            "trigger_source": "MANUAL",
            "actor_id": actor_id,
            "connection_id": config_id,
            "connection": self._connection_json(row),
        }
        api_token = str(self._config.get("mwaa_api_token", ""))

        logger.info(
            "Triggering orchestrator run",
            extra={
                "actor_id": actor_id,
                "config_id": config_id,
                "execution_id": execution_id,
                "dag_id": self._orchestrator_dag_id,
            },
        )
        try:
            run_id = self._mwaa.trigger_dag_run(
                dag_id=self._orchestrator_dag_id,
                conf=trigger_payload,
                api_token=api_token,
            )
        except Exception as exc:
            raise ClientError(f"Failed to trigger export run: {exc}") from exc

        if not run_id:
            raise ClientError("Orchestrator trigger returned no run identifier")

        return {"run_id": str(run_id), "execution_id": execution_id}

    def stop_run(
        self,
        config_id: int,
        actor_id: str,
        run_id: Optional[str] = None,
    ) -> Dict[str, Optional[str]]:
        """Request termination of a running export execution."""
        self._load_row(config_id)

        target_run_id = run_id
        if target_run_id is None:
            target_run_id = self._find_active_run_id(config_id)

        if target_run_id is None:
            raise NotFoundError(
                f"No active export run found for configuration {config_id}"
            )

        logger.info(
            "Requesting orchestrator run termination",
            extra={
                "actor_id": actor_id,
                "config_id": config_id,
                "run_id": target_run_id,
            },
        )
        try:
            self._mwaa.cancel_dag_run(
                dag_id=self._orchestrator_dag_id,
                dag_run_id=str(target_run_id),
            )
        except Exception as exc:
            raise ClientError(f"Failed to stop export run: {exc}") from exc

        return {"run_id": str(target_run_id)}

    # -- Internals ------------------------------------------------------------

    def _load_row(self, config_id: int) -> Dict[str, Any]:
        sql = (
            f"SELECT {self._SELECT_COLUMNS} FROM {self.TABLE} WHERE id = %(config_id)s"
        )
        try:
            rows = self._db.execute_query(sql, {"config_id": config_id})
        except Exception as exc:
            raise ClientError(f"Failed to load export configuration: {exc}") from exc

        row = _first_row(rows)
        if row is None:
            raise NotFoundError(f"Export configuration {config_id} was not found")
        return row

    def _find_active_run_id(self, config_id: int) -> Optional[str]:
        """Locate the newest running orchestrator run for a configuration."""
        try:
            runs: List[Dict[str, Any]] = list(
                self._mwaa.list_dag_runs(
                    dag_id=self._orchestrator_dag_id,
                    state="running",
                )
            )
        except Exception as exc:
            raise ClientError(f"Failed to list orchestrator runs: {exc}") from exc

        for run in runs:
            conf = run.get("conf") or {}
            if conf.get("connection_id") == config_id:
                candidate = run.get("dag_run_id") or run.get("run_id")
                if candidate:
                    return str(candidate)
        return None

    def _connection_json(self, row: Mapping[str, Any]) -> Dict[str, Any]:
        """Build the connection JSON contract consumed by the orchestrator."""
        return {
            "connection_id": row.get("id"),
            "connection_name": row.get("connection_name"),
            "source_dataset": row.get("dataset"),
            "destination_type": row.get("target_type"),
            "extraction_mode": row.get("mode"),
            "cron_schedule": row.get("cron_schedule"),
            "is_active": bool(row.get("is_active", False)),
        }

    def _to_response(self, row: Mapping[str, Any]) -> ExportConfigResponse:
        encrypted = row.get("encrypted_credentials")
        masked = (
            self._cipher.masked(encrypted)
            if isinstance(encrypted, str) and encrypted
            else None
        )
        return ExportConfigResponse(
            id=int(row["id"]),
            connection_name=row["connection_name"],
            target_type=row["target_type"],
            dataset=row["dataset"],
            mode=row["mode"],
            cron_schedule=row["cron_schedule"],
            is_active=bool(row.get("is_active", False)),
            created_at=_format_utc(row.get("created_at")),
            updated_at=_format_utc(row.get("updated_at")),
            credentials_masked=masked,
        )
