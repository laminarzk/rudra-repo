"""Typed MWAA control client built on the Airflow REST API."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from botocore.exceptions import BotoCoreError
from botocore.exceptions import ClientError as BotoClientError

from dataoutbound.common.clients.aws.session import get_client
from dataoutbound.common.clients.rest.rest_client import RestClient
from dataoutbound.common.logging.logger import get_logger
from dataoutbound.config.config_loader import ConfigLoader
from dataoutbound.util.exceptions import ClientError, ConfigError

logger = get_logger(__name__)

DAG_RUNS_PATH = "/api/v1/dags/{dag_id}/dagRuns"
DAG_RUN_PATH = "/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}"


class MwaaClient:
    """Trigger, inspect and cancel MWAA (Airflow) DAG runs."""

    def __init__(
        self,
        config: Optional[ConfigLoader] = None,
        *,
        client: Any = None,
        environment_name: Optional[str] = None,
    ) -> None:
        self._config = config or ConfigLoader.load_config("dags")
        self._client = client
        self._environment_name = environment_name or self._resolve_environment_name()

    # -- configuration -----------------------------------------------------
    def _resolve_environment_name(self) -> str:
        configured = self._config.get("mwaa.environment_name")
        if not configured:
            raise ConfigError(
                "Missing required configuration value: mwaa.environment_name"
            )
        return str(configured)

    @property
    def client(self) -> Any:
        if self._client is None:
            self._client = get_client("mwaa", config=self._config)
        return self._client

    def _rest_client(self) -> RestClient:
        """Build a one-shot Airflow REST client using a fresh CLI token."""
        try:
            token_response = self.client.create_cli_token(Name=self._environment_name)
        except BotoClientError as exc:
            raise ClientError(
                f"MWAA create_cli_token failed for '{self._environment_name}': {exc}"
            ) from exc
        except BotoCoreError as exc:
            raise ClientError(
                f"MWAA create_cli_token failed for '{self._environment_name}': {exc}"
            ) from exc

        hostname = str(token_response.get("WebServerHostname", ""))
        token = str(token_response.get("CliToken", ""))
        if not hostname or not token:
            raise ClientError("MWAA create_cli_token returned an incomplete response")

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        return RestClient(
            config=self._config,
            base_url=f"https://{hostname}",
            default_headers=headers,
        )

    # -- DAG runs ----------------------------------------------------------
    def trigger_dag_run(
        self,
        dag_id: str,
        conf: Optional[Dict[str, Any]] = None,
        *,
        dag_run_id: Optional[str] = None,
        logical_date: Optional[str] = None,
        note: Optional[str] = None,
    ) -> str:
        """Trigger a DAG run and return its ``dag_run_id``."""
        payload: Dict[str, Any] = {"conf": conf or {}}
        if dag_run_id:
            payload["dag_run_id"] = dag_run_id
        if logical_date:
            payload["logical_date"] = logical_date
        if note:
            payload["note"] = note

        rest = self._rest_client()
        response = rest.post(DAG_RUNS_PATH.format(dag_id=dag_id), json=payload)
        if not isinstance(response, dict):
            raise ClientError(
                f"Unexpected MWAA response when triggering DAG '{dag_id}'"
            )

        resolved_run_id = str(response.get("dag_run_id", dag_run_id or ""))
        logger.info(
            "Triggered MWAA DAG run",
            extra={"dag_id": dag_id, "dag_run_id": resolved_run_id},
        )
        return resolved_run_id

    def get_dag_run(self, dag_id: str, dag_run_id: str) -> Dict[str, Any]:
        """Return the raw state object for a single DAG run."""
        rest = self._rest_client()
        response = rest.get(DAG_RUN_PATH.format(dag_id=dag_id, dag_run_id=dag_run_id))
        if not isinstance(response, dict):
            raise ClientError(
                f"Unexpected MWAA response when reading DAG run '{dag_run_id}'"
            )
        return response

    def cancel_dag_run(self, dag_id: str, dag_run_id: str) -> None:
        """Request cancellation of a running DAG run."""
        rest = self._rest_client()
        rest.patch(
            DAG_RUN_PATH.format(dag_id=dag_id, dag_run_id=dag_run_id),
            json={"state": "failed"},
        )
        logger.info(
            "Requested MWAA DAG run cancellation",
            extra={"dag_id": dag_id, "dag_run_id": dag_run_id},
        )

    def list_dag_runs(
        self,
        dag_id: str,
        *,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Return recent DAG runs for a DAG, newest first."""
        rest = self._rest_client()
        response = rest.get(
            DAG_RUNS_PATH.format(dag_id=dag_id),
            params={"limit": int(limit), "order_by": "-execution_date"},
        )
        if isinstance(response, dict):
            runs = response.get("dag_runs", [])
            return list(runs) if isinstance(runs, list) else []
        return []
