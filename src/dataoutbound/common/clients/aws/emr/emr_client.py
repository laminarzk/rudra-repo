"""Typed EMR Serverless client for Spark job submission and polling."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Iterable, Optional

from botocore.exceptions import BotoCoreError
from botocore.exceptions import ClientError as BotoClientError

from dataoutbound.common.clients.aws.session import get_client
from dataoutbound.common.logging.logger import get_logger
from dataoutbound.config.config_loader import ConfigLoader
from dataoutbound.util.exceptions import ClientError

logger = get_logger(__name__)

DEFAULT_POLL_SECONDS = 30
DEFAULT_TIMEOUT_SECONDS = 3600


class EmrJobState(str, Enum):
    """Typed EMR Serverless job run states."""

    SUBMITTED = "SUBMITTED"
    PENDING = "PENDING"
    SCHEDULED = "SCHEDULED"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def from_raw(cls, value: Optional[str]) -> "EmrJobState":
        """Map a raw API string onto a typed state, defaulting to UNKNOWN."""
        if not value:
            return cls.UNKNOWN
        try:
            return cls(str(value).upper())
        except ValueError:
            return cls.UNKNOWN


TERMINAL_JOB_STATES = frozenset(
    {EmrJobState.SUCCESS, EmrJobState.FAILED, EmrJobState.CANCELLED}
)


@dataclass
class EmrJobRunInfo:
    """Normalised view of an EMR Serverless job run."""

    application_id: str
    job_run_id: str
    state: EmrJobState
    name: Optional[str] = None
    state_details: Optional[str] = None
    raw: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_terminal(self) -> bool:
        return self.state in TERMINAL_JOB_STATES

    @property
    def is_success(self) -> bool:
        return self.state is EmrJobState.SUCCESS


class EmrClient:
    """Submit, poll and cancel EMR Serverless job runs."""

    def __init__(
        self,
        config: Optional[ConfigLoader] = None,
        *,
        client: Any = None,
    ) -> None:
        self._config = config or ConfigLoader.load_config("common")
        self._client = client

    @property
    def client(self) -> Any:
        if self._client is None:
            self._client = get_client("emr-serverless", config=self._config)
        return self._client

    # -- submission --------------------------------------------------------
    def start_job_run(
        self,
        application_id: str,
        *,
        name: str,
        execution_role_arn: str,
        job_driver: Dict[str, Any],
        configuration_overrides: Optional[Dict[str, Any]] = None,
        tags: Optional[Dict[str, str]] = None,
    ) -> str:
        """Start a job run and return its ``jobRunId``."""
        kwargs: Dict[str, Any] = {
            "applicationId": application_id,
            "name": name,
            "executionRoleArn": execution_role_arn,
            "jobDriver": job_driver,
        }
        if configuration_overrides:
            kwargs["configurationOverrides"] = configuration_overrides
        if tags:
            kwargs["tags"] = {str(k): str(v) for k, v in tags.items()}

        try:
            response = self.client.start_job_run(**kwargs)
        except BotoClientError as exc:
            raise ClientError(
                f"EMR start_job_run failed for application '{application_id}': {exc}"
            ) from exc
        except BotoCoreError as exc:
            raise ClientError(
                f"EMR start_job_run failed for application '{application_id}': {exc}"
            ) from exc

        job_run_id = str(response.get("jobRunId", ""))
        logger.info(
            "EMR job run started",
            extra={
                "application_id": application_id,
                "job_run_id": job_run_id,
                "name": name,
            },
        )
        return job_run_id

    # -- inspection --------------------------------------------------------
    def get_job_run(self, application_id: str, job_run_id: str) -> EmrJobRunInfo:
        """Return the normalised state of a single job run."""
        try:
            response = self.client.get_job_run(
                applicationId=application_id,
                jobRunId=job_run_id,
            )
        except BotoClientError as exc:
            raise ClientError(
                f"EMR get_job_run failed for '{job_run_id}': {exc}"
            ) from exc
        except BotoCoreError as exc:
            raise ClientError(
                f"EMR get_job_run failed for '{job_run_id}': {exc}"
            ) from exc

        job_run = response.get("jobRun", {}) or {}
        return EmrJobRunInfo(
            application_id=str(job_run.get("applicationId", application_id)),
            job_run_id=str(job_run.get("jobRunId", job_run_id)),
            state=EmrJobState.from_raw(job_run.get("state")),
            name=job_run.get("name"),
            state_details=job_run.get("stateDetails"),
            raw=job_run,
        )

    def wait_for_job_run(
        self,
        application_id: str,
        job_run_id: str,
        *,
        poll_seconds: int = DEFAULT_POLL_SECONDS,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    ) -> EmrJobRunInfo:
        """Poll until the job run reaches a terminal state or the wait times out."""
        deadline = time.monotonic() + max(1, int(timeout_seconds))
        interval = max(1, int(poll_seconds))
        while True:
            info = self.get_job_run(application_id, job_run_id)
            if info.is_terminal:
                return info
            if time.monotonic() >= deadline:
                logger.warning(
                    "Timed out waiting for EMR job run",
                    extra={"job_run_id": job_run_id, "state": info.state.value},
                )
                return info
            time.sleep(interval)

    # -- cancellation ------------------------------------------------------
    def cancel_job_run(self, application_id: str, job_run_id: str) -> None:
        """Request cancellation of a running job run."""
        try:
            self.client.cancel_job_run(
                applicationId=application_id,
                jobRunId=job_run_id,
            )
        except BotoClientError as exc:
            raise ClientError(
                f"EMR cancel_job_run failed for '{job_run_id}': {exc}"
            ) from exc
        except BotoCoreError as exc:
            raise ClientError(
                f"EMR cancel_job_run failed for '{job_run_id}': {exc}"
            ) from exc
        logger.info(
            "EMR job run cancellation requested",
            extra={"application_id": application_id, "job_run_id": job_run_id},
        )

    def list_job_runs(
        self,
        application_id: str,
        *,
        states: Optional[Iterable[EmrJobState]] = None,
    ) -> Dict[str, Any]:
        """List job runs for an application, optionally filtered by state."""
        kwargs: Dict[str, Any] = {"applicationId": application_id}
        if states:
            kwargs["states"] = [state.value for state in states]
        try:
            return dict(self.client.list_job_runs(**kwargs))
        except BotoClientError as exc:
            raise ClientError(
                f"EMR list_job_runs failed for application '{application_id}': {exc}"
            ) from exc
        except BotoCoreError as exc:
            raise ClientError(
                f"EMR list_job_runs failed for application '{application_id}': {exc}"
            ) from exc
