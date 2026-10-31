"""Child Export DAG for a single export connection.

Validates the orchestrator-supplied run configuration, resolves the incremental
watermark for the connection, submits the EMR Serverless Spark extract job and
records the execution outcome in the audit store.
"""

import hashlib
import re
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional

from airflow import DAG
from airflow.operators.python import PythonOperator

from dataoutbound.common.clients.aws.emr.emr_client import EmrClient
from dataoutbound.common.clients.db.db_client import DbClient
from dataoutbound.common.logging.logger import get_logger
from dataoutbound.common.telemetry.auditor import AuditPublisher
from dataoutbound.config.config_loader import ConfigLoader
from dataoutbound.util.exceptions import ClientError, ConfigError, ValidationError

logger = get_logger(__name__)

DAG_ID = "child_export_dag"
CONFIG_SECTION = "child_export_dag"
ACTOR_ID = "system:child_export_dag"

TASK_READ_CONFIG = "read_connection_config"
TASK_SUBMIT_EMR = "submit_emr_extract_job"
TASK_RECORD_OUTCOME = "record_execution_outcome"

XCOM_JOB_ARGUMENTS = "job_arguments"
XCOM_JOB_RUN_ID = "emr_job_run_id"

EVENT_EXECUTION_STARTED = "EXECUTION_STARTED"
EVENT_EXECUTION_COMPLETED = "EXECUTION_COMPLETED"
EVENT_EXECUTION_FAILED = "EXECUTION_FAILED"
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
TASK_STATE_SUCCESS = "success"
FIRST_TRY_NUMBER = 1
FINALIZE_RETRIES = 0

# Parse-time defaults: Airflow parses this file frequently, so DAG-level
# arguments must be static. Per-environment values from dags/config.yml are
# resolved lazily inside task callables (see _get_settings).
DEFAULT_SCHEDULE = "@daily"
DEFAULT_RETRIES = 3
DEFAULT_RETRY_DELAY_MINUTES = 5
DEFAULT_SLA_MINUTES = 1440

EXTRACTION_MODE_TO_EMR_MODE: Dict[str, str] = {
    "initial": "FULL",
    "incremental": "INCREMENTAL",
}
REQUIRED_CONNECTION_KEYS = ("connection_id", "source_dataset", "extraction_mode")
UUID_V4_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.IGNORECASE,
)
WATERMARK_QUERY = """
    SELECT last_watermark_timestamp
    FROM export_watermarks
    WHERE connection_id = :connection_id
    ORDER BY updated_at DESC
    LIMIT 1
"""


def _require_setting(section: Dict[str, Any], key: str) -> Any:
    """Return a required setting from the DAG config section.

    Args:
        section: The per-DAG configuration section.
        key: The setting name to read.

    Returns:
        The configured value.

    Raises:
        ConfigError: If the setting is absent or empty.
    """
    value = section.get(key)
    if value is None or value == "":
        raise ConfigError(
            f"Missing required setting '{key}' in section '{CONFIG_SECTION}'"
        )
    return value


def _load_settings() -> Dict[str, Any]:
    """Load runtime settings for this DAG through ConfigLoader.

    Returns:
        A dictionary of typed settings for this DAG.

    Raises:
        ConfigError: If the section or any required setting is missing.
    """
    config = ConfigLoader.load_config("dags")
    section = config.get(CONFIG_SECTION)
    if not isinstance(section, dict):
        raise ConfigError(
            f"Configuration section '{CONFIG_SECTION}' is missing from dags/config.yml"
        )
    return {
        "schedule": _require_setting(section, "schedule"),
        "retries": int(_require_setting(section, "retries")),
        "retry_delay_minutes": int(_require_setting(section, "retry_delay_minutes")),
        "sla_minutes": int(_require_setting(section, "sla_minutes")),
        "environment": str(_require_setting(section, "environment")),
        "emr_application_id": str(_require_setting(section, "emr_application_id")),
        "emr_execution_role_arn": str(
            _require_setting(section, "emr_execution_role_arn")
        ),
        "spark_entry_point_uri": str(
            _require_setting(section, "spark_entry_point_uri")
        ),
    }


@lru_cache(maxsize=1)
def _get_settings() -> Dict[str, Any]:
    """Return the cached runtime settings, loading them on first use inside a task.

    Returns:
        The typed settings dictionary for this DAG.

    Raises:
        ConfigError: If the configuration is missing or invalid.
    """
    return _load_settings()


def _utc_now_ms() -> str:
    """Return the current UTC time as ISO-8601 with millisecond precision and Z suffix.

    Returns:
        A timestamp such as ``2025-01-31T02:15:00.000Z``.
    """
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S") + f".{now.microsecond // 1000:03d}Z"


def _publish_event(event_type: str, execution_id: str, payload: Dict[str, Any]) -> None:
    """Publish one business audit event for this execution.

    Args:
        event_type: One of the enumerated audit event types.
        execution_id: The lineage UUID of the execution.
        payload: Additional event fields to include in the record.
    """
    record: Dict[str, Any] = {
        "actor_id": ACTOR_ID,
        "timestamp_utc": _utc_now_ms(),
        "event_type": event_type,
        "execution_id": execution_id,
    }
    record.update(payload)
    AuditPublisher().publish(event_type, record)
    logger.info(
        "Audit event published",
        extra={"event_type": event_type, "execution_id": execution_id},
    )


def _run_conf(context: Dict[str, Any]) -> Dict[str, Any]:
    """Return the DAG run configuration as a dictionary.

    Args:
        context: The Airflow task context.

    Returns:
        The run conf, or an empty dictionary when none was supplied.

    Raises:
        ValidationError: If the run conf is not a JSON object.
    """
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run is not None and dag_run.conf else {}
    if not isinstance(conf, dict):
        raise ValidationError("DAG run conf must be a JSON object")
    return conf


def _parse_lineage_uuid(run_conf: Dict[str, Any]) -> str:
    """Validate and return the RFC 4122 v4 lineage UUID from the run conf.

    Args:
        run_conf: The DAG run configuration.

    Returns:
        The lineage UUID string.

    Raises:
        ValidationError: If the lineage UUID is missing or not version 4.
    """
    lineage = run_conf.get("lineage_uuid")
    if not isinstance(lineage, str) or not UUID_V4_PATTERN.match(lineage):
        raise ValidationError("lineage_uuid must be an RFC 4122 version 4 UUID")
    return lineage


def _parse_connection(run_conf: Dict[str, Any]) -> Dict[str, Any]:
    """Validate and return the connection payload for this child run.

    Accepts the ``connection`` key from the design schema and the
    ``connection_conf`` key named in the edge contract.

    Args:
        run_conf: The DAG run configuration.

    Returns:
        The connection dictionary.

    Raises:
        ValidationError: If the payload is absent, incomplete or unsupported.
    """
    connection = run_conf.get("connection") or run_conf.get("connection_conf")
    if not isinstance(connection, dict):
        raise ValidationError("Run conf must include a 'connection' object")
    missing = [key for key in REQUIRED_CONNECTION_KEYS if not connection.get(key)]
    if missing:
        raise ValidationError(f"Connection payload is missing required keys: {missing}")
    if connection["extraction_mode"] not in EXTRACTION_MODE_TO_EMR_MODE:
        raise ValidationError(
            f"Unsupported extraction_mode: {connection['extraction_mode']}"
        )
    return connection


def _fetch_watermark(connection_id: str) -> Optional[str]:
    """Read the last committed extraction watermark for a connection.

    Args:
        connection_id: The export connection identifier.

    Returns:
        The watermark as an ISO-8601 string, or None when no watermark exists.

    Raises:
        ClientError: If the watermark query fails.
    """
    db_client = DbClient(ConfigLoader.load_config("dags"))
    try:
        rows = db_client.execute_query(
            WATERMARK_QUERY, {"connection_id": connection_id}
        )
    except Exception as exc:
        raise ClientError(
            f"Failed to read watermark for connection {connection_id}: {exc}"
        ) from exc

    if not rows:
        logger.info(
            "No watermark found; incremental extract starts without a lower bound",
            extra={"connection_id": connection_id},
        )
        return None

    watermark = rows[0].get("last_watermark_timestamp")
    if watermark is None:
        return None
    if isinstance(watermark, datetime):
        return watermark.isoformat()
    return str(watermark)


def _build_job_arguments(
    lineage: str, connection: Dict[str, Any], watermark: Optional[str]
) -> Dict[str, Any]:
    """Build the job argument object passed from the read task to the submit task.

    Args:
        lineage: The validated lineage UUID.
        connection: The validated connection payload.
        watermark: The incremental watermark, or None for initial runs.

    Returns:
        The job argument dictionary.
    """
    return {
        "lineage_uuid": lineage,
        "connection_id": str(connection["connection_id"]),
        "environment": _get_settings()["environment"],
        "recipe_name": str(connection["source_dataset"]),
        "execution_mode": EXTRACTION_MODE_TO_EMR_MODE[connection["extraction_mode"]],
        "watermark_timestamp": watermark,
    }


def _to_cli_arguments(job_arguments: Dict[str, Any]) -> List[str]:
    """Convert the job argument object into Spark entrypoint CLI arguments.

    Args:
        job_arguments: The job argument dictionary.

    Returns:
        A flat list of CLI tokens for the Spark job entrypoint.
    """
    pairs = (
        ("--lineage-uuid", "lineage_uuid"),
        ("--environment", "environment"),
        ("--recipe-name", "recipe_name"),
        ("--execution-mode", "execution_mode"),
    )
    arguments: List[str] = []
    for flag, key in pairs:
        arguments.extend([flag, str(job_arguments[key])])
    watermark = job_arguments.get("watermark_timestamp")
    if watermark:
        arguments.extend(["--watermark-timestamp", str(watermark)])
    return arguments


def _client_token(lineage: str, connection_id: str) -> str:
    """Derive a deterministic EMR client token so task retries do not duplicate jobs.

    Args:
        lineage: The lineage UUID.
        connection_id: The export connection identifier.

    Returns:
        A 64-character hexadecimal token.
    """
    return hashlib.sha256(f"{lineage}:{connection_id}".encode("utf-8")).hexdigest()


def _build_emr_client() -> EmrClient:
    """Construct the EMR Serverless client from the shared dags configuration.

    Returns:
        A configured EmrClient instance.
    """
    return EmrClient(ConfigLoader.load_config("dags"))


def read_connection_config(**context: Any) -> Dict[str, Any]:
    """Validate the run conf, resolve the watermark and publish the start event.

    Args:
        **context: The Airflow task context.

    Returns:
        The job argument dictionary, also pushed to XCom.

    Raises:
        ValidationError: If the run conf is invalid.
        ClientError: If the watermark cannot be read.
    """
    run_conf = _run_conf(context)
    lineage = _parse_lineage_uuid(run_conf)
    connection = _parse_connection(run_conf)
    connection_id = str(connection["connection_id"])

    _publish_event(
        EVENT_EXECUTION_STARTED,
        lineage,
        {"task_id": TASK_READ_CONFIG, "connection_id": connection_id},
    )

    watermark: Optional[str] = None
    if connection["extraction_mode"] == "incremental":
        watermark = _fetch_watermark(connection_id)

    job_arguments = _build_job_arguments(lineage, connection, watermark)
    context["ti"].xcom_push(key=XCOM_JOB_ARGUMENTS, value=job_arguments)
    logger.info(
        "Resolved connection configuration",
        extra={"lineage_uuid": lineage, "connection_id": connection_id},
    )
    return job_arguments


def submit_emr_extract_job(**context: Any) -> str:
    """Submit the EMR Serverless extract job using an idempotent client token.

    Args:
        **context: The Airflow task context.

    Returns:
        The EMR job run identifier, also pushed to XCom.

    Raises:
        ValidationError: If the upstream job arguments are missing.
        ClientError: If the EMR job submission fails.
    """
    ti = context["ti"]
    job_arguments = ti.xcom_pull(key=XCOM_JOB_ARGUMENTS, task_ids=TASK_READ_CONFIG)
    if not isinstance(job_arguments, dict):
        raise ValidationError(
            "Job arguments were not produced by read_connection_config"
        )

    settings = _get_settings()
    lineage = str(job_arguments["lineage_uuid"])
    connection_id = str(job_arguments["connection_id"])
    client_token = _client_token(lineage, connection_id)

    try:
        job_run_id = _build_emr_client().start_job_run(
            application_id=settings["emr_application_id"],
            execution_role_arn=settings["emr_execution_role_arn"],
            entry_point_uri=settings["spark_entry_point_uri"],
            arguments=_to_cli_arguments(job_arguments),
            client_token=client_token,
        )
    except Exception as exc:
        raise ClientError(
            f"Failed to submit EMR job for connection {connection_id}: {exc}"
        ) from exc

    ti.xcom_push(key=XCOM_JOB_RUN_ID, value=job_run_id)
    logger.info(
        "Submitted EMR extract job",
        extra={
            "lineage_uuid": lineage,
            "connection_id": connection_id,
            "job_run_id": job_run_id,
        },
    )
    return job_run_id


def record_execution_outcome(**context: Any) -> None:
    """Publish the completion audit event for the submitted extract job.

    Args:
        **context: The Airflow task context.

    Raises:
        ValidationError: If the lineage UUID cannot be resolved from XCom.
    """
    ti = context["ti"]
    job_arguments = ti.xcom_pull(key=XCOM_JOB_ARGUMENTS, task_ids=TASK_READ_CONFIG)
    if not isinstance(job_arguments, dict):
        raise ValidationError(
            "Job arguments were not produced by read_connection_config"
        )

    job_run_id = ti.xcom_pull(key=XCOM_JOB_RUN_ID, task_ids=TASK_SUBMIT_EMR)
    lineage = str(job_arguments["lineage_uuid"])
    _publish_event(
        EVENT_EXECUTION_COMPLETED,
        lineage,
        {
            "task_id": TASK_RECORD_OUTCOME,
            "status": STATUS_SUCCESS,
            "connection_id": str(job_arguments["connection_id"]),
            "emr_job_run_id": job_run_id,
        },
    )


def on_task_failure(context: Dict[str, Any]) -> None:
    """Publish an EXECUTION_FAILED audit event when any task fails.

    Args:
        context: The Airflow failure callback context.
    """
    try:
        lineage = _parse_lineage_uuid(_run_conf(context))
    except ValidationError:
        logger.error(
            "Failure callback could not resolve lineage UUID",
            extra={"task_id": str(context.get("task_instance_key_str", ""))},
        )
        return

    exception = context.get("exception")
    _publish_event(
        EVENT_EXECUTION_FAILED,
        lineage,
        {"status": STATUS_FAILED, "error": str(exception)},
    )


DEFAULT_ARGS: Dict[str, Any] = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": DEFAULT_RETRIES,
    "retry_delay": timedelta(minutes=DEFAULT_RETRY_DELAY_MINUTES),
    "sla": timedelta(minutes=DEFAULT_SLA_MINUTES),
    "on_failure_callback": on_task_failure,
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Child export DAG submitting EMR Serverless extract jobs",
    schedule=DEFAULT_SCHEDULE,
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
)

read_task = PythonOperator(
    task_id=TASK_READ_CONFIG,
    python_callable=read_connection_config,
    dag=dag,
)

submit_task = PythonOperator(
    task_id=TASK_SUBMIT_EMR,
    python_callable=submit_emr_extract_job,
    dag=dag,
)

record_task = PythonOperator(
    task_id=TASK_RECORD_OUTCOME,
    python_callable=record_execution_outcome,
    dag=dag,
)

read_task >> submit_task >> record_task
