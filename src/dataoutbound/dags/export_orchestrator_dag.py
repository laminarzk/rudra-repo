"""Export Orchestrator DAG.

Selects active export connections whose cron schedule falls within the
+/- 10 minute window of the run reference time (or the single connection named
in a manual run), generates exactly one RFC 4122 v4 lineage UUID per run, and
submits one Child Export DAG run per selected connection.

Task sequence: fetch_configurations -> generate_lineage -> trigger_child_exports.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator
from croniter import croniter

from dataoutbound.common.clients.aws.mwaa.mwaa_client import MwaaClient
from dataoutbound.common.clients.db.db_client import DbClient
from dataoutbound.common.logging.logger import get_logger
from dataoutbound.common.telemetry.auditor import AuditPublisher
from dataoutbound.config.config_loader import ConfigLoader
from dataoutbound.util.exceptions import ClientError, ConfigError, ValidationError

logger = get_logger(__name__)

DAG_ID = "export_orchestrator_dag"
ACTOR_ID = "system:export_orchestrator_dag"
CONFIG_MODULE_KEY = "export_orchestrator_dag"
DAG_TAGS = ["dataoutbound", "orchestration"]
DAG_START_DATE = datetime(2025, 1, 1, tzinfo=timezone.utc)
SCHEDULE_WINDOW_MINUTES = 10

TRIGGER_SOURCE_SCHEDULED = "SCHEDULED"
TRIGGER_SOURCE_MANUAL = "MANUAL"
VALID_TRIGGER_SOURCES = (TRIGGER_SOURCE_SCHEDULED, TRIGGER_SOURCE_MANUAL)

EVENT_EXECUTION_STARTED = "EXECUTION_STARTED"
EVENT_EXECUTION_COMPLETED = "EXECUTION_COMPLETED"
EVENT_EXECUTION_FAILED = "EXECUTION_FAILED"
STATUS_STARTED = "STARTED"
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"

TASK_FETCH_CONFIGURATIONS = "fetch_configurations"
TASK_GENERATE_LINEAGE = "generate_lineage"
TASK_TRIGGER_CHILD_EXPORTS = "trigger_child_exports"

XCOM_SELECTED_CONNECTIONS = "selected_connections"
XCOM_RUN_CONTEXT = "run_context"
XCOM_LINEAGE_UUID = "lineage_uuid"
CHILD_DAG_ID = "child_export_dag"
CONFIG_LOADER_KEY = "dags"
MAX_ACTIVE_RUNS = 1
MANUAL_RUN_TYPE = "manual"
CONNECTION_FIELD_NAMES = (
    "connection_id",
    "customer_id",
    "cron_schedule",
    "source_dataset",
    "destination_type",
    "extraction_mode",
    "target_bucket",
)

QUERY_ACTIVE_CONNECTIONS = """
    SELECT id AS connection_id, customer_id, cron_schedule, is_active,
           dataset AS source_dataset, target_type AS destination_type,
           mode AS extraction_mode, connection_name AS target_bucket,
           extra_parameters
    FROM export_configurations
    WHERE is_active = TRUE
    ORDER BY id
"""

QUERY_ACTIVE_CONNECTION_BY_ID = """
    SELECT id AS connection_id, customer_id, cron_schedule, is_active,
           dataset AS source_dataset, target_type AS destination_type,
           mode AS extraction_mode, connection_name AS target_bucket,
           extra_parameters
    FROM export_configurations
    WHERE is_active = TRUE AND id = :connection_id
"""


@dataclass
class ExportConnectionConfig:
    """Connection metadata read from the Export Config Store."""

    connection_id: str
    customer_id: str
    cron_schedule: str
    is_active: bool
    source_dataset: str
    destination_type: str
    extraction_mode: str
    target_bucket: str
    extra_parameters: Dict[str, Any] = field(default_factory=dict)


@dataclass
class OrchestratorRunContext:
    """Per-run context; execution_id is the single lineage UUID for the run."""

    execution_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    trigger_source: str = TRIGGER_SOURCE_SCHEDULED
    actor_id: str = ACTOR_ID
    scheduled_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    selected_connections: List[ExportConnectionConfig] = field(default_factory=list)


@dataclass(frozen=True)
class OrchestratorSettings:
    """Schedule and retry settings for this DAG, loaded from dags/config.yml."""

    schedule: str
    owner: str
    retries: int
    retry_delay_minutes: int
    email_on_failure: bool


def _load_settings() -> OrchestratorSettings:
    """Read DAG settings for this module from dags/config.yml through ConfigLoader.

    Returns:
        Parsed OrchestratorSettings.

    Raises:
        ConfigError: If a required key is missing or has an invalid type.
    """
    try:
        config = ConfigLoader.load_config(CONFIG_LOADER_KEY)
        section = config[CONFIG_MODULE_KEY]
        return OrchestratorSettings(
            schedule=str(section["schedule"]),
            owner=str(section["owner"]),
            retries=int(section["retries"]),
            retry_delay_minutes=int(section["retry_delay_minutes"]),
            email_on_failure=bool(section["email_on_failure"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        logger.error(
            "Invalid DAG configuration", extra={"dag_id": DAG_ID, "error": str(exc)}
        )
        raise ConfigError(f"Invalid configuration for {DAG_ID}: {exc}") from exc


def _utc_now_iso_ms() -> str:
    """Return the current UTC time as ISO-8601 with millisecond precision and Z suffix."""
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


def _publish_audit_event(
    event_type: str,
    execution_id: str,
    status: str,
    extra: Dict[str, Any] | None = None,
) -> None:
    """Publish one execution lifecycle audit event for this run.

    Args:
        event_type: One of the enumerated business audit event types.
        execution_id: Lineage UUID of the run.
        status: Lifecycle status recorded in the payload.
        extra: Optional additional payload fields.
    """
    payload: Dict[str, Any] = {
        "actor_id": ACTOR_ID,
        "timestamp_utc": _utc_now_iso_ms(),
        "event_type": event_type,
        "execution_id": execution_id,
        "status": status,
    }
    payload.update(extra or {})
    AuditPublisher().publish(event_type, payload)


def _resolve_reference_time(context: Dict[str, Any]) -> datetime:
    """Return the schedule reference time as a timezone-aware datetime.

    Uses the run logical date when present, otherwise the current UTC time.
    """
    logical_date = context.get("logical_date")
    if isinstance(logical_date, datetime):
        if logical_date.tzinfo is None:
            return logical_date.replace(tzinfo=timezone.utc)
        return logical_date
    return datetime.now(timezone.utc)


def _parse_connection(row: Dict[str, Any]) -> ExportConnectionConfig:
    """Validate and convert one database row into an ExportConnectionConfig.

    Args:
        row: A row returned by the active connection query.

    Returns:
        A validated ExportConnectionConfig.

    Raises:
        ValidationError: If required fields are missing, the cron expression is
            invalid, or extra_parameters is not valid JSON.
    """
    missing = [name for name in CONNECTION_FIELD_NAMES if not row.get(name)]
    if missing:
        raise ValidationError(f"Connection row missing fields: {', '.join(missing)}")

    cron_schedule = str(row["cron_schedule"])
    if not croniter.is_valid(cron_schedule):
        raise ValidationError(
            f"Invalid cron expression for connection {row['connection_id']}"
        )

    extra = row.get("extra_parameters") or {}
    if isinstance(extra, str):
        try:
            extra = json.loads(extra)
        except json.JSONDecodeError as exc:
            raise ValidationError(
                f"extra_parameters is not valid JSON for connection {row['connection_id']}"
            ) from exc

    return ExportConnectionConfig(
        connection_id=str(row["connection_id"]),
        customer_id=str(row["customer_id"]),
        cron_schedule=cron_schedule,
        is_active=bool(row.get("is_active", True)),
        source_dataset=str(row["source_dataset"]),
        destination_type=str(row["destination_type"]),
        extraction_mode=str(row["extraction_mode"]),
        target_bucket=str(row["target_bucket"]),
        extra_parameters=dict(extra),
    )


def _within_schedule_window(
    cron_schedule: str,
    reference_time: datetime,
    window_minutes: int = SCHEDULE_WINDOW_MINUTES,
) -> bool:
    """Return True when the cron schedule fires within +/- window_minutes of reference_time.

    Args:
        cron_schedule: A valid 5-field cron expression.
        reference_time: Timezone-aware reference time.
        window_minutes: Half-width of the selection window.

    Returns:
        True if the first fire after (reference - window) is at or before (reference + window).
    """
    if reference_time.tzinfo is None:
        reference_time = reference_time.replace(tzinfo=timezone.utc)
    window = timedelta(minutes=window_minutes)
    next_fire = croniter(cron_schedule, reference_time - window).get_next(datetime)
    if next_fire.tzinfo is None:
        next_fire = next_fire.replace(tzinfo=timezone.utc)
    return next_fire <= reference_time + window


def _select_connections(
    rows: List[Dict[str, Any]],
    reference_time: datetime,
    manual_connection_id: str | None,
) -> List[ExportConnectionConfig]:
    """Filter raw rows to the connections that should run for this execution.

    Manual runs targeting a specific connection bypass the schedule window.
    Invalid rows are logged and skipped so one bad configuration does not block the run.
    """
    selected: List[ExportConnectionConfig] = []
    for row in rows:
        try:
            connection = _parse_connection(row)
        except ValidationError as exc:
            logger.error(
                "Skipping invalid export configuration",
                extra={"connection_id": row.get("connection_id"), "error": str(exc)},
            )
            continue
        if manual_connection_id is None and not _within_schedule_window(
            connection.cron_schedule, reference_time
        ):
            continue
        selected.append(connection)
    return selected


def fetch_configurations(**context: Any) -> List[Dict[str, Any]]:
    """Read active connection metadata and select connections for this run.

    Scheduled runs select connections whose cron time is within the schedule window.
    Manual runs with a connection_id select only that active connection.

    Returns:
        Selected connections as JSON-serialisable dictionaries.

    Raises:
        ClientError: If the Export Config Store query fails.
    """
    dag_run = context.get("dag_run")
    conf: Dict[str, Any] = dict(getattr(dag_run, "conf", None) or {})
    is_manual = getattr(dag_run, "run_type", None) == MANUAL_RUN_TYPE
    manual_connection_id = (
        str(conf["connection_id"]) if is_manual and conf.get("connection_id") else None
    )
    trigger_source = TRIGGER_SOURCE_MANUAL if is_manual else TRIGGER_SOURCE_SCHEDULED
    actor_id = str(conf.get("actor_id") or ACTOR_ID)
    reference_time = _resolve_reference_time(context)

    if manual_connection_id is None:
        query = QUERY_ACTIVE_CONNECTIONS
        params: Dict[str, Any] = {}
    else:
        query = QUERY_ACTIVE_CONNECTION_BY_ID
        params = {"connection_id": manual_connection_id}

    config = ConfigLoader.load_config(CONFIG_LOADER_KEY)
    db_client = DbClient(config)
    try:
        rows = db_client.execute_query(query, params)
    except Exception as exc:
        logger.error("Failed to read export configurations", extra={"error": str(exc)})
        raise ClientError(f"Failed to fetch export configurations: {exc}") from exc

    selected = _select_connections(rows, reference_time, manual_connection_id)
    logger.info(
        "Selected export connections",
        extra={"selected_count": len(selected), "trigger_source": trigger_source},
    )

    ti = context["ti"]
    run_context = {
        "trigger_source": trigger_source,
        "actor_id": actor_id,
        "reference_time": reference_time.isoformat(),
    }
    ti.xcom_push(key=XCOM_RUN_CONTEXT, value=run_context)
    selected_dicts = [asdict(connection) for connection in selected]
    ti.xcom_push(key=XCOM_SELECTED_CONNECTIONS, value=selected_dicts)
    return selected_dicts


def generate_lineage(**context: Any) -> str:
    """Generate exactly one RFC 4122 v4 lineage UUID for the run and record EXECUTION_STARTED.

    Returns:
        The lineage UUID string (execution_id).
    """
    ti = context["ti"]
    run_context_data = (
        ti.xcom_pull(key=XCOM_RUN_CONTEXT, task_ids=TASK_FETCH_CONFIGURATIONS) or {}
    )
    selected_dicts = (
        ti.xcom_pull(key=XCOM_SELECTED_CONNECTIONS, task_ids=TASK_FETCH_CONFIGURATIONS)
        or []
    )
    selected = [ExportConnectionConfig(**item) for item in selected_dicts]

    run = OrchestratorRunContext(
        trigger_source=str(
            run_context_data.get("trigger_source", TRIGGER_SOURCE_SCHEDULED)
        ),
        actor_id=str(run_context_data.get("actor_id", ACTOR_ID)),
        scheduled_time=datetime.now(timezone.utc),
        selected_connections=selected,
    )
    execution_id = run.execution_id
    ti.xcom_push(key=XCOM_LINEAGE_UUID, value=execution_id)

    _publish_audit_event(
        EVENT_EXECUTION_STARTED,
        execution_id,
        STATUS_STARTED,
        {
            "trigger_source": run.trigger_source,
            "trigger_actor": run.actor_id,
            "selected_connection_count": len(selected),
        },
    )
    logger.info(
        "Generated lineage UUID",
        extra={"execution_id": execution_id, "trigger_source": run.trigger_source},
    )
    return execution_id


def _build_child_conf(execution_id: str, connection: Dict[str, Any]) -> Dict[str, Any]:
    """Build the ChildExportDagRunConf payload for one connection."""
    return {
        "lineage_uuid": execution_id,
        "connection_id": connection["connection_id"],
        "dataset": connection["source_dataset"],
        "mode": connection["extraction_mode"],
        "cron_schedule": connection["cron_schedule"],
        "connection": connection,
    }


def trigger_child_exports(**context: Any) -> int:
    """Submit one Child Export DAG run per selected connection, carrying the lineage UUID.

    Returns:
        The number of child runs submitted.

    Raises:
        ValidationError: If the lineage UUID is missing from XCom.
        ClientError: If submitting a child run fails.
    """
    ti = context["ti"]
    connections = (
        ti.xcom_pull(key=XCOM_SELECTED_CONNECTIONS, task_ids=TASK_FETCH_CONFIGURATIONS)
        or []
    )
    execution_id = ti.xcom_pull(key=XCOM_LINEAGE_UUID, task_ids=TASK_GENERATE_LINEAGE)
    if not execution_id:
        raise ValidationError("Lineage UUID missing; refusing to submit child exports")

    config = ConfigLoader.load_config(CONFIG_LOADER_KEY)
    mwaa = MwaaClient(config)

    submitted = 0
    for connection in connections:
        child_conf = _build_child_conf(execution_id, connection)
        try:
            mwaa.trigger_dag_run(dag_id=CHILD_DAG_ID, conf=child_conf)
        except Exception as exc:
            logger.error(
                "Failed to submit child export run",
                extra={
                    "execution_id": execution_id,
                    "connection_id": connection["connection_id"],
                    "error": str(exc),
                },
            )
            raise ClientError(
                f"Child export submission failed for connection {connection['connection_id']}: {exc}"
            ) from exc
        submitted += 1

    logger.info(
        "Submitted child export runs",
        extra={"execution_id": execution_id, "submitted": submitted},
    )
    _publish_audit_event(
        EVENT_EXECUTION_COMPLETED,
        execution_id,
        STATUS_SUCCESS,
        {"child_runs_submitted": submitted},
    )
    return submitted


def on_orchestrator_failure(context: Dict[str, Any]) -> None:
    """Failure callback: record EXECUTION_FAILED with status FAILED for the run.

    If no lineage UUID was generated, no execution has started and no audit event is written.
    """
    ti = context.get("ti")
    execution_id = None
    if ti is not None:
        execution_id = ti.xcom_pull(
            key=XCOM_LINEAGE_UUID, task_ids=TASK_GENERATE_LINEAGE
        )
    exception = context.get("exception")
    if not execution_id:
        logger.error(
            "Orchestrator failed before lineage generation",
            extra={"error": str(exception)},
        )
        return
    logger.error(
        "Orchestrator run failed",
        extra={"execution_id": execution_id, "error": str(exception)},
    )
    _publish_audit_event(
        EVENT_EXECUTION_FAILED,
        execution_id,
        STATUS_FAILED,
        {"error": str(exception)},
    )


def _build_default_args(settings: OrchestratorSettings) -> Dict[str, Any]:
    """Build DAG default_args from configuration."""
    return {
        "owner": settings.owner,
        "depends_on_past": False,
        "email_on_failure": settings.email_on_failure,
        "retries": settings.retries,
        "retry_delay": timedelta(minutes=settings.retry_delay_minutes),
        "on_failure_callback": on_orchestrator_failure,
    }


_SETTINGS = _load_settings()

dag = DAG(
    dag_id=DAG_ID,
    default_args=_build_default_args(_SETTINGS),
    description="Export Orchestrator DAG managing scheduled and manual export runs",
    schedule=_SETTINGS.schedule,
    start_date=DAG_START_DATE,
    catchup=False,
    max_active_runs=MAX_ACTIVE_RUNS,
    tags=DAG_TAGS,
)

fetch_task = PythonOperator(
    task_id=TASK_FETCH_CONFIGURATIONS,
    python_callable=fetch_configurations,
    dag=dag,
)

lineage_task = PythonOperator(
    task_id=TASK_GENERATE_LINEAGE,
    python_callable=generate_lineage,
    dag=dag,
)

trigger_task = PythonOperator(
    task_id=TASK_TRIGGER_CHILD_EXPORTS,
    python_callable=trigger_child_exports,
    dag=dag,
)

fetch_task >> lineage_task >> trigger_task
