"""Entrypoint for the Export Extract Spark Job running on EMR Serverless.

Parses the job arguments, builds the SparkSession through the session helper,
loads the job configuration and the dataset recipe, then drives
read -> input processor -> transformer -> output processor, emitting audit
events and a deterministic exit code. No transformation logic lives here.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Mapping, Sequence
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession

from dataoutbound.common.logging.logger import get_logger
from dataoutbound.common.telemetry.auditor import AuditPublisher
from dataoutbound.config.config_loader import ConfigLoader
from dataoutbound.spark_emr.export_extract_spark_job.helper.spark_session_helper import (
    get_spark_session,
)
from dataoutbound.spark_emr.export_extract_spark_job.io.child_export_golden_entity_source_store_reader import (
    read_table,
)
from dataoutbound.spark_emr.export_extract_spark_job.processor.export_extract_input_processor import (
    process_input,
)
from dataoutbound.spark_emr.export_extract_spark_job.processor.export_extract_output_processor import (
    OutputResult,
    process_output,
)
from dataoutbound.spark_emr.export_extract_spark_job.transform.export_extract_transformer import (
    DefaultTransformer,
)
from dataoutbound.spark_emr.export_extract_spark_job.util.constants import (
    DEFAULT_ENVIRONMENT,
    DEFAULT_RECIPES_DIR,
    ENVIRONMENT_KEYS,
    EVENT_EXECUTION_COMPLETED,
    EVENT_EXECUTION_FAILED,
    EVENT_EXECUTION_STARTED,
    EXIT_FAILURE,
    EXIT_SUCCESS,
    JOB_APPLICATION_NAME,
    MODULE_NAME,
    STATUS_FAILED,
    STATUS_STARTED,
    STATUS_SUCCESS,
)
from dataoutbound.spark_emr.export_extract_spark_job.util.file_utils import (
    build_audit_payload,
    normalise_execution_mode,
)
from dataoutbound.util.exceptions import ClientError, ConfigError, ValidationError

logger = get_logger(__name__)

_AUDIT_PUBLISHER = AuditPublisher()

RECIPE_FILE_EXTENSION = ".json"


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse the job arguments passed by the Child Export DAG."""
    parser = argparse.ArgumentParser(
        description="Export Extract Spark Job (EMR Serverless, application emr_do)"
    )
    parser.add_argument(
        "--lineage-uuid",
        required=True,
        help="RFC 4122 v4 execution lineage UUID",
    )
    parser.add_argument(
        "--environment",
        default=DEFAULT_ENVIRONMENT,
        help="Runtime environment name (dev, staging, prod)",
    )
    parser.add_argument(
        "--recipe-name",
        required=True,
        help="Name of the JSON recipe file under the recipes directory",
    )
    parser.add_argument(
        "--execution-mode",
        default="FULL",
        help="FULL or INCREMENTAL (aliases accepted)",
    )
    parser.add_argument(
        "--watermark-timestamp",
        default=None,
        help="ISO-8601 watermark timestamp for incremental runs",
    )
    return parser.parse_args(argv)


def main() -> None:
    """Console entrypoint: parse arguments and exit with the run's status code."""
    sys.exit(run(parse_args()))


def run(args: argparse.Namespace) -> int:
    """Execute the export extract job for a single recipe.

    Args:
        args: Parsed command line arguments.

    Returns:
        ``EXIT_SUCCESS`` on a completed run, ``EXIT_FAILURE`` otherwise.
    """
    try:
        lineage_uuid = _require_text(
            getattr(args, "lineage_uuid", None), "lineage_uuid"
        )
        environment = _require_text(getattr(args, "environment", None), "environment")
        recipe_name = _require_text(getattr(args, "recipe_name", None), "recipe_name")
        execution_mode = normalise_execution_mode(getattr(args, "execution_mode", None))
    except ValidationError as exc:
        logger.error("Invalid job arguments", extra={"error": str(exc)})
        return EXIT_FAILURE

    watermark_timestamp = getattr(args, "watermark_timestamp", None)
    spark: Optional[SparkSession] = None

    try:
        started = build_audit_payload(
            EVENT_EXECUTION_STARTED,
            lineage_uuid,
            status=STATUS_STARTED,
            extra={"environment": environment, "recipe": recipe_name},
        )
        _AUDIT_PUBLISHER.publish(EVENT_EXECUTION_STARTED, started)

        job_config = load_job_config(environment)
        recipe = load_recipe(recipe_name, job_config)
        spark = get_spark_session(JOB_APPLICATION_NAME, job_config)

        source_df = _extract_source(spark, recipe, job_config)
        prepared = process_input(
            source_df,
            recipe,
            job_config,
            execution_mode,
            watermark_timestamp,
        )
        transformed = DefaultTransformer(recipe, job_config).transform(prepared)
        result = process_output(
            transformed,
            recipe,
            job_config,
            lineage_uuid,
            execution_mode,
            watermark_timestamp,
            environment,
        )

        completed = build_audit_payload(
            EVENT_EXECUTION_COMPLETED,
            lineage_uuid,
            status=STATUS_SUCCESS,
            extra=_result_summary(result, environment, recipe_name),
        )
        _AUDIT_PUBLISHER.publish(EVENT_EXECUTION_COMPLETED, completed)

        logger.info(
            "Export Extract Spark Job completed",
            extra={
                "lineage_uuid": lineage_uuid,
                "recipe": recipe_name,
                "record_count": result.record_count,
            },
        )
        return EXIT_SUCCESS
    except (ValidationError, ConfigError, ClientError) as exc:
        logger.error(
            "Export Extract Spark Job failed",
            exc_info=True,
            extra={"lineage_uuid": lineage_uuid, "error": str(exc)},
        )
        _publish_failure(lineage_uuid, environment, recipe_name, exc)
        return EXIT_FAILURE
    except Exception as exc:
        logger.error(
            "Export Extract Spark Job failed unexpectedly",
            exc_info=True,
            extra={"lineage_uuid": lineage_uuid},
        )
        _publish_failure(lineage_uuid, environment, recipe_name, exc)
        return EXIT_FAILURE
    finally:
        _stop_spark_session(spark)


def load_job_config(environment: str) -> Dict[str, Any]:
    """Load the job configuration and select the requested environment section."""
    try:
        loaded = ConfigLoader.load_config(MODULE_NAME, environment=environment)
    except TypeError:
        loaded = ConfigLoader.load_config(MODULE_NAME)
    except Exception as exc:
        raise ConfigError(f"Failed to load job configuration: {exc}") from exc

    if not isinstance(loaded, Mapping):
        raise ConfigError("Job configuration must be a mapping")
    return _select_environment_section(loaded, environment)


def load_recipe(
    recipe_name: str,
    job_config: Mapping[str, Any],
    base_dir: Optional[str] = None,
) -> Dict[str, Any]:
    """Load a declarative recipe JSON file from the job's recipes directory."""
    if not isinstance(recipe_name, str) or not recipe_name.strip():
        raise ValidationError("recipe_name is required")

    recipes_dir = str(job_config.get("recipes_dir") or DEFAULT_RECIPES_DIR)
    job_root = base_dir or os.path.dirname(os.path.abspath(__file__))
    directory = (
        recipes_dir
        if os.path.isabs(recipes_dir)
        else os.path.join(job_root, recipes_dir)
    )

    file_name = recipe_name.strip()
    if not file_name.endswith(RECIPE_FILE_EXTENSION):
        file_name = f"{file_name}{RECIPE_FILE_EXTENSION}"
    path = os.path.join(directory, file_name)

    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except FileNotFoundError as exc:
        raise ConfigError(f"Recipe file not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ConfigError(f"Recipe file is not valid JSON: {path}") from exc
    except OSError as exc:
        raise ConfigError(f"Unable to read recipe file '{path}': {exc}") from exc

    if not isinstance(payload, Mapping):
        raise ConfigError(f"Recipe must be a JSON object: {path}")
    return dict(payload)


def _extract_source(
    spark: SparkSession,
    recipe: Mapping[str, Any],
    job_config: Mapping[str, Any],
) -> Any:
    """Read the source DataFrame from the Golden Entity Source Store over JDBC."""
    jdbc_config = _require_mapping(job_config.get("jdbc"), "jdbc")
    source_settings = (
        job_config.get("source")
        if isinstance(job_config.get("source"), Mapping)
        else {}
    )
    source_table = _require_text(recipe.get("source_table"), "source_table")

    return read_table(
        spark,
        jdbc_config,
        source_table,
        partition_column=source_settings.get("partition_column"),
        lower_bound=source_settings.get("lower_bound"),
        upper_bound=source_settings.get("upper_bound"),
        num_partitions=source_settings.get("num_partitions"),
        fetch_size=jdbc_config.get("fetch_size"),
    )


def _result_summary(
    result: OutputResult,
    environment: str,
    recipe_name: str,
) -> Dict[str, Any]:
    """Build the extra fields published with the EXECUTION_COMPLETED event."""
    return {
        "environment": environment,
        "recipe": recipe_name,
        "dataset_uri": result.dataset_uri,
        "manifest_uri": result.manifest_uri,
        "record_count": result.record_count,
        "file_count": result.file_count,
        "total_file_size_bytes": result.total_file_size_bytes,
    }


def _publish_failure(
    lineage_uuid: str,
    environment: str,
    recipe_name: str,
    exc: Exception,
) -> None:
    """Publish an EXECUTION_FAILED audit event without masking the original error."""
    try:
        payload = build_audit_payload(
            EVENT_EXECUTION_FAILED,
            lineage_uuid,
            status=STATUS_FAILED,
            extra={
                "environment": environment,
                "recipe": recipe_name,
                "error": str(exc),
            },
        )
        _AUDIT_PUBLISHER.publish(EVENT_EXECUTION_FAILED, payload)
    except Exception as publish_error:  # pragma: no cover - defensive
        logger.warning(
            "Failed to publish EXECUTION_FAILED audit event",
            extra={"error": str(publish_error)},
        )


def _stop_spark_session(spark: Optional[SparkSession]) -> None:
    """Stop the SparkSession, never raising on shutdown failures."""
    if spark is None:
        return
    try:
        spark.stop()
    except Exception as exc:  # pragma: no cover - depends on the runtime
        logger.warning("Failed to stop SparkSession cleanly", extra={"error": str(exc)})


def _select_environment_section(
    config: Mapping[str, Any],
    environment: str,
) -> Dict[str, Any]:
    """Merge the ``default`` and requested environment sections when present."""
    if not any(key in config for key in ENVIRONMENT_KEYS):
        return dict(config)

    merged: Dict[str, Any] = {}
    default_section = config.get("default")
    if isinstance(default_section, Mapping):
        merged = _deep_merge(merged, default_section)

    env_section = config.get(environment)
    if isinstance(env_section, Mapping):
        merged = _deep_merge(merged, env_section)
    return merged


def _deep_merge(
    base: Mapping[str, Any],
    overlay: Mapping[str, Any],
) -> Dict[str, Any]:
    """Recursively merge ``overlay`` into ``base`` returning a new dictionary."""
    merged: Dict[str, Any] = dict(base)
    for key, value in overlay.items():
        current = merged.get(key)
        if isinstance(current, Mapping) and isinstance(value, Mapping):
            merged[key] = _deep_merge(current, value)
        else:
            merged[key] = value
    return merged


def _require_text(value: Any, label: str) -> str:
    """Validate that a value is a non-empty string."""
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{label} is required")
    return value.strip()


def _require_mapping(value: Any, label: str) -> Dict[str, Any]:
    """Validate that a configuration value is a mapping."""
    if not isinstance(value, Mapping):
        raise ConfigError(f"{label} must be configured in the job configuration")
    return dict(value)


if __name__ == "__main__":
    main()
