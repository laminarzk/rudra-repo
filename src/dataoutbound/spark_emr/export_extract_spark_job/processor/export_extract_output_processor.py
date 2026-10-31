"""Assemble the final output dataset for the export extract job.

Derives the partition columns, orders the output when the recipe asks for it,
writes the partitioned dataset to the Customer Staging Buckets and publishes the
manifest control file. The physical object I/O lives in the ``io`` module.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dataoutbound.common.logging.logger import get_logger
from dataoutbound.spark_emr.export_extract_spark_job.io.customer_staging_buckets_audit_execution_log_store_writer import (
    DatasetWriteResult,
    build_manifest,
    write_dataset,
    write_manifest,
)
from dataoutbound.spark_emr.export_extract_spark_job.util.constants import (
    DEFAULT_INCREMENTAL_COLUMNS,
    DEFAULT_MAX_RECORDS_PER_FILE,
    DEFAULT_OUTPUT_FORMAT,
    DEFAULT_PARTITION_COLUMNS,
    DEFAULT_WRITE_MODE,
    PARTITION_DAY,
    PARTITION_MONTH,
    PARTITION_YEAR,
)
from dataoutbound.spark_emr.export_extract_spark_job.util.file_utils import (
    build_dataset_uri,
    build_manifest_uri,
    get_recipe_value,
)
from dataoutbound.util.exceptions import ValidationError

logger = get_logger(__name__)


@dataclass(frozen=True)
class OutputResult:
    """Everything the entrypoint needs to report on a completed dataset write."""

    dataset_uri: str
    manifest_uri: str
    record_count: int
    file_count: int
    total_file_size_bytes: int
    partition_columns: Tuple[str, ...] = ()


def process_output(
    df: DataFrame,
    recipe: Optional[Mapping[str, Any]] = None,
    job_config: Optional[Mapping[str, Any]] = None,
    lineage_uuid: Optional[str] = None,
    execution_mode: Optional[str] = None,
    watermark_timestamp: Optional[str] = None,
    environment: Optional[str] = None,
) -> OutputResult:
    """Assemble and write the final output datasets and manifest.

    Args:
        df: Transformed DataFrame produced by the transformer.
        recipe: Parsed recipe mapping for the dataset.
        job_config: Job configuration mapping.
        lineage_uuid: RFC 4122 v4 execution lineage UUID.
        execution_mode: ``FULL``/``INCREMENTAL`` used for manifest metadata.
        watermark_timestamp: Watermark used for the incremental read.
        environment: Runtime environment name for manifest metadata.

    Returns:
        An :class:`OutputResult` describing the write.

    Raises:
        ValidationError: when inputs or configuration are invalid.
    """
    if df is None:
        raise ValidationError("A DataFrame is required for output processing")
    if not isinstance(df, DataFrame):
        raise ValidationError("process_output expects a Spark DataFrame")
    if not isinstance(lineage_uuid, str) or not lineage_uuid.strip():
        raise ValidationError("lineage_uuid is required for output processing")

    recipe_map = recipe if isinstance(recipe, Mapping) else {}
    config_map = job_config if isinstance(job_config, Mapping) else {}

    output_settings = _settings(config_map.get("output"))
    sink_settings = _settings(config_map.get("sink"))

    staging_base_uri = sink_settings.get("staging_base_uri")
    if not isinstance(staging_base_uri, str) or not staging_base_uri.strip():
        raise ValidationError("sink.staging_base_uri must be configured")

    output_format = (
        str(output_settings.get("format") or DEFAULT_OUTPUT_FORMAT).strip().lower()
    )
    write_mode = str(output_settings.get("mode") or DEFAULT_WRITE_MODE).strip().lower()
    compute_checksums = bool(output_settings.get("compute_checksums"))
    max_records = _positive_int(
        output_settings.get("max_records_per_file"), DEFAULT_MAX_RECORDS_PER_FILE
    )

    dataset_name = _resolve_dataset_name(recipe_map)
    partition_columns = _resolve_partition_columns(recipe_map, output_settings)
    timestamp_column = _resolve_timestamp_column(df, recipe_map)

    prepared = _derive_partitions(df, partition_columns, timestamp_column)
    prepared = _apply_ordering(prepared, recipe_map)

    record_count = int(prepared.count())
    target_files = _target_file_count(record_count, max_records)

    dataset_uri = build_dataset_uri(staging_base_uri, dataset_name, lineage_uuid)
    manifest_uri = build_manifest_uri(staging_base_uri, dataset_name, lineage_uuid)

    write_result: DatasetWriteResult = write_dataset(
        prepared,
        dataset_uri,
        output_format=output_format,
        partition_columns=partition_columns,
        mode=write_mode,
        num_files=target_files,
        record_count=record_count,
        compute_checksums=compute_checksums,
    )

    manifest = build_manifest(
        lineage_uuid,
        dataset_name,
        write_result,
        execution_mode=execution_mode,
        watermark_timestamp=watermark_timestamp,
        environment=environment,
    )
    write_manifest(prepared.sparkSession, manifest, manifest_uri)

    logger.info(
        "Output processing complete",
        extra={
            "dataset_uri": dataset_uri,
            "manifest_uri": manifest_uri,
            "record_count": record_count,
            "file_count": len(write_result.output_files),
        },
    )

    return OutputResult(
        dataset_uri=dataset_uri,
        manifest_uri=manifest_uri,
        record_count=record_count,
        file_count=len(write_result.output_files),
        total_file_size_bytes=write_result.total_file_size_bytes,
        partition_columns=write_result.partition_columns,
    )


def _settings(value: Any) -> Mapping[str, Any]:
    """Return a config sub-mapping, or an empty mapping when absent."""
    return value if isinstance(value, Mapping) else {}


def _positive_int(value: Any, default: int) -> int:
    """Coerce an optional positive integer, falling back to ``default``."""
    if value is None:
        return int(default)
    try:
        resolved = int(value)
    except (TypeError, ValueError):
        return int(default)
    return resolved if resolved > 0 else int(default)


def _resolve_dataset_name(recipe_map: Mapping[str, Any]) -> str:
    """Resolve the dataset name used in staging and manifest paths."""
    for key in ("recipe_name", "dataset", "entity_type", "source_table"):
        value = recipe_map.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    raise ValidationError("Recipe must declare recipe_name, dataset or source_table")


def _resolve_partition_columns(
    recipe_map: Mapping[str, Any],
    output_settings: Mapping[str, Any],
) -> Tuple[str, ...]:
    """Resolve the output partition columns from recipe then job configuration."""
    declared = get_recipe_value(recipe_map, "output_partition_cols")
    if not declared:
        declared = output_settings.get("partition_columns")

    if not declared:
        return tuple(DEFAULT_PARTITION_COLUMNS)
    if isinstance(declared, str):
        return (declared,)
    if isinstance(declared, Sequence):
        return tuple(str(item) for item in declared)
    raise ValidationError("output_partition_cols must be a list of column names")


def _resolve_timestamp_column(
    df: DataFrame,
    recipe_map: Mapping[str, Any],
) -> Optional[str]:
    """Resolve the timestamp column used to derive year/month/day partitions."""
    declared = recipe_map.get("incremental_column")
    if isinstance(declared, str) and declared in df.columns:
        return declared
    for candidate in DEFAULT_INCREMENTAL_COLUMNS:
        if candidate in df.columns:
            return candidate
    return None


def _derive_partitions(
    df: DataFrame,
    partition_columns: Sequence[str],
    timestamp_column: Optional[str],
) -> DataFrame:
    """Add year/month/day partition columns derived from the timestamp column."""
    working = df
    for partition in partition_columns:
        if partition in working.columns:
            continue
        if timestamp_column is None:
            logger.warning(
                "Partition column cannot be derived without a timestamp column",
                extra={"column": partition},
            )
            continue
        source = F.col(timestamp_column)
        if partition == PARTITION_YEAR:
            working = working.withColumn(PARTITION_YEAR, F.year(source))
        elif partition == PARTITION_MONTH:
            working = working.withColumn(PARTITION_MONTH, F.month(source))
        elif partition == PARTITION_DAY:
            working = working.withColumn(PARTITION_DAY, F.dayofmonth(source))
        else:
            logger.warning(
                "Unsupported partition column requested",
                extra={"column": partition},
            )
    return working


def _apply_ordering(df: DataFrame, recipe_map: Mapping[str, Any]) -> DataFrame:
    """Order the output when the recipe declares explicit ordering columns."""
    declared = get_recipe_value(recipe_map, "output_order_cols")
    if not declared:
        return df
    if isinstance(declared, str):
        candidates = [declared]
    elif isinstance(declared, Sequence):
        candidates = [str(item) for item in declared]
    else:
        raise ValidationError("output_order_cols must be a list of column names")

    existing = [column for column in candidates if column in df.columns]
    if not existing:
        logger.warning("No output ordering columns present in the dataset")
        return df
    return df.orderBy(*[F.col(column).asc_nulls_last() for column in existing])


def _target_file_count(record_count: int, max_records_per_file: int) -> int:
    """Compute the target physical file count for the dataset size."""
    if record_count <= 0 or max_records_per_file <= 0:
        return 1
    return max(1, (record_count + max_records_per_file - 1) // max_records_per_file)
