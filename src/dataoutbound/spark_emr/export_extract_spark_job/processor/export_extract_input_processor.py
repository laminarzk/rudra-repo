"""Validate and normalise the source DataFrame for the export extract job.

Applies the recipe filters, the reservation-subtype predicate and the
incremental watermark filter, then enforces timestamp types on the incremental
columns. Pure function of (DataFrame, config) so it is unit-testable with a
local SparkSession.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

from dataoutbound.common.logging.logger import get_logger
from dataoutbound.spark_emr.export_extract_spark_job.util.constants import (
    DEFAULT_INCREMENTAL_COLUMNS,
    ENTITY_RESERVATION_ITEM,
    EXECUTION_MODE_INCREMENTAL,
    RESERVATION_SUBTYPES,
    RESERVATION_TYPE_COLUMN,
)
from dataoutbound.spark_emr.export_extract_spark_job.util.file_utils import (
    get_recipe_value,
    normalise_execution_mode,
    parse_iso8601_utc,
)
from dataoutbound.util.exceptions import ValidationError

logger = get_logger(__name__)


def process_input(
    df: DataFrame,
    recipe: Optional[Mapping[str, Any]] = None,
    job_config: Optional[Mapping[str, Any]] = None,
    execution_mode: Optional[str] = None,
    watermark_timestamp: Optional[str] = None,
) -> DataFrame:
    """Validate and normalise the extracted source DataFrame.

    Args:
        df: Raw source DataFrame read over JDBC.
        recipe: Parsed recipe mapping for the dataset.
        job_config: Job configuration mapping.
        execution_mode: ``FULL``/``INCREMENTAL`` (aliases accepted).
        watermark_timestamp: ISO-8601 watermark ``T`` for incremental runs.

    Returns:
        The filtered and type-normalised DataFrame.

    Raises:
        ValidationError: when inputs are structurally invalid.
    """
    if df is None:
        raise ValidationError("A DataFrame is required for input processing")
    if not isinstance(df, DataFrame):
        raise ValidationError("process_input expects a Spark DataFrame")

    recipe_map = recipe if isinstance(recipe, Mapping) else {}
    config_map = job_config if isinstance(job_config, Mapping) else {}

    working = _apply_recipe_filters(df, recipe_map)
    working = _apply_reservation_filter(working, recipe_map)
    working = _cast_timestamp_columns(working)
    working = _apply_incremental_filter(
        working, recipe_map, config_map, execution_mode, watermark_timestamp
    )

    logger.info(
        "Input processing complete",
        extra={
            "execution_mode": normalise_execution_mode(execution_mode),
            "columns": len(working.columns),
        },
    )
    return working


def _apply_recipe_filters(df: DataFrame, recipe_map: Mapping[str, Any]) -> DataFrame:
    """Apply each recipe filter expression as a Spark predicate."""
    raw_filters = get_recipe_value(recipe_map, "filters")
    if not raw_filters:
        return df
    if isinstance(raw_filters, str):
        filters: List[str] = [raw_filters]
    elif isinstance(raw_filters, Sequence):
        filters = [str(item) for item in raw_filters]
    else:
        raise ValidationError("filters must be a list of SQL predicates")

    working = df
    for predicate in filters:
        if not predicate.strip():
            continue
        working = working.filter(F.expr(predicate.strip()))
    return working


def _apply_reservation_filter(
    df: DataFrame, recipe_map: Mapping[str, Any]
) -> DataFrame:
    """Restrict reservation-item extractions to the declared reservation subtypes."""
    if (
        str(get_recipe_value(recipe_map, "entity_type") or "")
        != ENTITY_RESERVATION_ITEM
    ):
        return df
    if RESERVATION_TYPE_COLUMN not in df.columns:
        logger.warning(
            "Reservation entity requested but reservation type column is absent",
            extra={"column": RESERVATION_TYPE_COLUMN},
        )
        return df
    return df.filter(F.col(RESERVATION_TYPE_COLUMN).isin(list(RESERVATION_SUBTYPES)))


def _cast_timestamp_columns(df: DataFrame) -> DataFrame:
    """Cast the incremental timestamp columns to ``TimestampType`` when present."""
    working = df
    for column in DEFAULT_INCREMENTAL_COLUMNS:
        if column not in working.columns:
            continue
        existing = working.schema[column].dataType
        if not isinstance(existing, TimestampType):
            working = working.withColumn(column, F.col(column).cast(TimestampType()))
    return working


def _apply_incremental_filter(
    df: DataFrame,
    recipe_map: Mapping[str, Any],
    config_map: Mapping[str, Any],
    execution_mode: Optional[str],
    watermark_timestamp: Optional[str],
) -> DataFrame:
    """Filter strictly greater than the watermark for incremental runs."""
    if normalise_execution_mode(execution_mode) != EXECUTION_MODE_INCREMENTAL:
        return df
    if not watermark_timestamp:
        logger.warning("Incremental mode requested without a watermark; full read used")
        return df

    column = _resolve_incremental_column(df, recipe_map, config_map)
    if column is None:
        logger.warning(
            "Incremental mode requested but no incremental column found; full read used"
        )
        return df

    watermark = parse_iso8601_utc(watermark_timestamp)
    logger.info(
        "Applying incremental watermark filter",
        extra={"incremental_column": column, "watermark": watermark_timestamp},
    )
    return df.filter(F.col(column) > F.lit(watermark))


def _resolve_incremental_column(
    df: DataFrame,
    recipe_map: Mapping[str, Any],
    config_map: Mapping[str, Any],
) -> Optional[str]:
    """Resolve the incremental watermark column from recipe or job configuration."""
    declared = get_recipe_value(recipe_map, "incremental_column")
    if isinstance(declared, str) and declared in df.columns:
        return declared

    source_settings = config_map.get("source")
    if isinstance(source_settings, Mapping):
        configured = source_settings.get("incremental_column")
        if isinstance(configured, str) and configured in df.columns:
            return configured

    for candidate in DEFAULT_INCREMENTAL_COLUMNS:
        if candidate in df.columns:
            return candidate
    return None
