"""JDBC access to the Golden Entity Source Store (Redshift).

Provides partitioned batch reads of the golden entity tables and an append-only
writer used for relational sinks. Contains no business rules - the input
processor owns filtering, casting and PII handling.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession

from dataoutbound.common.logging.logger import get_logger
from dataoutbound.spark_emr.export_extract_spark_job.util.constants import (
    DEFAULT_JDBC_FETCH_SIZE,
    DEFAULT_JDBC_NUM_PARTITIONS,
    JDBC_FORMAT,
    SUPPORTED_WRITE_MODES,
)
from dataoutbound.util.exceptions import ClientError, ValidationError

logger = get_logger(__name__)

_QUALIFIED_IDENTIFIER = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}$"
)
_REDACTED = "***"


def read_table(
    spark: SparkSession,
    jdbc_config: Mapping[str, Any],
    table_name: str,
    partition_column: Optional[str] = None,
    lower_bound: Optional[int] = None,
    upper_bound: Optional[int] = None,
    num_partitions: Optional[int] = None,
    fetch_size: Optional[int] = None,
    predicates: Optional[Sequence[str]] = None,
) -> DataFrame:
    """Read a relational table over JDBC, optionally as a partitioned scan.

    Args:
        spark: Active SparkSession.
        jdbc_config: Connection settings (``url``, ``user``, ``password``,
            ``driver``, ``fetch_size``). Credentials are resolved upstream.
        table_name: Qualified table name (``schema.table``).
        partition_column: Numeric column used for Spark's partitioned JDBC read.
        lower_bound: Inclusive lower bound of the partitioning range.
        upper_bound: Exclusive upper bound of the partitioning range.
        num_partitions: Number of parallel JDBC connections.
        fetch_size: JDBC fetch size (rows per round trip).
        predicates: Optional list of SQL predicates used as read partitions.

    Returns:
        The unmodified source DataFrame.

    Raises:
        ValidationError: when arguments are structurally invalid.
        ClientError: when the JDBC read fails.
    """
    if spark is None:
        raise ValidationError("A SparkSession is required to read a table")

    validated_table = _validate_qualified_identifier(table_name, "table_name")
    options = _build_connection_options(jdbc_config)
    options["dbtable"] = validated_table
    options["fetchsize"] = str(
        _positive_int(fetch_size, DEFAULT_JDBC_FETCH_SIZE, "fetch_size")
    )

    if predicates:
        options["predicates"] = json.dumps(_validate_predicates(predicates))

    if partition_column:
        options["partitionColumn"] = _validate_qualified_identifier(
            partition_column, "partition_column"
        )
        options["lowerBound"] = str(_required_int(lower_bound, "lower_bound"))
        options["upperBound"] = str(_required_int(upper_bound, "upper_bound"))
        options["numPartitions"] = str(
            _positive_int(num_partitions, DEFAULT_JDBC_NUM_PARTITIONS, "num_partitions")
        )

    logger.info(
        "Reading JDBC table",
        extra={
            "table_name": validated_table,
            "partitioned": bool(partition_column),
            "options": _redacted_options(options),
        },
    )

    try:
        return spark.read.format(JDBC_FORMAT).options(**options).load()
    except Exception as exc:
        raise ClientError(f"Failed to read table '{validated_table}': {exc}") from exc


def write_table(
    df: DataFrame,
    jdbc_config: Mapping[str, Any],
    table_name: str,
    mode: str = "append",
    fetch_size: Optional[int] = None,
) -> None:
    """Write a DataFrame to a relational table over JDBC.

    Args:
        df: DataFrame to persist.
        jdbc_config: Connection settings, as for :func:`read_table`.
        table_name: Qualified target table name.
        mode: Spark write mode (``append``, ``overwrite``, ``error``, ``ignore``).
        fetch_size: JDBC batch size.

    Raises:
        ValidationError: when arguments are structurally invalid.
        ClientError: when the JDBC write fails.
    """
    if df is None:
        raise ValidationError("A DataFrame is required to write a table")

    validated_table = _validate_qualified_identifier(table_name, "table_name")
    write_mode = str(mode or "append").strip().lower()
    if write_mode not in SUPPORTED_WRITE_MODES:
        raise ValidationError(f"Unsupported JDBC write mode: {mode!r}")

    options = _build_connection_options(jdbc_config)
    options["dbtable"] = validated_table
    options["fetchsize"] = str(
        _positive_int(fetch_size, DEFAULT_JDBC_FETCH_SIZE, "fetch_size")
    )

    logger.info(
        "Writing JDBC table",
        extra={
            "table_name": validated_table,
            "mode": write_mode,
            "options": _redacted_options(options),
        },
    )

    try:
        df.write.format(JDBC_FORMAT).mode(write_mode).options(**options).save()
    except Exception as exc:
        raise ClientError(f"Failed to write table '{validated_table}': {exc}") from exc


def _build_connection_options(jdbc_config: Mapping[str, Any]) -> Dict[str, str]:
    """Translate a job JDBC config mapping into Spark JDBC reader/writer options."""
    if not isinstance(jdbc_config, Mapping):
        raise ValidationError("jdbc_config must be a mapping")

    url = jdbc_config.get("url")
    if not isinstance(url, str) or not url.strip().startswith("jdbc:"):
        raise ValidationError("jdbc_config.url must be a JDBC connection string")

    options: Dict[str, str] = {"url": url.strip()}
    for key in ("user", "password", "driver"):
        value = jdbc_config.get(key)
        if value:
            options[key] = str(value)
    return options


def _redacted_options(options: Mapping[str, str]) -> Dict[str, str]:
    """Return a log-safe copy of the Spark JDBC options."""
    return {
        key: (_REDACTED if key == "password" else value)
        for key, value in options.items()
    }


def _validate_qualified_identifier(value: Any, label: str) -> str:
    """Validate a possibly schema-qualified SQL identifier."""
    if not isinstance(value, str):
        raise ValidationError(f"{label} must be a string")
    candidate = value.strip()
    if not _QUALIFIED_IDENTIFIER.match(candidate):
        raise ValidationError(f"Invalid {label}: {value!r}")
    return candidate


def _validate_predicates(predicates: Sequence[str]) -> List[str]:
    """Validate SQL predicates used for JDBC read partitioning."""
    if not isinstance(predicates, Sequence) or isinstance(predicates, str):
        raise ValidationError("predicates must be a sequence of SQL strings")

    cleaned: List[str] = []
    for predicate in predicates:
        if not isinstance(predicate, str) or not predicate.strip():
            raise ValidationError("Each predicate must be a non-empty string")
        if ";" in predicate:
            raise ValidationError("Predicates must not contain statement separators")
        cleaned.append(predicate.strip())
    if not cleaned:
        raise ValidationError("predicates must not be empty")
    return cleaned


def _required_int(value: Any, label: str) -> int:
    """Coerce a required integer argument."""
    if value is None:
        raise ValidationError(f"{label} is required for a partitioned read")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"{label} must be an integer: {value!r}") from exc


def _positive_int(value: Any, default: int, label: str) -> int:
    """Coerce an optional positive integer argument."""
    if value is None:
        return int(default)
    try:
        resolved = int(value)
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"{label} must be an integer: {value!r}") from exc
    if resolved <= 0:
        raise ValidationError(f"{label} must be greater than zero: {value!r}")
    return resolved
