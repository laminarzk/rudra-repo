"""SparkSession construction for the Export Extract Spark Job.

Applies the job's Spark configuration (shuffle, committer and S3A settings),
optionally enables the Hive/Glue catalog and returns a session ready for the
processors. No job logic lives here.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession

from dataoutbound.common.logging.logger import get_logger
from dataoutbound.util.exceptions import ConfigError

logger = get_logger(__name__)

DEFAULT_LOG_LEVEL = "WARN"

DEFAULT_SPARK_CONF: Dict[str, str] = {
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.parquet.compression.codec": "snappy",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
}

S3_COMMITTER_CONF: Dict[str, str] = {
    "spark.hadoop.fs.s3a.committer.name": "directory",
    "spark.hadoop.fs.s3a.committer.staging.conflict-mode": "append",
    "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
}

GLUE_CATALOG_CONF: Dict[str, str] = {
    "spark.hadoop.hive.metastore.client.factory.class": (
        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
    ),
}


def get_spark_session(
    app_name: str,
    config: Optional[Mapping[str, Any]] = None,
) -> SparkSession:
    """Create (or reuse) the SparkSession for the export extract job.

    Args:
        app_name: Spark application name; required and non-empty.
        config: Job configuration mapping. Recognised keys are ``spark`` (extra
            Spark conf), ``hadoop`` (raw Hadoop conf overrides), ``hive``
            (``enabled`` / ``glue_catalog``) and ``log_level``.

    Returns:
        A configured, ready-to-use ``SparkSession``.

    Raises:
        ConfigError: when the session cannot be created.
    """
    if not isinstance(app_name, str) or not app_name.strip():
        raise ConfigError("app_name is required to build a SparkSession")

    job_config: Mapping[str, Any] = config if isinstance(config, Mapping) else {}

    spark_conf: Dict[str, str] = dict(DEFAULT_SPARK_CONF)
    spark_conf.update(S3_COMMITTER_CONF)
    spark_conf.update(_stringify_mapping(job_config.get("spark")))

    hive_settings = job_config.get("hive") or {}
    hive_enabled = False
    glue_enabled = False
    if isinstance(hive_settings, Mapping):
        hive_enabled = bool(hive_settings.get("enabled"))
        glue_enabled = bool(hive_settings.get("glue_catalog"))
    if glue_enabled:
        spark_conf.update(GLUE_CATALOG_CONF)

    builder = SparkSession.builder.appName(app_name.strip())
    for key, value in spark_conf.items():
        builder = builder.config(key, value)
    if hive_enabled:
        builder = builder.enableHiveSupport()

    try:
        spark = builder.getOrCreate()
    except Exception as exc:  # pragma: no cover - depends on the runtime
        raise ConfigError(f"Unable to create SparkSession '{app_name}': {exc}") from exc

    log_level = str(job_config.get("log_level") or DEFAULT_LOG_LEVEL).upper()
    spark.sparkContext.setLogLevel(log_level)
    _apply_hadoop_conf(spark, job_config.get("hadoop"))

    logger.info(
        "SparkSession created",
        extra={
            "app_name": app_name,
            "log_level": log_level,
            "hive_enabled": hive_enabled,
        },
    )
    return spark


def _stringify_mapping(mapping: Any) -> Dict[str, str]:
    """Convert a config mapping into a ``str -> str`` Spark conf dictionary."""
    if not isinstance(mapping, Mapping):
        return {}
    resolved: Dict[str, str] = {}
    for key, value in mapping.items():
        if key is None or value is None:
            continue
        resolved[str(key)] = str(value)
    return resolved


def _apply_hadoop_conf(spark: SparkSession, mapping: Any) -> None:
    """Apply explicit Hadoop configuration overrides on the live Spark context."""
    overrides = _stringify_mapping(mapping)
    if not overrides:
        return

    context = getattr(spark, "sparkContext", None)
    jsc = getattr(context, "_jsc", None) if context is not None else None
    if jsc is None:
        raise ConfigError("SparkContext has no JVM gateway; cannot apply Hadoop conf")

    hadoop_conf = jsc.hadoopConfiguration()
    for key, value in overrides.items():
        hadoop_conf.set(key, value)
    logger.info("Applied Hadoop overrides", extra={"keys": sorted(overrides)})
