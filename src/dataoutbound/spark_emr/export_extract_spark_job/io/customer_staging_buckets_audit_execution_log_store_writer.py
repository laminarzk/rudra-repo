"""Dataset read/write helpers for the Customer Staging Buckets and the
Audit & Execution Log Store.

Writes partitioned Parquet/JSON datasets plus the manifest control file that the
Egress Lambda and Transform & Encrypt DAG consume. Hadoop FileSystem access is
used for listing and manifest publication so the module works identically
against ``s3a://`` in EMR Serverless and ``file://`` in unit tests.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession

from dataoutbound.common.logging.logger import get_logger
from dataoutbound.spark_emr.export_extract_spark_job.util.constants import (
    DEFAULT_OUTPUT_FORMAT,
    DEFAULT_WRITE_MODE,
    SUPPORTED_OUTPUT_FORMATS,
    SUPPORTED_WRITE_MODES,
)
from dataoutbound.spark_emr.export_extract_spark_job.util.file_utils import (
    sha256_of_file,
    utc_now_iso_millis,
)
from dataoutbound.util.exceptions import ClientError, ValidationError

logger = get_logger(__name__)

_HIDDEN_PREFIXES: Tuple[str, ...] = ("_", ".")


@dataclass(frozen=True)
class OutputFileEntry:
    """One physical file produced by the extract job."""

    file_name: str
    file_size: int
    sha256_checksum: Optional[str] = None

    def as_dict(self) -> Dict[str, Any]:
        """Render the entry using the manifest control-file schema."""
        entry: Dict[str, Any] = {
            "file_name": self.file_name,
            "file_size": int(self.file_size),
        }
        if self.sha256_checksum:
            entry["sha256_checksum"] = self.sha256_checksum
        return entry


@dataclass(frozen=True)
class DatasetWriteResult:
    """Summary of a completed dataset write, used to build the manifest."""

    uri: str
    output_format: str
    record_count: int
    partition_columns: Tuple[str, ...]
    output_files: Tuple[OutputFileEntry, ...]

    @property
    def total_file_size_bytes(self) -> int:
        """Total size of every written file in bytes."""
        return sum(entry.file_size for entry in self.output_files)


def write_dataset(
    df: DataFrame,
    uri: str,
    output_format: str = DEFAULT_OUTPUT_FORMAT,
    partition_columns: Optional[Sequence[str]] = None,
    mode: str = DEFAULT_WRITE_MODE,
    num_files: Optional[int] = None,
    record_count: Optional[int] = None,
    compute_checksums: bool = False,
) -> DatasetWriteResult:
    """Write a DataFrame to ``uri`` with optional partition columns.

    Args:
        df: DataFrame to persist.
        uri: Target dataset prefix on S3 or a local filesystem.
        output_format: One of ``parquet``, ``json`` or ``csv``.
        partition_columns: Columns used for directory partitioning.
        mode: Spark write mode.
        num_files: Target file count; drives a repartition when supplied.
        record_count: Pre-computed row count (avoids a second Spark job).
        compute_checksums: When True, stream each written file back to compute a
            SHA-256 digest for the manifest.

    Returns:
        A :class:`DatasetWriteResult` describing the written files.

    Raises:
        ValidationError: for invalid arguments.
        ClientError: when the Spark write or file listing fails.
    """
    if df is None:
        raise ValidationError("A DataFrame is required to write a dataset")
    if not isinstance(uri, str) or not uri.strip():
        raise ValidationError("A target uri is required to write a dataset")

    resolved_format = str(output_format or DEFAULT_OUTPUT_FORMAT).strip().lower()
    if resolved_format not in SUPPORTED_OUTPUT_FORMATS:
        raise ValidationError(f"Unsupported output format: {output_format!r}")

    resolved_mode = str(mode or DEFAULT_WRITE_MODE).strip().lower()
    if resolved_mode not in SUPPORTED_WRITE_MODES:
        raise ValidationError(f"Unsupported write mode: {mode!r}")

    partitions = tuple(str(column) for column in (partition_columns or ()))
    target = df
    if num_files is not None and int(num_files) > 0:
        target = df.repartition(int(num_files))

    resolved_count = (
        int(record_count) if record_count is not None else int(target.count())
    )

    logger.info(
        "Writing dataset",
        extra={
            "uri": uri,
            "format": resolved_format,
            "mode": resolved_mode,
            "record_count": resolved_count,
            "partition_columns": list(partitions),
        },
    )

    writer = target.write.mode(resolved_mode).format(resolved_format)
    if partitions:
        writer = writer.partitionBy(*partitions)
    try:
        writer.save(uri)
    except Exception as exc:
        raise ClientError(f"Failed to write dataset to '{uri}': {exc}") from exc

    files = collect_output_files(
        target.sparkSession, uri, compute_checksums=compute_checksums
    )
    return DatasetWriteResult(
        uri=uri,
        output_format=resolved_format,
        record_count=resolved_count,
        partition_columns=partitions,
        output_files=files,
    )


def read_dataset(
    spark: SparkSession,
    uri: str,
    input_format: str = DEFAULT_OUTPUT_FORMAT,
    options: Optional[Mapping[str, Any]] = None,
) -> DataFrame:
    """Read a dataset from S3 or a local filesystem.

    Args:
        spark: Active SparkSession.
        uri: Dataset prefix to read.
        input_format: ``parquet``, ``json`` or ``csv``.
        options: Additional reader options.

    Returns:
        The loaded DataFrame.
    """
    if spark is None:
        raise ValidationError("A SparkSession is required to read a dataset")
    if not isinstance(uri, str) or not uri.strip():
        raise ValidationError("A dataset uri is required")

    resolved_format = str(input_format or DEFAULT_OUTPUT_FORMAT).strip().lower()
    if resolved_format not in SUPPORTED_OUTPUT_FORMATS:
        raise ValidationError(f"Unsupported input format: {input_format!r}")

    reader = spark.read.format(resolved_format)
    if options:
        if not isinstance(options, Mapping):
            raise ValidationError("Reader options must be a mapping")
        resolved_options = {str(k): str(v) for k, v in options.items()}
        reader = reader.options(**resolved_options)

    try:
        return reader.load(uri)
    except Exception as exc:
        raise ClientError(f"Failed to read dataset from '{uri}': {exc}") from exc


def collect_output_files(
    spark: SparkSession,
    uri: str,
    compute_checksums: bool = False,
) -> Tuple[OutputFileEntry, ...]:
    """List every physical, non-hidden file written below ``uri``."""
    jvm, filesystem, path = _hadoop_path(spark, uri)
    try:
        statuses = filesystem.listStatus(path)
    except Exception as exc:
        raise ClientError(f"Failed to list dataset files under '{uri}': {exc}") from exc

    entries: List[OutputFileEntry] = []
    for status in statuses:
        child = status.getPath()
        name = str(child.getName())
        if bool(status.isDirectory()):
            entries.extend(
                _collect_directory(jvm, filesystem, child, name, compute_checksums)
            )
        elif not _is_hidden(name):
            entries.append(
                _build_entry(jvm, filesystem, child, name, status, compute_checksums)
            )
    return tuple(entries)


def build_manifest(
    lineage_uuid: str,
    dataset: str,
    result: DatasetWriteResult,
    execution_mode: Optional[str] = None,
    watermark_timestamp: Optional[str] = None,
    environment: Optional[str] = None,
    generated_at: Optional[str] = None,
) -> Dict[str, Any]:
    """Build the manifest control file for a completed dataset write."""
    if not isinstance(result, DatasetWriteResult):
        raise ValidationError("result must be a DatasetWriteResult")

    return {
        "lineage_uuid": str(lineage_uuid),
        "dataset": str(dataset),
        "environment": environment,
        "execution_mode": execution_mode,
        "watermark_timestamp": watermark_timestamp,
        "generated_at_utc": generated_at or utc_now_iso_millis(),
        "record_count": int(result.record_count),
        "total_file_size_bytes": int(result.total_file_size_bytes),
        "output_uri": result.uri,
        "output_format": result.output_format,
        "partition_columns": list(result.partition_columns),
        "output_files": [entry.as_dict() for entry in result.output_files],
    }


def write_manifest(
    spark: SparkSession,
    manifest: Mapping[str, Any],
    manifest_uri: str,
) -> str:
    """Publish the manifest control file to the staging bucket.

    Returns:
        The URI the manifest was written to.
    """
    if not isinstance(manifest, Mapping):
        raise ValidationError("manifest must be a mapping")
    if not isinstance(manifest_uri, str) or not manifest_uri.strip():
        raise ValidationError("manifest_uri is required")

    payload = json.dumps(dict(manifest), sort_keys=True, indent=2).encode("utf-8")
    jvm, filesystem, path = _hadoop_path(spark, manifest_uri)

    try:
        parent = path.getParent()
        if parent is not None and not filesystem.exists(parent):
            filesystem.mkdirs(parent)
        stream = filesystem.create(path, True)
        try:
            stream.write(bytearray(payload))
        finally:
            stream.close()
    except Exception as exc:
        raise ClientError(
            f"Failed to write manifest to '{manifest_uri}': {exc}"
        ) from exc

    logger.info(
        "Manifest written",
        extra={"manifest_uri": manifest_uri, "bytes": len(payload)},
    )
    return manifest_uri


def _collect_directory(
    jvm: Any,
    filesystem: Any,
    path: Any,
    prefix: str,
    compute_checksums: bool,
) -> List[OutputFileEntry]:
    """Recursively collect non-hidden files below a partition directory."""
    collected: List[OutputFileEntry] = []
    try:
        statuses = filesystem.listStatus(path)
    except Exception as exc:
        raise ClientError(f"Failed to list partition '{prefix}': {exc}") from exc

    for status in statuses:
        child = status.getPath()
        name = str(child.getName())
        relative = f"{prefix}/{name}"
        if bool(status.isDirectory()):
            collected.extend(
                _collect_directory(jvm, filesystem, child, relative, compute_checksums)
            )
        elif not _is_hidden(name):
            collected.append(
                _build_entry(
                    jvm, filesystem, child, relative, status, compute_checksums
                )
            )
    return collected


def _build_entry(
    jvm: Any,
    filesystem: Any,
    path: Any,
    relative_name: str,
    status: Any,
    compute_checksums: bool,
) -> OutputFileEntry:
    """Build a manifest file entry, optionally computing its SHA-256 digest."""
    size = int(status.getLen())
    checksum = None
    if compute_checksums:
        checksum = _sha256_of_hadoop_file(jvm, filesystem, path)
    return OutputFileEntry(
        file_name=relative_name,
        file_size=size,
        sha256_checksum=checksum,
    )


def _sha256_of_hadoop_file(jvm: Any, filesystem: Any, path: Any) -> str:
    """Copy a filesystem object to local disk and hash it."""
    temp_dir = tempfile.mkdtemp(prefix="export_extract_")
    local_path = os.path.join(temp_dir, "payload")
    try:
        local_target = jvm.org.apache.hadoop.fs.Path(f"file://{local_path}")
        filesystem.copyToLocalFile(path, local_target)
        return sha256_of_file(local_path)
    except Exception as exc:
        raise ClientError(f"Failed to checksum '{path}': {exc}") from exc
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def _is_hidden(name: str) -> bool:
    """Return True for Hadoop/Spark control files such as ``_SUCCESS``."""
    return name.startswith(_HIDDEN_PREFIXES)


def _hadoop_path(spark: SparkSession, uri: str) -> Tuple[Any, Any, Any]:
    """Return the JVM gateway, FileSystem and Path for a URI."""
    if spark is None:
        raise ValidationError("A SparkSession is required for filesystem access")

    context = getattr(spark, "sparkContext", None)
    jsc = getattr(context, "_jsc", None) if context is not None else None
    jvm = getattr(context, "_jvm", None) if context is not None else None
    if jsc is None or jvm is None:
        raise ClientError(
            "SparkSession has no JVM gateway for Hadoop filesystem access"
        )

    try:
        hadoop_conf = jsc.hadoopConfiguration()
        path = jvm.org.apache.hadoop.fs.Path(uri)
        filesystem = path.getFileSystem(hadoop_conf)
    except Exception as exc:
        raise ClientError(f"Unable to resolve filesystem for '{uri}': {exc}") from exc
    return jvm, filesystem, path
