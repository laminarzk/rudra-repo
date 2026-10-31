"""Transform & Encrypt DAG (AWS MWAA) — canvas component "egress dag".

Triggered by the Egress Lambda once a staged export object lands in the
customer staging bucket. The DAG downloads the staged payload, formats it
into chunked JSON/TXT files using the connection-defined split threshold,
optionally GZIP-compresses and then PGP-encrypts every file under strict
fail-closed semantics, writes a ``manifest_<lineage_uuid>.json`` control
file containing the SHA-256 checksum of every output file, and delivers
the package to the configured customer destination (S3, Azure Blob, GCS
or sFTP).

Every task publishes structured audit events carrying the immutable
lineage UUID (``execution_id``) through the shared ``AuditPublisher``.
Heavy processing stays inside operator ``execute()`` callables so top-level
DAG parsing remains lightweight.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import shutil
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator

from dataoutbound.common.clients.aws.s3.s3_client import S3Client
from dataoutbound.common.clients.aws.ssm.ssm_client import SsmClient
from dataoutbound.common.clients.azure.blob_client import BlobClient
from dataoutbound.common.clients.gcp.gcs_client import GcsClient
from dataoutbound.common.clients.sftp.sftp_client import SftpClient
from dataoutbound.common.logging.logger import get_logger
from dataoutbound.common.telemetry.auditor import AuditPublisher
from dataoutbound.config.config_loader import ConfigLoader
from dataoutbound.util.exceptions import ClientError, ValidationError

logger = get_logger("dataoutbound.dags.transform_encrypt_dag")
auditor = AuditPublisher()

DAG_ID = "transform_encrypt_dag"
ACTOR_ID = "system:transform_encrypt_dag"
WORK_ROOT = "/tmp/transform_encrypt"
CHUNK_BYTES = 1024 * 1024
DEFAULT_SPLIT_THRESHOLD = 500_000
MAX_TRANSFER_RETRIES = 3
PACKAGE_XCOM_KEY = "package"
DEFAULT_SCHEDULE = "@daily"
DEFAULT_RETRIES = 3
DEFAULT_RETRY_DELAY_SECONDS = 300
DEFAULT_PGP_KEY_PREFIX = "/dataoutbound/pgp/public"


def _load_dag_settings() -> Dict[str, Any]:
    """Load this DAG's settings block from ``dags/config.yml``.

    The read is a small local file access performed once at DAG parse time;
    no network, database or secret access happens here.
    """
    try:
        config = ConfigLoader.load_config("dags")
    except Exception as exc:  # noqa: BLE001 - DAG must still parse on bad config
        logger.warning(
            "Unable to load dags config; falling back to defaults",
            extra={"error": str(exc)},
        )
        return {}

    if not isinstance(config, dict):
        return {}

    block = config.get(DAG_ID)
    if isinstance(block, dict):
        return dict(block)
    return {}


_DAG_SETTINGS = _load_dag_settings()
SCHEDULE = _DAG_SETTINGS.get("schedule", DEFAULT_SCHEDULE)
RETRIES = int(_DAG_SETTINGS.get("retries", DEFAULT_RETRIES))
RETRY_DELAY_SECONDS = int(
    _DAG_SETTINGS.get("retry_delay_seconds", DEFAULT_RETRY_DELAY_SECONDS)
)
PGP_KEY_PARAMETER_PREFIX = str(
    _DAG_SETTINGS.get("pgp_key_parameter_prefix", DEFAULT_PGP_KEY_PREFIX)
)

DEFAULT_ARGS: Dict[str, Any] = {
    "owner": _DAG_SETTINGS.get("owner", "data-platform"),
    "depends_on_past": False,
    "email_on_failure": bool(_DAG_SETTINGS.get("email_on_failure", True)),
    "retries": RETRIES,
    "retry_delay": timedelta(seconds=RETRY_DELAY_SECONDS),
}


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------
def _utc_now_ms() -> str:
    """Return the current UTC time as ISO-8601 with millisecond precision."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _execution_id_from_context(context: Dict[str, Any]) -> str:
    """Extract the lineage UUID carried by the DAG run conf payload."""
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    return str(conf.get("lineage_uuid", ""))


def _publish(event_type: str, execution_id: str, payload: Dict[str, Any]) -> None:
    """Publish a business audit event without failing the caller's flow."""
    record: Dict[str, Any] = {
        "actor_id": ACTOR_ID,
        "timestamp_utc": _utc_now_ms(),
        "event_type": event_type,
        "execution_id": execution_id,
        "payload": payload,
    }
    try:
        auditor.publish(event_type, record)
    except Exception as exc:  # noqa: BLE001 - audit must never break the DAG
        logger.warning(
            "Audit publish failed",
            extra={
                "event_type": event_type,
                "execution_id": execution_id,
                "error": str(exc),
            },
        )


def _sha256_of_file(path: str) -> str:
    """Stream a file through SHA-256 so large payloads stay off-heap."""
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            block = handle.read(CHUNK_BYTES)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def _gzip_compress(source_path: str, target_path: str) -> None:
    """GZIP-compress ``source_path`` into ``target_path`` with a stable mtime."""
    with open(source_path, "rb") as source_handle:
        with open(target_path, "wb") as target_handle:
            with gzip.GzipFile(
                fileobj=target_handle, mode="wb", mtime=0
            ) as gzip_handle:
                while True:
                    block = source_handle.read(CHUNK_BYTES)
                    if not block:
                        break
                    gzip_handle.write(block)


def _load_pgp_key_material(key_id: str) -> str:
    """Resolve the PGP public key for ``key_id`` from SSM Parameter Store."""
    config = ConfigLoader.load_config("dags")
    parameter_name = f"{PGP_KEY_PARAMETER_PREFIX.rstrip('/')}/{key_id}"
    try:
        ssm_client = SsmClient(config)
        key_material = ssm_client.get_parameter(parameter_name)
    except Exception as exc:  # noqa: BLE001 - surfaced as a fail-closed error
        raise AirflowFailException(
            f"Unable to load PGP public key '{key_id}': {exc}"
        ) from exc

    if not key_material:
        raise AirflowFailException(
            f"PGP public key '{key_id}' resolved to an empty value"
        )
    return str(key_material)


class _PgpEncryptor:
    """Fail-closed PGP encryption helper built on ``python-gnupg``."""

    def __init__(self, key_id: str, key_material: str, homedir: str) -> None:
        self._key_id = key_id
        self._key_material = key_material
        self._homedir = homedir

    def encrypt_file(self, source_path: str, target_path: str) -> None:
        """Encrypt ``source_path`` to ``target_path`` or raise immediately."""
        try:
            import gnupg  # imported lazily so DAG parsing never needs it
        except ImportError as exc:
            raise AirflowFailException(
                "PGP encryption requested but python-gnupg is unavailable"
            ) from exc

        os.makedirs(self._homedir, exist_ok=True)
        gpg = gnupg.GPG(gnupghome=self._homedir)

        import_result = gpg.import_keys(self._key_material)
        if not getattr(import_result, "fingerprints", None):
            raise AirflowFailException(
                f"PGP key import for '{self._key_id}' produced no fingerprints"
            )

        with open(source_path, "rb") as source_handle:
            status = gpg.encrypt_file(
                source_handle,
                recipients=[self._key_id],
                output=target_path,
                always_trust=True,
            )

        if not status.ok:
            raise AirflowFailException(
                f"PGP encryption failed for '{self._key_id}': {status.status}"
            )


def _destination_from_context(context: Dict[str, Any]) -> str:
    """Best-effort destination name for audit callbacks."""
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    connection = conf.get("connection") or {}
    return str(conf.get("destination_type") or connection.get("destination_type") or "")


def _on_task_failure(context: Dict[str, Any]) -> None:
    """Record a TRANSFER_FAILED event when a task exhausts its retries."""
    execution_id = _execution_id_from_context(context)
    task_instance = context.get("task_instance")
    task_id = getattr(task_instance, "task_id", "unknown")
    exception = context.get("exception")
    logger.error(
        "Transform & Encrypt task failed",
        extra={
            "execution_id": execution_id,
            "task_id": task_id,
            "error": str(exception),
        },
    )
    _publish(
        "TRANSFER_FAILED",
        execution_id,
        {
            "destination": _destination_from_context(context),
            "task_id": task_id,
            "status": "FAILED",
            "error": str(exception),
        },
    )


def _on_delivery_success(context: Dict[str, Any]) -> None:
    """Record EXECUTION_COMPLETED once delivery has succeeded."""
    execution_id = _execution_id_from_context(context)
    _publish(
        "EXECUTION_COMPLETED",
        execution_id,
        {"component": ACTOR_ID, "status": "SUCCESS"},
    )


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------
def resolve_package_config(**context: Dict[str, Any]) -> Dict[str, Any]:
    """Resolve runtime configuration and normalise the DAG-run conf payload."""
    conf = dict(getattr(context.get("dag_run"), "conf", None) or {})

    lineage_uuid = str(conf.get("lineage_uuid") or uuid.uuid4())

    source_reference = conf.get("source_s3_reference") or {}
    if not isinstance(source_reference, dict):
        source_reference = {}

    source_bucket = source_reference.get("bucket") or conf.get("source_bucket")
    source_key = source_reference.get("key") or conf.get("source_key")
    if not source_bucket or not source_key:
        raise ValidationError(
            "DAG run conf must provide 'source_s3_reference' with 'bucket' and "
            "'key', or the flat 'source_bucket' and 'source_key' fields"
        )

    connection = dict(conf.get("connection") or {})
    destination = dict(conf.get("destination") or connection.get("destination") or {})
    destination_type = str(
        conf.get("destination_type") or connection.get("destination_type") or "s3"
    )

    package: Dict[str, Any] = {
        "lineage_uuid": lineage_uuid,
        "source_bucket": str(source_bucket),
        "source_key": str(source_key),
        "destination_type": destination_type,
        "destination": destination,
        "compress_gzip": bool(
            connection.get("compress_gzip", conf.get("compress_gzip", True))
        ),
        "encrypt_pgp": bool(
            connection.get("encrypt_pgp", conf.get("encrypt_pgp", True))
        ),
        "pgp_key_id": connection.get("pgp_key_id") or conf.get("pgp_key_id"),
        "split_threshold_records": int(
            connection.get("split_threshold_records", DEFAULT_SPLIT_THRESHOLD)
        ),
        "work_dir": os.path.join(WORK_ROOT, lineage_uuid),
    }

    context["ti"].xcom_push(key=PACKAGE_XCOM_KEY, value=package)
    logger.info(
        "Resolved transform & encrypt configuration",
        extra={
            "execution_id": lineage_uuid,
            "destination_type": destination_type,
            "compress_gzip": package["compress_gzip"],
            "encrypt_pgp": package["encrypt_pgp"],
        },
    )
    return package


def download_staged_file(**context: Dict[str, Any]) -> Dict[str, Any]:
    """Download the staged export object from the customer staging bucket."""
    ti = context["ti"]
    package = ti.xcom_pull(key=PACKAGE_XCOM_KEY, task_ids="resolve_package_config")
    work_dir = package["work_dir"]
    os.makedirs(work_dir, exist_ok=True)

    local_path = os.path.join(work_dir, "source.jsonl")
    config = ConfigLoader.load_config("dags")
    s3_client = S3Client(config)

    try:
        s3_client.download_file(
            package["source_bucket"], package["source_key"], local_path
        )
    except Exception as exc:  # noqa: BLE001 - mapped to a retryable client error
        raise ClientError(
            f"Failed to download staged object '{package['source_key']}': {exc}"
        ) from exc

    package["local_source_path"] = local_path
    ti.xcom_push(key=PACKAGE_XCOM_KEY, value=package)
    logger.info(
        "Downloaded staged export object",
        extra={
            "execution_id": package["lineage_uuid"],
            "source_key": package["source_key"],
        },
    )
    return package


def format_and_split(**context: Dict[str, Any]) -> Dict[str, Any]:
    """Format records into chunked JSON and TXT files under the split threshold."""
    ti = context["ti"]
    package = ti.xcom_pull(key=PACKAGE_XCOM_KEY, task_ids="download_staged_file")
    work_dir = package["work_dir"]
    local_source = package["local_source_path"]
    threshold = max(1, int(package["split_threshold_records"]))

    raw_dir = os.path.join(work_dir, "raw")
    os.makedirs(raw_dir, exist_ok=True)

    lineage_uuid = package["lineage_uuid"]
    output_files: List[str] = []
    buffer: List[str] = []
    total_records = 0
    part_index = 0

    def _flush(records: List[str], index: int) -> List[str]:
        if not records:
            return []
        base = f"{lineage_uuid}_part{index:04d}"
        json_path = os.path.join(raw_dir, f"{base}.json")
        txt_path = os.path.join(raw_dir, f"{base}.txt")
        with open(json_path, "w", encoding="utf-8") as json_handle:
            for record in records:
                json_handle.write(record + "\n")
        with open(txt_path, "w", encoding="utf-8") as txt_handle:
            for record in records:
                txt_handle.write(record + "\n")
        return [json_path, txt_path]

    with open(local_source, "r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            buffer.append(stripped)
            total_records += 1
            if len(buffer) >= threshold:
                output_files.extend(_flush(buffer, part_index))
                part_index += 1
                buffer = []

    output_files.extend(_flush(buffer, part_index))

    package["raw_files"] = output_files
    package["raw_dir"] = raw_dir
    package["record_count"] = total_records
    ti.xcom_push(key=PACKAGE_XCOM_KEY, value=package)
    logger.info(
        "Formatted and split staged records",
        extra={
            "execution_id": lineage_uuid,
            "record_count": total_records,
            "file_count": len(output_files),
        },
    )
    return package


def compress_and_encrypt(**context: Dict[str, Any]) -> Dict[str, Any]:
    """GZIP-compress then PGP-encrypt every output file (fail-closed)."""
    ti = context["ti"]
    package = ti.xcom_pull(key=PACKAGE_XCOM_KEY, task_ids="format_and_split")

    work_dir = package["work_dir"]
    raw_files = list(package.get("raw_files") or [])
    compress_gzip = bool(package.get("compress_gzip"))
    encrypt_pgp = bool(package.get("encrypt_pgp"))
    lineage_uuid = package["lineage_uuid"]

    if not raw_files:
        raise AirflowFailException("No staged records were available to process")

    package_dir = os.path.join(work_dir, "package")
    os.makedirs(package_dir, exist_ok=True)

    encryptor = None
    if encrypt_pgp:
        key_id = package.get("pgp_key_id")
        if not key_id:
            raise AirflowFailException(
                "PGP encryption is enabled but no PGP key id is configured"
            )
        key_material = _load_pgp_key_material(str(key_id))
        encryptor = _PgpEncryptor(
            str(key_id), key_material, os.path.join(work_dir, ".gnupg")
        )

    produced: List[str] = []
    for raw_path in raw_files:
        base_name = os.path.basename(raw_path)
        if compress_gzip:
            current = os.path.join(package_dir, f"{base_name}.gz")
            _gzip_compress(raw_path, current)
        else:
            current = os.path.join(package_dir, base_name)
            shutil.copy2(raw_path, current)

        if encryptor is not None:
            encrypted_path = f"{current}.pgp"
            encryptor.encrypt_file(current, encrypted_path)
            os.remove(current)
            current = encrypted_path

        produced.append(current)

    package["package_files"] = produced
    package["package_dir"] = package_dir
    ti.xcom_push(key=PACKAGE_XCOM_KEY, value=package)
    logger.info(
        "Compressed and encrypted export files",
        extra={
            "execution_id": lineage_uuid,
            "file_count": len(produced),
            "gzip": compress_gzip,
            "pgp": encrypt_pgp,
        },
    )
    return package


def build_manifest(**context: Dict[str, Any]) -> Dict[str, Any]:
    """Write the manifest control file listing sizes and SHA-256 checksums."""
    ti = context["ti"]
    package = ti.xcom_pull(key=PACKAGE_XCOM_KEY, task_ids="compress_and_encrypt")
    lineage_uuid = package["lineage_uuid"]
    work_dir = package["work_dir"]
    package_files = list(package.get("package_files") or [])

    entries: List[Dict[str, Any]] = []
    total_bytes = 0
    for path in package_files:
        size = os.path.getsize(path)
        total_bytes += size
        entries.append(
            {
                "file_name": os.path.basename(path),
                "file_size": size,
                "sha256_checksum": _sha256_of_file(path),
            }
        )

    manifest: Dict[str, Any] = {
        "lineage_uuid": lineage_uuid,
        "record_count": int(package.get("record_count", 0)),
        "total_file_size_bytes": total_bytes,
        "output_files": entries,
    }

    manifest_path = os.path.join(work_dir, f"manifest_{lineage_uuid}.json")
    with open(manifest_path, "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)

    package["manifest"] = manifest
    package["manifest_path"] = manifest_path
    package["delivery_files"] = package_files + [manifest_path]
    ti.xcom_push(key=PACKAGE_XCOM_KEY, value=package)
    logger.info(
        "Built export manifest control file",
        extra={
            "execution_id": lineage_uuid,
            "output_files": len(entries),
            "total_file_size_bytes": total_bytes,
        },
    )
    return package


# ---------------------------------------------------------------------------
# Destination delivery with read-back SHA-256 verification
# ---------------------------------------------------------------------------
def _expected_checksums(manifest: Dict[str, Any]) -> Dict[str, str]:
    entries = manifest.get("output_files") or []
    return {str(entry["file_name"]): str(entry["sha256_checksum"]) for entry in entries}


def _verify_checksum(
    expected: Dict[str, str], file_name: str, actual: str, location: str
) -> None:
    want = expected.get(file_name)
    if want and want != actual:
        raise ClientError(
            f"SHA-256 mismatch for delivered artefact '{location}': "
            f"expected {want}, got {actual}"
        )


def _deliver_to_s3(
    package: Dict[str, Any], files: List[str], manifest: Dict[str, Any]
) -> None:
    config = ConfigLoader.load_config("dags")
    destination = dict(package.get("destination") or {})
    bucket = destination.get("bucket")
    if not bucket:
        raise ValidationError("S3 destination configuration requires a 'bucket'")
    prefix = str(destination.get("prefix") or "").strip("/")

    client = S3Client(config)
    expected = _expected_checksums(manifest)
    verify_dir = tempfile.mkdtemp(prefix="s3_verify_")
    try:
        for path in files:
            name = os.path.basename(path)
            key = f"{prefix}/{name}" if prefix else name
            client.upload_file(path, str(bucket), key)
            read_back = os.path.join(verify_dir, name)
            client.download_file(str(bucket), key, read_back)
            _verify_checksum(expected, name, _sha256_of_file(read_back), key)
    finally:
        shutil.rmtree(verify_dir, ignore_errors=True)


def _deliver_to_azure_blob(
    package: Dict[str, Any], files: List[str], manifest: Dict[str, Any]
) -> None:
    config = ConfigLoader.load_config("dags")
    destination = dict(package.get("destination") or {})
    container = destination.get("container")
    if not container:
        raise ValidationError(
            "Azure Blob destination configuration requires a 'container'"
        )
    prefix = str(destination.get("prefix") or "").strip("/")

    client = BlobClient(config)
    expected = _expected_checksums(manifest)
    verify_dir = tempfile.mkdtemp(prefix="blob_verify_")
    try:
        for path in files:
            name = os.path.basename(path)
            blob_name = f"{prefix}/{name}" if prefix else name
            client.upload_blob(str(container), blob_name, path)
            read_back = os.path.join(verify_dir, name)
            client.download_blob(str(container), blob_name, read_back)
            _verify_checksum(expected, name, _sha256_of_file(read_back), blob_name)
    finally:
        shutil.rmtree(verify_dir, ignore_errors=True)


def _deliver_to_gcs(
    package: Dict[str, Any], files: List[str], manifest: Dict[str, Any]
) -> None:
    config = ConfigLoader.load_config("dags")
    destination = dict(package.get("destination") or {})
    bucket = destination.get("bucket")
    if not bucket:
        raise ValidationError("GCS destination configuration requires a 'bucket'")
    prefix = str(destination.get("prefix") or "").strip("/")

    client = GcsClient(config)
    expected = _expected_checksums(manifest)
    verify_dir = tempfile.mkdtemp(prefix="gcs_verify_")
    try:
        for path in files:
            name = os.path.basename(path)
            object_path = f"{prefix}/{name}" if prefix else name
            client.upload_file(str(bucket), object_path, path)
            read_back = os.path.join(verify_dir, name)
            client.download_file(str(bucket), object_path, read_back)
            _verify_checksum(expected, name, _sha256_of_file(read_back), object_path)
    finally:
        shutil.rmtree(verify_dir, ignore_errors=True)


def _is_host_key_error(exc: Exception) -> bool:
    """Detect a strict host-key verification refusal so it is never retried."""
    if type(exc).__name__ == "HostKeyVerificationError":
        return True
    return "host key" in str(exc).lower()


def _deliver_to_sftp(
    package: Dict[str, Any], files: List[str], manifest: Dict[str, Any]
) -> None:
    config = ConfigLoader.load_config("dags")
    destination = dict(package.get("destination") or {})
    remote_dir = str(destination.get("path") or "").strip("/")
    expected = _expected_checksums(manifest)

    client = SftpClient(config)
    for path in files:
        name = os.path.basename(path)
        remote_path = f"{remote_dir}/{name}" if remote_dir else name
        try:
            client.upload(path, remote_path)
            remote_checksum = client.remote_sha256(remote_path)
        except Exception as exc:  # noqa: BLE001 - classified below
            if _is_host_key_error(exc):
                raise AirflowFailException(
                    f"SFTP host-key verification failed: {exc}"
                ) from exc
            raise
        _verify_checksum(expected, name, str(remote_checksum), remote_path)


def deliver_package(**context: Dict[str, Any]) -> Dict[str, Any]:
    """Deliver the finished package and manifest to the customer destination."""
    ti = context["ti"]
    package = ti.xcom_pull(key=PACKAGE_XCOM_KEY, task_ids="build_manifest")

    lineage_uuid = package["lineage_uuid"]
    destination_type = package["destination_type"]
    files = list(package.get("delivery_files") or [])
    manifest = dict(package.get("manifest") or {})
    file_names = [os.path.basename(path) for path in files]

    try_number = int(getattr(context.get("task_instance"), "try_number", 1) or 1)
    if try_number > 1:
        _publish(
            "TRANSFER_STARTED",
            lineage_uuid,
            {
                "destination": destination_type,
                "file_names": file_names,
                "retry_attempt": try_number - 1,
            },
        )

    _publish(
        "TRANSFER_STARTED",
        lineage_uuid,
        {"destination": destination_type, "file_names": file_names},
    )

    if destination_type == "s3":
        _deliver_to_s3(package, files, manifest)
    elif destination_type == "azure_blob":
        _deliver_to_azure_blob(package, files, manifest)
    elif destination_type == "gcs":
        _deliver_to_gcs(package, files, manifest)
    elif destination_type == "sftp":
        _deliver_to_sftp(package, files, manifest)
    else:
        raise ValidationError(f"Unsupported destination type '{destination_type}'")

    _publish(
        "TRANSFER_COMPLETED",
        lineage_uuid,
        {
            "destination": destination_type,
            "file_names": file_names,
            "checksums": _expected_checksums(manifest),
            "status": "SUCCESS",
        },
    )
    logger.info(
        "Export package delivered",
        extra={
            "execution_id": lineage_uuid,
            "destination_type": destination_type,
            "file_count": len(files),
        },
    )
    return package


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
dag = DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description=(
        "Transform, compress, encrypt and deliver staged exports to "
        "customer destinations"
    ),
    schedule=SCHEDULE,
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags={"domain": "dataoutbound", "tier": "egress"},
)

resolve_task = PythonOperator(
    task_id="resolve_package_config",
    python_callable=resolve_package_config,
    on_failure_callback=_on_task_failure,
    dag=dag,
)

download_task = PythonOperator(
    task_id="download_staged_file",
    python_callable=download_staged_file,
    on_failure_callback=_on_task_failure,
    dag=dag,
)

format_task = PythonOperator(
    task_id="format_and_split",
    python_callable=format_and_split,
    on_failure_callback=_on_task_failure,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="compress_and_encrypt",
    python_callable=compress_and_encrypt,
    retries=0,
    on_failure_callback=_on_task_failure,
    dag=dag,
)

manifest_task = PythonOperator(
    task_id="build_manifest",
    python_callable=build_manifest,
    on_failure_callback=_on_task_failure,
    dag=dag,
)

deliver_task = PythonOperator(
    task_id="deliver_package",
    python_callable=deliver_package,
    retries=MAX_TRANSFER_RETRIES,
    retry_delay=timedelta(seconds=RETRY_DELAY_SECONDS),
    on_success_callback=_on_delivery_success,
    on_failure_callback=_on_task_failure,
    dag=dag,
)

(
    resolve_task
    >> download_task
    >> format_task
    >> transform_task
    >> manifest_task
    >> deliver_task
)
