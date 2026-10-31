"""Dependency providers for the Export Admin API.

This is the single construction point for ConfigLoader, AuditPublisher and the
``common.clients`` instances used by the API. No route or service constructs an
SDK client itself.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Any, Dict

from dataoutbound.apis.export_admin_api.export_admin_service import (
    ExportAdminService,
)
from dataoutbound.common.clients.aws.mwaa.mwaa_client import MwaaClient
from dataoutbound.common.clients.db.db_client import DbClient
from dataoutbound.common.logging.logger import get_logger
from dataoutbound.common.security.auth import require_role
from dataoutbound.common.telemetry.auditor import AuditPublisher
from dataoutbound.config.config_loader import ConfigLoader

logger = get_logger(__name__)

MODULE_NAME = "export_admin_api"
API_CONFIG_NAME = "apis"
REQUIRED_ROLE = "export-admin"


@lru_cache(maxsize=1)
def get_config() -> Dict[str, Any]:
    """Load the API configuration exactly once per process."""
    config = ConfigLoader.load_config(API_CONFIG_NAME)
    return dict(config or {})


@lru_cache(maxsize=1)
def get_audit_publisher() -> AuditPublisher:
    """Return the shared AuditPublisher emitting business audit events."""
    return AuditPublisher()


@lru_cache(maxsize=1)
def get_db_client() -> DbClient:
    """Return the shared Export Config Store client."""
    return DbClient(get_config())


@lru_cache(maxsize=1)
def get_mwaa_client() -> MwaaClient:
    """Return the shared MWAA client used for manual trigger and stop."""
    return MwaaClient(get_config())


def get_export_admin_service() -> ExportAdminService:
    """Assemble the ExportAdminService with its collaborators."""
    return ExportAdminService(
        db_client=get_db_client(),
        mwaa_client=get_mwaa_client(),
        audit_publisher=get_audit_publisher(),
        config=get_config(),
    )


# Authentication / authorization dependency: returns the decoded subject for
# requests carrying a valid JWT with the ``export-admin`` role, and raises
# AuthError (401) or a 403 HTTP exception otherwise.
require_export_admin = require_role(REQUIRED_ROLE)
