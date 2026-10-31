"""Application factory for the Export Admin API.

Creates the FastAPI application, registers the export admin router, correlation
id / request logging middleware and maps domain exceptions onto the uniform
error envelope described in the service rules.
"""

from __future__ import annotations

import time
import uuid
from typing import Any, Callable, Dict, List

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from dataoutbound.apis.export_admin_api.dependencies import get_config
from dataoutbound.apis.export_admin_api.export_admin_routes import (
    router as export_admin_router,
)
from dataoutbound.common.logging.logger import get_logger
from dataoutbound.util.exceptions import (
    AuthError,
    ClientError,
    ConfigError,
    ConflictError,
    LaminarError,
    NotFoundError,
    ValidationError,
)

logger = get_logger(__name__)

CORRELATION_HEADER = "X-Correlation-Id"
HEALTH_PATH = "/health"
SERVICE_NAME = "do-admin-api"

_STATUS_TO_CODE: Dict[int, str] = {
    400: "BAD_REQUEST",
    401: "UNAUTHORIZED",
    403: "FORBIDDEN",
    404: "NOT_FOUND",
    409: "CONFLICT",
    422: "UNPROCESSABLE_ENTITY",
    429: "RATE_LIMITED",
    500: "INTERNAL_ERROR",
    502: "BAD_GATEWAY",
    503: "SERVICE_UNAVAILABLE",
}


def _error_body(
    code: str,
    message: str,
    details: List[Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    """Build the standard ``{"error": {...}}`` response body."""
    return {
        "error": {
            "code": code,
            "message": message,
            "details": details or [],
        }
    }


def create_app() -> FastAPI:
    """Create and configure the Export Admin API application."""
    config = get_config()
    cors = config.get("cors") or {}

    application = FastAPI(
        title="DataOutbound Export Admin API",
        description="CRUD and run control for customer export configurations.",
        version=str(config.get("version", "v1")),
        docs_url="/docs",
        redoc_url="/redoc",
    )

    application.add_middleware(
        CORSMiddleware,
        allow_origins=list(cors.get("allow_origins", ["*"])),
        allow_methods=list(cors.get("allow_methods", ["*"])),
        allow_headers=list(cors.get("allow_headers", ["*"])),
        allow_credentials=bool(cors.get("allow_credentials", False)),
    )

    application.include_router(export_admin_router)

    _register_middleware(application)
    _register_exception_handlers(application)

    @application.get(HEALTH_PATH, tags=["health"], status_code=status.HTTP_200_OK)
    def health() -> Dict[str, str]:
        """Liveness probe used by the platform load balancer."""
        return {"status": "ok", "service": SERVICE_NAME}

    logger.info(
        "Export Admin API application created",
        extra={"service": SERVICE_NAME, "module": "export_admin_api"},
    )
    return application


def _register_middleware(application: FastAPI) -> None:
    """Attach correlation-id propagation and structured request logging."""

    @application.middleware("http")
    async def correlation_and_logging(
        request: Request,
        call_next: Callable[[Request], Any],
    ):
        correlation_id = request.headers.get(CORRELATION_HEADER) or str(uuid.uuid4())
        started = time.perf_counter()
        logger.info(
            "request received",
            extra={
                "correlation_id": correlation_id,
                "method": request.method,
                "path": request.url.path,
                "service": SERVICE_NAME,
            },
        )
        try:
            response = await call_next(request)
        except Exception:
            logger.error(
                "unhandled error while processing request",
                exc_info=True,
                extra={
                    "correlation_id": correlation_id,
                    "path": request.url.path,
                    "service": SERVICE_NAME,
                },
            )
            raise
        elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
        response.headers[CORRELATION_HEADER] = correlation_id
        logger.info(
            "request completed",
            extra={
                "correlation_id": correlation_id,
                "method": request.method,
                "path": request.url.path,
                "status_code": response.status_code,
                "elapsed_ms": elapsed_ms,
                "service": SERVICE_NAME,
            },
        )
        return response


def _register_exception_handlers(application: FastAPI) -> None:
    """Map validation, HTTP and domain exceptions to the error envelope."""

    @application.exception_handler(RequestValidationError)
    async def _validation_handler(
        request: Request,
        exc: RequestValidationError,
    ) -> JSONResponse:
        details: List[Dict[str, Any]] = []
        for error in exc.errors():
            location = error.get("loc", ())
            field = ".".join(
                str(part) for part in location if part not in ("body", "query", "path")
            )
            details.append(
                {
                    "field": field,
                    "message": error.get("msg", "invalid value"),
                }
            )
        logger.warning(
            "request payload failed validation",
            extra={"path": request.url.path, "invalid_fields": len(details)},
        )
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content=_error_body(
                "VALIDATION_ERROR",
                "Request payload failed validation",
                details,
            ),
        )

    @application.exception_handler(StarletteHTTPException)
    async def _http_handler(
        request: Request,
        exc: StarletteHTTPException,
    ) -> JSONResponse:
        if isinstance(exc.detail, dict) and "error" in exc.detail:
            return JSONResponse(status_code=exc.status_code, content=exc.detail)
        code = _STATUS_TO_CODE.get(exc.status_code, "HTTP_ERROR")
        message = (
            exc.detail
            if isinstance(exc.detail, str)
            else "Request could not be processed"
        )
        return JSONResponse(
            status_code=exc.status_code,
            content=_error_body(code, message),
        )

    @application.exception_handler(ValidationError)
    async def _domain_validation_handler(
        request: Request,
        exc: ValidationError,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content=_error_body("VALIDATION_ERROR", str(exc)),
        )

    @application.exception_handler(NotFoundError)
    async def _not_found_handler(
        request: Request,
        exc: NotFoundError,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content=_error_body("NOT_FOUND", str(exc)),
        )

    @application.exception_handler(ConflictError)
    async def _conflict_handler(
        request: Request,
        exc: ConflictError,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content=_error_body("CONFLICT", str(exc)),
        )

    @application.exception_handler(AuthError)
    async def _auth_handler(
        request: Request,
        exc: AuthError,
    ) -> JSONResponse:
        reported = getattr(exc, "status_code", None)
        status_code = reported if isinstance(reported, int) else 401
        code = "FORBIDDEN" if status_code == 403 else "UNAUTHORIZED"
        return JSONResponse(
            status_code=status_code,
            content=_error_body(code, str(exc)),
        )

    @application.exception_handler(ConfigError)
    async def _config_handler(
        request: Request,
        exc: ConfigError,
    ) -> JSONResponse:
        logger.error("configuration error", exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=_error_body("CONFIGURATION_ERROR", str(exc)),
        )

    @application.exception_handler(ClientError)
    async def _client_error_handler(
        request: Request,
        exc: ClientError,
    ) -> JSONResponse:
        logger.error("upstream client error", exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_502_BAD_GATEWAY,
            content=_error_body("UPSTREAM_ERROR", str(exc)),
        )

    @application.exception_handler(LaminarError)
    async def _laminar_handler(
        request: Request,
        exc: LaminarError,
    ) -> JSONResponse:
        logger.error("unhandled domain error", exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=_error_body("INTERNAL_ERROR", str(exc)),
        )


app = create_app()
