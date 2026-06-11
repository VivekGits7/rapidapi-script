from typing import List, Optional

import asyncpg
from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from logger import get_logger

logger = get_logger(__name__)


# ==================== CUSTOM EXCEPTIONS ====================

class AppException(Exception):
    """Base exception for application-specific errors."""

    def __init__(
        self,
        status_code: int = 500,
        detail: str = "Internal server error",
        error_code: Optional[str] = None,
        headers: Optional[dict] = None,
    ):
        self.status_code = status_code
        self.detail = detail
        self.error_code = error_code
        self.headers = headers
        super().__init__(self.detail)


class NotFoundException(AppException):
    def __init__(self, detail: str = "Resource not found", error_code: str = "NOT_FOUND"):
        super().__init__(status_code=404, detail=detail, error_code=error_code)


class BadRequestException(AppException):
    def __init__(self, detail: str = "Bad request", error_code: str = "BAD_REQUEST"):
        super().__init__(status_code=400, detail=detail, error_code=error_code)


class ConflictException(AppException):
    def __init__(self, detail: str = "Resource already exists", error_code: str = "CONFLICT"):
        super().__init__(status_code=409, detail=detail, error_code=error_code)


class UnauthorizedException(AppException):
    def __init__(self, detail: str = "Unauthorized", error_code: str = "UNAUTHORIZED"):
        super().__init__(status_code=401, detail=detail, error_code=error_code)


class ForbiddenException(AppException):
    def __init__(self, detail: str = "Forbidden", error_code: str = "FORBIDDEN"):
        super().__init__(status_code=403, detail=detail, error_code=error_code)


# ==================== DUMP-SPECIFIC EXCEPTIONS ====================

class AllKeysExhaustedError(AppException):
    """Every RapidAPI key is cooling longer than KEY_EXHAUSTION_THRESHOLD_SEC.
    Likely monthly quota hit across all keys. Pause job and exit cleanly."""

    def __init__(
        self,
        detail: str = "All RapidAPI keys are exhausted. Upgrade plan or wait for monthly reset.",
        error_code: str = "ALL_KEYS_EXHAUSTED",
    ):
        super().__init__(status_code=503, detail=detail, error_code=error_code)


class MonthlyQuotaReachedError(AppException):
    """The RapidAPI monthly request ceiling (plan hard limit minus safety buffer)
    has been reached. Pause the job cleanly and resume next month."""

    def __init__(
        self,
        detail: str = "Monthly RapidAPI request limit reached. Pausing until next month.",
        error_code: str = "MONTHLY_QUOTA_REACHED",
    ):
        super().__init__(status_code=503, detail=detail, error_code=error_code)


class DumpAlreadyRunningError(AppException):
    def __init__(self, detail: str = "A dump is already running.", error_code: str = "DUMP_ALREADY_RUNNING"):
        super().__init__(status_code=409, detail=detail, error_code=error_code)


class NoResumableJobError(AppException):
    def __init__(
        self,
        detail: str = "No paused or failed job available to resume.",
        error_code: str = "NO_RESUMABLE_JOB",
    ):
        super().__init__(status_code=404, detail=detail, error_code=error_code)


# ==================== UTILITY ====================

def validate_uuid(*values: tuple[str, str]) -> None:
    """Validate UUID path params. Not used in the dumper itself but available
    for any FastAPI routes that take UUIDs."""
    import uuid as _uuid
    for value, param_name in values:
        try:
            _uuid.UUID(value)
        except (ValueError, AttributeError):
            raise BadRequestException(f"Invalid {param_name} format. Expected a valid UUID.")


HTTP_STATUS_MESSAGES = {
    400: "BAD REQUEST",
    401: "UNAUTHORIZED",
    403: "FORBIDDEN",
    404: "NOT FOUND",
    409: "CONFLICT",
    422: "UNPROCESSABLE ENTITY",
    429: "TOO MANY REQUESTS",
    500: "INTERNAL SERVER ERROR",
    502: "BAD GATEWAY",
    503: "SERVICE UNAVAILABLE",
}


def create_error_response(
    status_code: int,
    detail: str,
    error_code: Optional[str] = None,
    errors: Optional[List[dict]] = None,
) -> dict:
    response = {
        "success": False,
        "error": {
            "status_code": status_code,
            "status_message": HTTP_STATUS_MESSAGES.get(status_code, "ERROR"),
            "message": detail,
        },
    }
    if error_code:
        response["error"]["code"] = error_code
    if errors:
        response["error"]["details"] = errors
    return response


# ==================== HANDLERS ====================

async def app_exception_handler(request: Request, exc: AppException) -> JSONResponse:
    log_level = logger.error if exc.status_code >= 500 else logger.warning
    log_level(f"AppException: {exc.detail} | Path: {request.url.path}")
    return JSONResponse(
        status_code=exc.status_code,
        content=create_error_response(exc.status_code, exc.detail, exc.error_code),
        headers=exc.headers,
    )


async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    log_level = logger.error if exc.status_code >= 500 else logger.warning
    log_level(f"HTTPException: {exc.detail} | Path: {request.url.path}")
    return JSONResponse(
        status_code=exc.status_code,
        content=create_error_response(exc.status_code, str(exc.detail)),
    )


async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    errors = [
        {"field": ".".join(str(loc) for loc in e["loc"]), "message": e["msg"], "type": e["type"]}
        for e in exc.errors()
    ]
    logger.warning(f"Validation error | Path: {request.url.path}")
    return JSONResponse(
        status_code=422,
        content=create_error_response(
            422, "Please check your input and try again.", "VALIDATION_ERROR", errors
        ),
    )


async def pydantic_validation_handler(request: Request, exc: ValidationError) -> JSONResponse:
    errors = [
        {"field": ".".join(str(loc) for loc in e["loc"]), "message": e["msg"], "type": e["type"]}
        for e in exc.errors()
    ]
    logger.warning(f"Pydantic validation error | Path: {request.url.path}")
    return JSONResponse(
        status_code=422,
        content=create_error_response(
            422, "Please check your input and try again.", "VALIDATION_ERROR", errors
        ),
    )


async def asyncpg_exception_handler(request: Request, exc: asyncpg.PostgresError) -> JSONResponse:
    logger.error(f"Database error | Path: {request.url.path} | Error: {exc}")
    if isinstance(exc, asyncpg.UniqueViolationError):
        return JSONResponse(
            status_code=409,
            content=create_error_response(409, "This resource already exists.", "CONFLICT"),
        )
    if isinstance(exc, asyncpg.ForeignKeyViolationError):
        return JSONResponse(
            status_code=400,
            content=create_error_response(400, "Related resource not found.", "FOREIGN_KEY_ERROR"),
        )
    return JSONResponse(
        status_code=500,
        content=create_error_response(500, "Database error.", "DATABASE_ERROR"),
    )


async def generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.error(f"Unhandled exception | Path: {request.url.path} | Error: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content=create_error_response(500, "Something went wrong. Please try again later.", "INTERNAL_ERROR"),
    )


def setup_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(AppException, app_exception_handler)
    app.add_exception_handler(HTTPException, http_exception_handler)
    app.add_exception_handler(RequestValidationError, validation_exception_handler)
    app.add_exception_handler(ValidationError, pydantic_validation_handler)
    app.add_exception_handler(asyncpg.PostgresError, asyncpg_exception_handler)
    app.add_exception_handler(Exception, generic_exception_handler)
    logger.info("Error handlers registered")
