"""Reusable response / error schemas + COMMON_ERROR_RESPONSES dict.

Imported by every router for consistent error documentation in Swagger.
"""

from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field


class BaseResponse(BaseModel):
    """Base success envelope. Routers extend this with additional fields."""

    success: bool = Field(default=True, description="True for successful responses")
    message: str = Field(..., description="Human-readable status message")


# ==================== ERROR SHAPE ====================

class ErrorDetail(BaseModel):
    field: str = Field(..., description="Dot-separated path to the invalid field", examples=["body.mode"])
    message: str = Field(..., description="Human-readable validation message", examples=["Field required"])
    type: str = Field(..., description="Machine-readable error type", examples=["missing"])


class ErrorBody(BaseModel):
    status_code: int = Field(..., description="HTTP status code", examples=[400])
    status_message: str = Field(..., description="HTTP status text", examples=["BAD REQUEST"])
    message: str = Field(..., description="User-friendly error message")
    code: Optional[str] = Field(None, description="Machine-readable error code")
    details: Optional[List[ErrorDetail]] = Field(None, description="Per-field validation details (only on 422)")


# ==================== PER-CODE ERROR RESPONSE MODELS ====================

class _ErrorEnvelope(BaseModel):
    success: bool = Field(default=False)
    error: ErrorBody


class BadRequestResponse(_ErrorEnvelope):
    model_config = ConfigDict(json_schema_extra={
        "example": {
            "success": False,
            "error": {
                "status_code": 400,
                "status_message": "BAD REQUEST",
                "message": "Invalid request. Please check your input.",
                "code": "BAD_REQUEST",
            },
        }
    })


class UnauthorizedResponse(_ErrorEnvelope):
    model_config = ConfigDict(json_schema_extra={
        "example": {
            "success": False,
            "error": {
                "status_code": 401,
                "status_message": "UNAUTHORIZED",
                "message": "Please log in to continue.",
                "code": "UNAUTHORIZED",
            },
        }
    })


class ForbiddenResponse(_ErrorEnvelope):
    model_config = ConfigDict(json_schema_extra={
        "example": {
            "success": False,
            "error": {
                "status_code": 403,
                "status_message": "FORBIDDEN",
                "message": "You don't have permission to access this resource.",
                "code": "FORBIDDEN",
            },
        }
    })


class NotFoundResponse(_ErrorEnvelope):
    model_config = ConfigDict(json_schema_extra={
        "example": {
            "success": False,
            "error": {
                "status_code": 404,
                "status_message": "NOT FOUND",
                "message": "The requested resource was not found.",
                "code": "NOT_FOUND",
            },
        }
    })


class ConflictResponse(_ErrorEnvelope):
    model_config = ConfigDict(json_schema_extra={
        "example": {
            "success": False,
            "error": {
                "status_code": 409,
                "status_message": "CONFLICT",
                "message": "A dump is already running.",
                "code": "DUMP_ALREADY_RUNNING",
            },
        }
    })


class ValidationErrorResponse(_ErrorEnvelope):
    model_config = ConfigDict(json_schema_extra={
        "example": {
            "success": False,
            "error": {
                "status_code": 422,
                "status_message": "UNPROCESSABLE ENTITY",
                "message": "Please check your input and try again.",
                "code": "VALIDATION_ERROR",
                "details": [
                    {"field": "body.mode", "message": "Input should be 'run' or 'resume'", "type": "literal_error"}
                ],
            },
        }
    })


class RateLimitResponse(_ErrorEnvelope):
    model_config = ConfigDict(json_schema_extra={
        "example": {
            "success": False,
            "error": {
                "status_code": 429,
                "status_message": "TOO MANY REQUESTS",
                "message": "Rate limit exceeded. Please try again later.",
                "code": "RATE_LIMITED",
            },
        }
    })


class InternalServerErrorResponse(_ErrorEnvelope):
    model_config = ConfigDict(json_schema_extra={
        "example": {
            "success": False,
            "error": {
                "status_code": 500,
                "status_message": "INTERNAL SERVER ERROR",
                "message": "Something went wrong. Please try again later.",
                "code": "INTERNAL_ERROR",
            },
        }
    })


class ServiceUnavailableResponse(_ErrorEnvelope):
    model_config = ConfigDict(json_schema_extra={
        "example": {
            "success": False,
            "error": {
                "status_code": 503,
                "status_message": "SERVICE UNAVAILABLE",
                "message": "All RapidAPI keys exhausted. Upgrade plan or wait for monthly reset.",
                "code": "ALL_KEYS_EXHAUSTED",
            },
        }
    })


# ==================== COMMON RESPONSES (used everywhere) ====================

COMMON_ERROR_RESPONSES = {
    422: {"model": ValidationErrorResponse, "description": "Request body failed schema validation"},
    429: {"model": RateLimitResponse, "description": "Rate limit exceeded for this endpoint"},
    500: {"model": InternalServerErrorResponse, "description": "Unexpected server error"},
}
