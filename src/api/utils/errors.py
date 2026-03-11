"""Global Exception boundaries for the API."""

import logging
import sentry_sdk
from fastapi import Request
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)

class BaseAPIException(Exception):
    """Base exception for all domain errors that should be handled gracefully."""
    def __init__(self, message: str, status_code: int = 400, context: dict = None):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.context = context or {}


def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Catches all unhandled exceptions, logs to Sentry, returns generic 500."""
    # Capture unhandled exceptions in Sentry
    sentry_sdk.capture_exception(exc)
    logger.error(f"Unhandled server error: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal Server Error",
            "message": "An unexpected error occurred. It has been reported."
        }
    )

def base_api_exception_handler(request: Request, exc: BaseAPIException) -> JSONResponse:
    """Handles domain-specific BaseAPIExceptions, returns clean JSON response."""
    # We might not need to report known business flow exceptions to Sentry,
    # or perhaps log them as warnings.
    logger.warning(f"Domain error {exc.status_code}: {exc.message} | Context: {exc.context}")
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": "DomainError",
            "message": exc.message,
            "context": exc.context
        }
    )
