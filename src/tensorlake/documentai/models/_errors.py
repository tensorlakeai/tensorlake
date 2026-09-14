from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator


class DocumentAIError(Exception):
    """
    Base class for Document AI API errors.

    This exception is raised for errors that occur during the operation of the Document AI API.
    It can be used to catch and handle errors in a generic way.
    """

    def __init__(self, message: str, code: Optional[str] = None):
        super().__init__(message)
        self.code = code


class ErrorCode(str, Enum):
    """
    Error codes for Document AI API.

    These codes are used to identify specific error conditions in the API.
    They can be used for programmatic handling of errors.
    """

    QUOTA_EXCEEDED = "QUOTA_EXCEEDED"
    INVALID_JSON_SCHEMA = "INVALID_JSON_SCHEMA"
    INVALID_CONFIGURATION = "INVALID_CONFIGURATION"
    INVALID_PAGE_CLASSIFICATION = "INVALID_PAGE_CLASSIFICATION"
    ENTITY_NOT_FOUND = "ENTITY_NOT_FOUND"
    ENTITY_ALREADY_EXISTS = "ENTITY_ALREADY_EXISTS"
    INVALID_FILE = "INVALID_FILE"
    INVALID_PAGE_RANGE = "INVALID_PAGE_RANGE"
    INVALID_MIME_TYPE = "INVALID_MIME_TYPE"
    INVALID_DATASET_NAME = "INVALID_DATASET_NAME"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    INVALID_MULTIPART = "INVALID_MULTIPART"
    MULTIPART_STREAM_END = "MULTIPART_STREAM_END"
    INVALID_QUERY_PARAMS = "INVALID_QUERY_PARAMS"
    # Returned by the SDK, never by the service: the error carried a code this SDK version
    # does not know, no code at all, or the response body was not a valid error response.
    UNKNOWN = "UNKNOWN"


class ErrorResponse(BaseModel):
    """
    Error response for Document AI API.

    This model is used to return error information when the Document AI API encounters
    a user-facing error.
    """

    message: str = Field(..., description="A human-readable error message")
    code: ErrorCode = Field(
        ErrorCode.UNKNOWN, description="The error code for programmatic handling"
    )
    trace_id: Optional[str] = Field(
        None, description="Optional correlation ID for distributed tracing"
    )
    details: Optional[dict] = Field(
        None, description="Optional extra details (e.g., field-level validation errors)"
    )

    @field_validator("code", mode="before")
    @classmethod
    def _tolerate_unrecognized_code(cls, value: Any) -> Any:
        """Maps a code this SDK version does not know onto ErrorCode.UNKNOWN.

        `code` is chosen by the service, so treating an unrecognized value as a validation
        error would mean the service could not add an error code without breaking error
        reporting in every released SDK: the whole response would fail to deserialize and
        the human-readable message would be lost along with it. The message is the part
        users need, so it must survive a code this SDK cannot interpret.
        """
        if value is None:
            return ErrorCode.UNKNOWN
        if isinstance(value, ErrorCode):
            return value
        if isinstance(value, str):
            try:
                return ErrorCode(value)
            except ValueError:
                return ErrorCode.UNKNOWN
        return ErrorCode.UNKNOWN
