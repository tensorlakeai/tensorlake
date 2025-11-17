from typing import Any

from ..request_context.contextvar import get_current_request_context
from .exceptions import InternalError


class RequestState:
    """Abstract interface for request state key-value API.

    The API allows to set and get key-value pairs from Indexify functions.
    The key-value pairs are scoped per application request.
    Each new request starts with an empty state (empty set of key-value pairs).
    A value can be any serializable object, the serializer of the current function
    is used to serialize and deserialize values."""

    def set(self, key: str, value: Any) -> None:
        """Set a key-value pair.

        Raises SerializationError if failed to serialize the value.
        Raises TensorlakeError on other errors.
        """
        raise InternalError("RequestState subclasses must implement set method.")

    def get(self, key: str, default: Any | None = None) -> Any | None:
        """Get a value by key. If the key does not exist, return the default value.

        Raises DeserializationError if failed to deserialize the value.
        Raises TensorlakeError on other errors."""
        raise InternalError("RequestState subclasses must implement get method.")


class RequestMetrics:
    """Abstract interface for reporting application request metrics."""

    def timer(self, name: str, value: float):
        """Records a duration metric with the supplied name and value.

        Raises TensorlakeError on error.
        """
        raise InternalError("RequestMetrics subclasses must implement timer method.")

    def counter(self, name: str, value: int = 1):
        """Adds the supplied value to the counter with the supplied name.

        If the counter does not exist, it is created with the supplied value.

        Raises TensorlakeError on error.
        """
        raise InternalError("RequestMetrics subclasses must implement counter method.")


class FunctionProgress:
    """Abstract interface for reporting Tensorlake Function call progress."""

    def update(self, current: float, total: float) -> None:
        """Update the progress of the current Tensorlake Function call execution.

        Args:
            current: Current request progress value
            total: Total request progress value

        Raises TensorlakeError on error.
        """
        raise InternalError("FunctionProgress subclasses must implement update method.")


class RequestContext:
    """Abstract interface for request context."""

    @classmethod
    def get(cls) -> "RequestContext":
        """Returns context of the running request.

        Raises SDKUsageError if called outside of a Tensorlake Function call
        or if called from a thread spawned by a Tensorlake Function.
        """
        return get_current_request_context()

    @property
    def request_id(self) -> str:
        raise InternalError(
            "RequestContext subclasses must implement request_id property."
        )

    @property
    def state(self) -> RequestState:
        raise InternalError("RequestContext subclasses must implement state property.")

    @property
    def progress(self) -> FunctionProgress:
        raise InternalError(
            "RequestContext subclasses must implement progress property."
        )

    @property
    def metrics(self) -> RequestMetrics:
        raise InternalError(
            "RequestContext subclasses must implement metrics property."
        )
