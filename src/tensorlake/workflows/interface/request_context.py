from typing import Any


class RequestState:
    """Abstract interface for request state key-value API.

    The API allows to set and get key-value pairs from Indexify functions.
    The key-value pairs are scoped per Graph request.
    Each new request starts with an empty state (empty set of key-value pairs).
    A value can be any serializable object, the serializer of the current function
    is used to serialize and deserialize values."""

    def set(self, key: str, value: Any) -> None:
        """Set a key-value pair.

        Raises Exception if an error occurred."""
        raise NotImplementedError()

    def get(self, key: str, default: Any | None = None) -> Any | None:
        """Get a value by key. If the key does not exist, return the default value.

        Raises Exception if an error occurred."""
        raise NotImplementedError()


class RequestMetrics:
    """Abstract interface for reporting graph request metrics."""

    def timer(self, name: str, value: float):
        """Records a duration metric with the supplied name and value."""
        raise NotImplementedError()

    def counter(self, name: str, value: int = 1):
        """Adds the supplied value to the counter with the supplied name.

        If the counter does not exist, it is created with the supplied value."""
        raise NotImplementedError()


class RequestProgress:
    """Abstract interface for reporting graph request progress."""

    def update(self, current: float, total: float) -> None:
        """Update the progress of the current task execution.

        Args:
            current: Current progress value
            total: Total progress value
        """
        raise NotImplementedError()


class RequestContext:
    """Abstract interface for request context."""

    @property
    def request_id(self) -> str:
        raise NotImplementedError()

    @property
    def state(self) -> RequestState:
        raise NotImplementedError()

    @property
    def progress(self) -> RequestProgress:
        raise NotImplementedError()

    @property
    def metrics(self) -> RequestMetrics:
        raise NotImplementedError()


class RequestContextPlaceholder(RequestContext):
    """A placeholder used by user code in place of RequestContext when calling a @tensorlake_function.

    Inherits from RequestContext just to show explicitly that it can be passed into a function call
    instead of an actual RequestContext instance. Must be serializable by any user serializer because
    this object is stored in user args and kwargs.
    """

    def __eq__(self, value):
        # A placeholder is equal to any other placeholder.
        # This allows to easily compare function calls in tests.
        return isinstance(self, RequestContextPlaceholder) and isinstance(
            value, RequestContextPlaceholder
        )
