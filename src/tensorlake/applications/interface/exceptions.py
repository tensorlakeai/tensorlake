class TensorlakeException(Exception):
    """Base class for all Tensorlake exceptions."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class TensorlakeError(TensorlakeException):
    """Base class for all errors in Tensorlake Applications."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class RequestNotFinished(TensorlakeError):
    """Raised when trying to access the output of a request that is not yet finished."""

    def __init__(self) -> None:
        super().__init__("Request is still in progress, its output is not ready yet.")


class RequestFailed(TensorlakeError):
    """Raised when trying to access the output of a request that has failed."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class RequestError(RequestFailed):
    """Raised by a Tensorlake Function when it decides to fail the current request.

    The calling Tensorlake Function can catch the exception to avoid failing the request.
    If not caught, the exception will still cause the request to fail.

    The message passed to this exception is preserved exactly as is and will be returned
    to the user as the error message for the failed request. If SDK is used to get the request
    output then it'll raise RequestError with the same message.
    """

    def __init__(self, message: str) -> None:
        super().__init__(message)

    @property
    def message(self) -> str:
        return self.args[0]


class RemoteAPIError(TensorlakeError):
    """Raised when a remote Tensorlake Applications API call failed."""

    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code

    def __str__(self):
        return f"{super().__str__()} (Status Code: {self.status_code})"


class SDKUsageError(TensorlakeError):
    """Raised when Applications SDK is used incorrectly."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class SerializationError(TensorlakeError):
    """Raised when serialization of request input, output or Tensorlake Function parameters failed."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class DeserializationError(TensorlakeError):
    """Raised when deserialization of request input, output or Tensorlake Function parameters failed."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


# NB: all SDK code paths directly called from Tensorlake Functions must catch all
# unexpected exceptions and re-raise them as InternalError.
class InternalError(TensorlakeError):
    """Raised when an internal error occurs in Tensorlake Applications SDK."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class FunctionError(TensorlakeError):
    """Raised when a Tensorlake Function failed and the calling function tries to get its result.

    If the failed function raised RequestError, it will be re-raised in the calling function
    instead of FunctionError.
    """

    def __init__(self, message: str) -> None:
        super().__init__(message)


class TimeoutError(TensorlakeError):
    """Raised when an operation timeout expires."""

    def __init__(self) -> None:
        super().__init__("Timed out.")
