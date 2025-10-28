class TensorlakeException(Exception):
    """Base class for all Tensorlake exceptions."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class ApplicationValidationError(TensorlakeException):
    """Raised when an error is detected in application configuration."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        # TODO: Add line number, file name, code snippet, etc.


class RemoteAPIError(TensorlakeException):
    """Raised when a remote API call fails."""

    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code

    def __str__(self):
        return f"{super().__str__()} (Status Code: {self.status_code})"


class FunctionCallFailure(TensorlakeException):
    """Raised when a function call failed."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class RequestNotFinished(TensorlakeException):
    """Raised when trying to access the output of a request that is not yet finished."""

    def __init__(self) -> None:
        super().__init__("request is still in progress, its output is not ready yet")


class RequestFailureException(TensorlakeException):
    """
    Raised if request failed.

    Args:
        message (str): Cause of the request failure.

    Attributes:
        message (str): The error message provided during initialization.
    """

    def __init__(self, message: str):
        super().__init__(message)

    @property
    def message(self) -> str:
        return self.args[0]


class RequestError(RequestFailureException):
    """
    Raised by a Tensorlake Function code when it needs to fail the current request.

    Permanently fails the current request.

    Args:
        message (str): A description of the error that caused the request failure.

    Attributes:
        message (str): The error message provided during initialization.
    """

    def __init__(self, message: str):
        super().__init__(message)

    @property
    def message(self) -> str:
        return self.args[0]
