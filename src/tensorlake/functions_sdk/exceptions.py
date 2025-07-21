class ApiException(Exception):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code

    def __str__(self):
        return f"{super().__str__()} (Status Code: {self.status_code})"


class GraphStillProcessing(Exception):
    def __init__(self) -> None:
        super().__init__("graph is still processing")


class RequestException(Exception):
    """
    Raised when the current graph request cannot complete.

    Permanently fails the request.

    Args:
        message (str): A description of the error that caused the invocation to fail.

    Attributes:
        message (str): The error message provided during initialization.
    """

    def __init__(self, message: str):
        super().__init__(message)

    @property
    def message(self) -> str:
        return self.args[0]
