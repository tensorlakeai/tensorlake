"""
This file contains exceptions that can be raised by Tensorlake functions.
"""

_MAX_INVOCATION_ERROR_MESSAGE_LENGTH = 5 * 1024  # 5 KiB


class InvocationError(Exception):
    """
    Raised when the current graph invocation cannot complete.

    Permanently fails the invocation.

    Args:
        message (str): A description of the error that caused the invocation to fail.
                       max length is 5 KiB.

    Attributes:
        message (str): The error message provided during initialization.
    """

    def __init__(self, message: str):
        if len(message) > _MAX_INVOCATION_ERROR_MESSAGE_LENGTH:
            message = message[:_MAX_INVOCATION_ERROR_MESSAGE_LENGTH]
            print(
                "Warning: InvocationError message truncated to {_MAX_INVOCATION_ERROR_MESSAGE_LENGTH} characters"
            )
        super().__init__(message)

    @property
    def message(self) -> str:
        return self.args[0]
