"""
This file contains exceptions that can be raised by Tensorlake functions.
"""


class InvocationError(Exception):
    """
    Raised when the current graph invocation cannot complete.

    Permanently fails the invocation.

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
