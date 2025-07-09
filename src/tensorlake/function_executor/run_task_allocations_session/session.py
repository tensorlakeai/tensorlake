from typing import Any, Generator, Iterator

from ..proto.function_executor_pb2 import (
    RunTaskAllocationsSessionClientMessage,
    RunTaskAllocationsSessionServerMessage,
    Status,
)
from .message_validators import validate_client_session_message


class CloseSession(Exception):
    """Exception raised when a session is closed.

    This is similar to StopIteration exception."""

    def __init__(self, message: str):
        super().__init__(message)


class RunTaskAllocationsSession:
    def __init__(self, id: str, logger: Any):
        self._id = id
        self._logger = logger.bind(module=__name__, session_id=id)

    def join(
        self, client_stream: Iterator[RunTaskAllocationsSessionClientMessage]
    ) -> Generator[RunTaskAllocationsSessionServerMessage, None, None]:
        """Starts processing the client messages in the scope of the session

        Raises CloseSession when the session is fully closed."""
        self._logger.info("joined session")
        for message in client_stream:
            message: RunTaskAllocationsSessionClientMessage
            validate_client_session_message(message)
            yield message
            # TODO if close session message is received, raise CloseSession exception
