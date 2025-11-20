from typing import Any

from tensorlake.applications import InternalError, TensorlakeError
from tensorlake.applications.interface.request_context import RequestState
from tensorlake.applications.request_context.request_state import (
    REQUEST_STATE_USER_DATA_SERIALIZER,
)

from ..logger import FunctionExecutorLogger
from .request_state_proxy_server import RequestStateProxyServer


class ProxiedRequestState(RequestState):
    """RequestState that proxies the calls via RequestStateProxyServer."""

    def __init__(
        self,
        allocation_id: str,
        proxy_server: RequestStateProxyServer,
        logger: FunctionExecutorLogger,
    ):
        self._allocation_id: str = allocation_id
        self._proxy_server: RequestStateProxyServer = proxy_server
        self._logger: FunctionExecutorLogger = logger.bind(module=__name__)

    def set(self, key: str, value: Any) -> None:
        """Set a key-value pair."""
        # NB: This is called from user code, user code is blocked.
        # Any exception raised here goes directly to user code.
        try:
            self._proxy_server.set(
                self._allocation_id,
                key,
                REQUEST_STATE_USER_DATA_SERIALIZER.serialize(value),
            )
        except TensorlakeError:
            raise
        except Exception as e:
            self._logger.error(
                "Failed to set request state",
                exc_info=e,
                key=key,
            )
            raise InternalError(f"Failed to set request state for key '{key}'.")

    def get(self, key: str, default: Any | None = None) -> Any | None:
        """Get a value by key. If the key does not exist, return the default value."""
        # NB: This is called from user code, user code is blocked.
        # Any exception raised here goes directly to user code.
        try:
            value: bytes | None = self._proxy_server.get(self._allocation_id, key)
            # possible_types=[] because pickle deserializer knows the target type already.
            return (
                default
                if value is None
                else REQUEST_STATE_USER_DATA_SERIALIZER.deserialize(
                    value, possible_types=[]
                )
            )
        except TensorlakeError:
            raise
        except Exception as e:
            self._logger.error(
                "Failed to get request state",
                exc_info=e,
                key=key,
            )
            raise InternalError(f"Failed to get request state for key '{key}'.")
