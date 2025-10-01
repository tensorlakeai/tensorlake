from typing import Any

from tensorlake.applications.interface.request_context import RequestState
from tensorlake.applications.request_context.request_state import (
    REQUEST_STATE_USER_DATA_SERIALIZER,
)

from .request_state_proxy_server import RequestStateProxyServer


class ProxiedRequestState(RequestState):
    """RequestState that proxies the calls via RequestStateProxyServer."""

    def __init__(
        self,
        allocation_id: str,
        proxy_server: RequestStateProxyServer,
    ):
        self._allocation_id: str = allocation_id
        self._proxy_server: RequestStateProxyServer = proxy_server

    def set(self, key: str, value: Any) -> None:
        """Set a key-value pair."""
        self._proxy_server.set(
            self._allocation_id,
            key,
            REQUEST_STATE_USER_DATA_SERIALIZER.serialize(value),
        )

    def get(self, key: str, default: Any | None = None) -> Any | None:
        """Get a value by key. If the key does not exist, return the default value."""
        value: bytes | None = self._proxy_server.get(self._allocation_id, key)
        # possible_types=[] because pickle deserializer knows the target type already.
        return (
            default
            if value is None
            else REQUEST_STATE_USER_DATA_SERIALIZER.deserialize(
                value, possible_types=[]
            )
        )
