from typing import Any

from tensorlake.workflows.request_state_base import RequestStateBase
from tensorlake.workflows.user_data_serializer import UserDataSerializer

from .request_state_proxy_server import RequestStateProxyServer


class ProxiedRequestState(RequestStateBase):
    """RequestState that proxies the calls via RequestStateProxyServer."""

    def __init__(
        self,
        allocation_id: str,
        proxy_server: RequestStateProxyServer,
        user_serializer: UserDataSerializer,
    ):
        super().__init__(user_serializer)
        self._allocation_id: str = allocation_id
        self._proxy_server: RequestStateProxyServer = proxy_server

    def set(self, key: str, value: Any) -> None:
        """Set a key-value pair."""
        self._proxy_server.set(
            self._allocation_id, key, self._user_serializer.serialize(value)
        )

    def get(self, key: str, default: Any | None = None) -> Any | None:
        """Get a value by key. If the key does not exist, return the default value."""
        value: bytes | None = self._proxy_server.get(self._allocation_id, key)
        return default if value is None else self._user_serializer.deserialize(value)
