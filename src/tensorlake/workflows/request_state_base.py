from typing import Dict

from .interface.request_context import RequestState
from .user_data_serializer import UserDataSerializer


class RequestStateBase(RequestState):
    """Base class with common functionality for request state implementations"""

    def __init__(
        self,
        user_serializer: UserDataSerializer,
    ):
        self._user_serializer: UserDataSerializer = user_serializer
        self._timers: Dict[str, float] = {}
        self._counters: Dict[str, int] = {}

    @property
    def timers(self) -> Dict[str, float]:
        return self._timers

    def timer(self, name: str, value: float):
        self._timers[name] = value

    @property
    def counters(self) -> Dict[str, int]:
        return self._counters

    def counter(self, name: str, value: int = 1):
        self._counters[name] = self._counters.get(name, 0) + value
