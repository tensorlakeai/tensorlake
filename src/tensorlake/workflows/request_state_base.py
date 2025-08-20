from typing import Dict

from .interface.request_context import RequestState
from .user_data_serializer import UserDataSerializer


class RequestStateBase(RequestState):
    """Base class with common functionality for request state implementations"""

    def __init__(
        self,
        input_serializer: UserDataSerializer,
        output_serializer: UserDataSerializer,
    ):
        self._input_serializer: UserDataSerializer = input_serializer
        self._output_serializer: UserDataSerializer = output_serializer
        self._timers: Dict[str, float] = {}
        self._counters: Dict[str, int] = {}

    def timer(self, name: str, value: float):
        self._timers[name] = value

    def counter(self, name: str, value: int = 1):
        self._counters[name] = self._counters.get(name, 0) + value
