from typing import Any, Dict, Optional

from ..request_state_base import RequestStateBase
from ..user_data_serializer import UserDataSerializer


class LocalRequestState(RequestStateBase):
    """RequestState that stores the key-value pairs in memory.

    This is intended to be used with local graphs."""

    def __init__(
        self,
        input_serializer: UserDataSerializer,
        output_serializer: UserDataSerializer,
        initial_state: Dict[str, bytes],
    ):
        """Creates a new instance.

        Caller needs to ensure that the returned instance is only used for a single request state.
        """
        super().__init__(input_serializer, output_serializer)
        self._state: Dict[str, bytes] = initial_state

    def set(self, key: str, value: Any) -> None:
        # It's important to serialize the value even in the local implementation
        # so there are no unexpected errors when running in remote graph mode.

        self._state[key] = self._output_serializer.serialize(value)

    def get(self, key: str, default: Any | None = None) -> Optional[Any]:
        type_hint: Any | None = type(default) if default is not None else None
        serialized_value: Optional[bytes] = self._state.get(key, None)
        return (
            default
            if serialized_value is None
            else self._input_serializer.deserialize(serialized_value, type_hint)
        )

    def get_state(self) -> Dict[str, bytes]:
        return self._state
