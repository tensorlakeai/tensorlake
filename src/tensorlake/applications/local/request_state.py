from typing import Any, Dict

from ..interface.request_context import RequestState
from ..request_context.request_state import REQUEST_STATE_USER_DATA_SERIALIZER


class LocalRequestState(RequestState):
    def __init__(self):
        # Store all data in serialized form to be consistent with remote mode.
        self._state: Dict[str, bytes] = {}

    def set(self, key: str, value: Any) -> None:
        self._state[key] = REQUEST_STATE_USER_DATA_SERIALIZER.serialize(value)

    def get(self, key: str, default: Any | None = None) -> Any | None:
        serialized_value: bytes | None = self._state.get(key, None)
        # possible_types=[] because pickle deserializer knows the target type already.
        return (
            default
            if serialized_value is None
            else REQUEST_STATE_USER_DATA_SERIALIZER.deserialize(
                serialized_value, possible_types=[]
            )
        )
