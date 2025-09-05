from typing import Any, Dict

from ..ast.value_node import ValueNode
from ..request_state_base import RequestStateBase
from ..user_data_serializer import UserDataSerializer


class LocalRequestState(RequestStateBase):
    def __init__(
        self,
        user_serializer: UserDataSerializer,
        state: Dict[str, ValueNode],
    ):
        super().__init__(user_serializer)
        # Use ValueNode to store serialized value and its metadata.
        # This makes local UX close to remote where serialized values
        # are stored outside of FE.
        self._state: Dict[str, ValueNode] = state

    def set(self, key: str, value: Any) -> None:
        self._state[key] = ValueNode.from_value(value, self._user_serializer)

    def get(self, key: str, default: Any | None = None) -> Any | None:
        value_node: ValueNode | None = self._state.get(key, None)
        return default if value_node is None else value_node.to_value()
