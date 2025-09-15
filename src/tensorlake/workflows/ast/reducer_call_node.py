from typing import Any, List

from ..function.user_data_serializer import function_input_serializer
from ..interface.function import Function
from ..interface.future import FutureList
from ..interface.reduce import ReducerFunctionCall
from ..registry import get_function
from ..user_data_serializer import UserDataSerializer
from .ast import ast_from_user_object
from .ast_node import ASTNode
from .future_list_metadata import FutureListMetadata
from .value_node import ValueNode


class ReducerFunctionCallNode(ASTNode):
    def __init__(self, reducer_function_name: str):
        super().__init__()
        self._reducer_function_name: str = reducer_function_name
        self._inputs: FutureListMetadata = FutureListMetadata(nids=[])

    @property
    def reducer_function_name(self) -> str:
        return self._reducer_function_name

    def to_reducer_function_call(self) -> ReducerFunctionCall:
        """Converts the node back to its original ReducerFunctionCall.

        All children must be value nodes (they must already be resolved/finished).
        """
        inputs: List[Any] = []
        for input_node_id in self._inputs.nids:
            input_node: ValueNode = self.children[input_node_id]
            inputs.append(input_node.to_value())

        return ReducerFunctionCall(
            reducer_function_name=self.reducer_function_name,
            inputs=FutureList(inputs),
        )

    @classmethod
    def from_reducer_function_call(
        cls, reducer_call: ReducerFunctionCall
    ) -> "ReducerFunctionCallNode":
        function: Function = get_function(reducer_call.function_name)
        input_serializer: UserDataSerializer = function_input_serializer(function)
        node: ReducerFunctionCallNode = ReducerFunctionCallNode(
            reducer_call.function_name
        )
        for input in reducer_call.inputs.items:
            input_node: ASTNode = ast_from_user_object(input, input_serializer)
            node.add_child(input_node)
            node._inputs.nids.append(input_node.id)

        return node
