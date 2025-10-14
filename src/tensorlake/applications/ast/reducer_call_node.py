from typing import Any, List

from ..function.user_data_serializer import function_input_serializer
from ..interface.function import Function
from ..interface.futures import ReduceOperationFuture
from ..registry import get_function
from ..user_data_serializer import UserDataSerializer
from .ast import ast_from_user_object
from .ast_node import ASTNode, ASTNodeMetadata
from .collection import CollectionMetadata
from .value_node import ValueNode


class ReducerFunctionCallMetadata(ASTNodeMetadata):
    # Output serializer name override if any.
    oso: str | None


class ReducerFunctionCallNode(ASTNode):
    def __init__(self, id: str, reducer_function_name: str):
        super().__init__(id)
        self._reducer_function_name: str = reducer_function_name
        self._inputs_metadata: CollectionMetadata = CollectionMetadata(nids=[])

    @property
    def reducer_function_name(self) -> str:
        return self._reducer_function_name

    def to_reducer_function_call(self) -> ReduceOperationFuture:
        """Converts the node back to its original ReducerFunctionCall.

        All children must be value nodes (they must already be resolved/finished).
        """
        inputs: List[Any] = []
        for input_node_id in self._inputs_metadata.nids:
            input_node: ValueNode = self.children[input_node_id]
            inputs.append(input_node.value)

        return ReduceOperationFuture(
            id=self.id,
            reducer_function_name=self.reducer_function_name,
            inputs=inputs,
            start_delay=None,
        )

    @classmethod
    def from_reducer_function_call(
        cls, reducer_call: ReduceOperationFuture
    ) -> "ReducerFunctionCallNode":
        function: Function = get_function(reducer_call._function_name)
        input_serializer: UserDataSerializer = function_input_serializer(function)
        node: ReducerFunctionCallNode = ReducerFunctionCallNode(
            id=reducer_call.id, reducer_function_name=reducer_call._function_name
        )
        for input in reducer_call._inputs:
            input_node: ASTNode = ast_from_user_object(input, input_serializer)
            node.add_child(input_node)
            node._inputs_metadata.nids.append(input_node.id)

        node.metadata = ReducerFunctionCallMetadata(
            nid=node.id,
            # Set by the node parent after this node is created.
            oso=None,
        )

        return node
