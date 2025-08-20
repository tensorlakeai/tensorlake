from typing import Any, List

from ..function.user_data_serializer import function_input_serializer
from ..interface.function import Function
from ..interface.future import FutureList
from ..interface.reduce import ReducerFunctionCall
from ..registry import get_function
from ..user_data_serializer import UserDataSerializer
from .ast import ast_from_user_object
from .ast_node import ASTNode
from .value_node import ValueNode


# A reducer call can't have any metadata because Indexify Server protocol
# currently doesn't allow that.
class ReducerFunctionCallNode(ASTNode):
    def __init__(self, reducer_function_name: str):
        super().__init__()
        self._reducer_function_name: str = reducer_function_name
        self._inputs: List[ASTNode] = []
        self._initial: ASTNode | None = None

    @property
    def reducer_function_name(self) -> str:
        return self._reducer_function_name

    @property
    def inputs(self) -> List[ASTNode]:
        return self._inputs

    @property
    def initial(self) -> ASTNode | None:
        return self._initial

    def replace_child(self, old_child: "ASTNode", new_child: "ASTNode") -> None:
        """Replaces an old child node with a new child node.

        The old child node is not valid after the replacement."""
        super().replace_child(old_child, new_child)
        if self._initial is not None and old_child.id == self._initial.id:
            self._initial = new_child
            return

        # Linear lookup using ASTNode equality operator.
        old_child_inputs_ix: int = self._inputs.index(old_child)
        self._inputs[old_child_inputs_ix] = new_child

    def to_reducer_function_call(
        node: "ReducerFunctionCallNode",
    ) -> ReducerFunctionCall:
        """Converts the node back to its original ReducerFunctionCall.

        All children must be value nodes (they must already be resolved/finished).
        """
        node._initial: ValueNode | None
        initial: Any = node._initial.to_value() if node._initial is not None else None

        inputs: List[Any] = []
        for input_node in node._inputs:
            input_node: ValueNode
            inputs.append(input_node.to_value())

        return ReducerFunctionCall(
            reducer_function_name=node._reducer_function_name,
            inputs=FutureList(inputs),
            is_initial_missing=node._initial is None,
            initial=initial,
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
        node._initial = (
            None
            if reducer_call.is_initial_missing
            else ast_from_user_object(reducer_call.initial, input_serializer)
        )
        for input in reducer_call.inputs.items:
            input_node: ASTNode = ast_from_user_object(input, input_serializer)
            input_node.parent = node
            node.children[input_node.id] = input_node
            node._inputs.append(input_node)

        return node
