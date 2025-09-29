from typing import Any, Dict

from ..interface.function_call import RegularFunctionCall
from ..interface.reduce import ReducerFunctionCall
from ..user_data_serializer import UserDataSerializer
from .ast_node import ASTNode


def ast_from_user_object(
    user_object: Any,
    value_serializer: UserDataSerializer,
) -> ASTNode:
    """Returns AST built from the supplied object constructed by user.

    Typically a user_object is a SDK object constructed by user using calls to tensorlake.*.
    Or it's a value created by user, e.g. a function output.
    The supplied value serializer is used to serialize the user object if it's a value, not
    an SDK object.
    """
    # import locally to resolve circular dependencies.
    from .function_call_node import RegularFunctionCallNode
    from .reducer_call_node import ReducerFunctionCallNode
    from .value_node import ValueNode

    if isinstance(user_object, RegularFunctionCall):
        return RegularFunctionCallNode.from_regular_function_call(user_object)
    elif isinstance(user_object, ReducerFunctionCall):
        if len(user_object.inputs.items) >= 2:
            return ReducerFunctionCallNode.from_reducer_function_call(user_object)
        else:
            # Return the single item directly, no need to create a reducer call node.
            # This is important, because otherwise this item will be serialized using
            # the reducer input serializer and then the item will be returned as
            # reducer call output which means reducer call output serializer will not be used.
            # ReducerFunctionCall.inputs are guaranteed to have at least one item.
            return ast_from_user_object(user_object.inputs.items[0], value_serializer)
    else:
        return ValueNode.from_value(user_object, value_serializer)


def flatten_ast(root: ASTNode) -> Dict[str, ASTNode]:
    """Flattens the AST into a dictionary mapping node IDs to nodes."""
    flattened = {}

    def _flatten(node: ASTNode):
        flattened[node.id] = node
        for child in node.children.values():
            _flatten(child)

    _flatten(root)
    return flattened


def traverse_ast(
    root: ASTNode,
):
    """Traverses the AST and yields each node."""
    yield root
    for child in root.children.values():
        yield from traverse_ast(child)
