from typing import Any

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
        return ReducerFunctionCallNode.from_reducer_function_call(user_object)
    else:
        return ValueNode.from_value(user_object, value_serializer)
