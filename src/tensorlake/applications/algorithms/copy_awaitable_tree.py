import copy

from ..interface.awaitables import (
    Awaitable,
    AwaitableList,
    FunctionCallAwaitable,
    ReduceOperationAwaitable,
)
from ..interface.exceptions import InternalError


def copy_awaitable_tree(root: Awaitable) -> Awaitable:
    """Returns a shallow copy of the supplied awaitable tree.

    Raises InternalError on error.
    """
    new_root: Awaitable = _copy_node(root)
    stack: list[Awaitable] = [new_root]

    while len(stack) > 0:
        copied_node = stack.pop()
        if isinstance(copied_node, AwaitableList):
            for index, item in enumerate(copied_node.items):
                if isinstance(item, Awaitable):
                    copied_item_node: Awaitable = _copy_node(item)
                    stack.append(copied_item_node)
                    copied_node.items[index] = copied_item_node
        elif isinstance(copied_node, ReduceOperationAwaitable):
            for index, input in enumerate(copied_node.inputs):
                if isinstance(input, Awaitable):
                    copied_input_node: Awaitable = _copy_node(input)
                    stack.append(copied_input_node)
                    copied_node.inputs[index] = copied_input_node
        elif isinstance(copied_node, FunctionCallAwaitable):
            for index, arg in enumerate(copied_node.args):
                if isinstance(arg, Awaitable):
                    copied_arg_node: Awaitable = _copy_node(arg)
                    stack.append(copied_arg_node)
                    copied_node.args[index] = copied_arg_node
            for key, kwarg in copied_node.kwargs.items():
                if isinstance(kwarg, Awaitable):
                    copied_kwarg_node: Awaitable = _copy_node(kwarg)
                    stack.append(copied_kwarg_node)
                    copied_node.kwargs[key] = copied_kwarg_node
        else:
            raise InternalError(f"Unexpected Awaitable type: {type(copied_node)}")

    return new_root


def _copy_node(node: Awaitable) -> Awaitable:
    """Returns shallow copy of the node without copying its child nodes."""
    if isinstance(node, AwaitableList):
        return AwaitableList(
            id=node.id,
            items=list(node.items),
            metadata=copy.copy(node.metadata),
        )
    elif isinstance(node, ReduceOperationAwaitable):
        return ReduceOperationAwaitable(
            id=node.id,
            function_name=node.function_name,
            inputs=list(node.inputs),
        )
    elif isinstance(node, FunctionCallAwaitable):
        return FunctionCallAwaitable(
            id=node.id,
            function_name=node.function_name,
            args=list(node.args),
            kwargs=dict(node.kwargs),
        )
    else:
        raise InternalError(f"Unexpected Awaitable type: {type(node)}")
