import inspect
from typing import Type, Union

from .functions import RouteTo, TensorlakeCompute


def validate_node(indexify_fn: Type[TensorlakeCompute]):
    if inspect.isfunction(indexify_fn):
        raise Exception(
            f"Unable to add node of type `{type(indexify_fn)}`. "
            f"Required, `TensorlakeCompute`"
        )
    if not (issubclass(indexify_fn, TensorlakeCompute)):
        raise Exception(
            f"Unable to add node of type `{indexify_fn.__name__}`. "
            f"Required, `TensorlakeCompute`"
        )

    signature = inspect.signature(indexify_fn.run)

    for param in signature.parameters.values():
        if param.name == "self":
            continue
        if param.annotation == inspect.Parameter.empty:
            raise Exception(
                f"Input param {param.name} in {indexify_fn.name} has empty"
                f" type annotation"
            )

    if signature.return_annotation == inspect.Signature.empty:
        raise Exception(f"Function {indexify_fn.name} has empty return type annotation")

    return_annotation = signature.return_annotation

    next_fns = (
        indexify_fn.next if isinstance(indexify_fn.next, list) else [indexify_fn.next]
    )

    def validate_route_arg(arg):
        if arg not in next_fns:
            raise Exception(
                f"Unable to find '{arg.name}' in available next nodes: {[node.name for node in next_fns]}"
            )

    if (
        hasattr(return_annotation, "__origin__")
        and return_annotation.__origin__ is RouteTo
    ):
        if len(return_annotation.__args__) != 2:
            raise Exception(
                f"Incorrect type parameters for RouteTo: {return_annotation.__args__}"
            )
        route_type = return_annotation.__args__[1]
        if hasattr(route_type, "__origin__") and route_type.__origin__ is Union:
            for arg in route_type.__args__:
                validate_route_arg(arg)
        else:
            validate_route_arg(route_type)
