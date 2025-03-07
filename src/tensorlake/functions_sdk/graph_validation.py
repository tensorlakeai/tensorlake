import inspect
import re
from typing import List, Type, Union

from .functions import TensorlakeCompute, TensorlakeRouter


def validate_node(indexify_fn: Union[Type[TensorlakeCompute], Type[TensorlakeRouter]]):
    if inspect.isfunction(indexify_fn):
        raise Exception(
            f"Unable to add node of type `{type(indexify_fn)}`. "
            f"Required, `TensorlakeCompute` or `TensorlakeRouter`"
        )
    if not (
        issubclass(indexify_fn, TensorlakeCompute)
        or issubclass(indexify_fn, TensorlakeRouter)
    ):
        raise Exception(
            f"Unable to add node of type `{indexify_fn.__name__}`. "
            f"Required, `TensorlakeCompute` or `TensorlakeRouter`"
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


def validate_route(
    from_node: Type[TensorlakeRouter], to_nodes: List[Type[TensorlakeCompute]]
):
    signature = inspect.signature(from_node.run)

    if signature.return_annotation == inspect.Signature.empty:
        raise Exception(f"Function {from_node.name} has empty return type annotation")

    return_annotation = signature.return_annotation

    if (
        hasattr(return_annotation, "__origin__")
        and return_annotation.__origin__ is Union
    ):
        for arg in return_annotation.__args__:
            if hasattr(arg, "name"):
                if arg not in to_nodes:
                    raise Exception(
                        f"Unable to find {arg.name} in to_nodes {[node.name for node in to_nodes]}"
                    )
    elif (
        hasattr(return_annotation, "__origin__")
        and return_annotation.__origin__ is list
    ):
        union_args = return_annotation.__args__[0].__args__
        for arg in union_args:
            if hasattr(arg, "name"):
                if arg not in to_nodes:
                    raise Exception(
                        f"Unable to find {arg.name} in to_nodes {[node.name for node in to_nodes]}"
                    )
    else:
        raise Exception(f"Return type of {from_node.name} is not a Union")
