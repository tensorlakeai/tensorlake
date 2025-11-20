from typing import Any

from .awaitables import (
    Awaitable,
    AwaitableList,
    FunctionCallAwaitable,
    Future,
    ReduceOperationAwaitable,
)
from .exceptions import InternalError

# Limit the size of pretty printed Awaitables and Futures in error messages.
# This is because it can be a large object tree and it includes user objects too.
_DEFAULT_PRETTY_PRINT_CHAR_LIMIT = 1000
_PRETTY_PRINT_INDENT_STEP = 2


def pretty_print(
    obj: Awaitable | Future | Any,
    indent: int = 0,
    char_limit: int = _DEFAULT_PRETTY_PRINT_CHAR_LIMIT,
) -> str:
    if isinstance(obj, Awaitable):
        if isinstance(obj, AwaitableList):
            return _pretty_print_awaitable_list(obj, indent, char_limit)
        elif isinstance(obj, FunctionCallAwaitable):
            return _pretty_print_function_call_awaitable(obj, indent, char_limit)
        elif isinstance(obj, ReduceOperationAwaitable):
            return _pretty_print_reduce_operation_awaitable(obj, indent, char_limit)
        else:
            raise InternalError(f"Unknown Awaitable type: {type(obj)}")
    elif isinstance(obj, Future):
        # __str__ does pretty print for futures.
        return str(obj)
    else:
        try:
            # i.e. repr() returns "'foo'" instead of "foo".
            obj_str: str = repr(obj)
            if len(obj_str) > char_limit:
                obj_str = obj_str[: char_limit - 3] + "..."
            return obj_str
        except Exception:
            return f"<unprintable object of type {type(obj)}>"


def _pretty_print_awaitable_list(
    awaitable: AwaitableList, indent: int, char_limit: int
) -> str:
    indent_str: str = " " * indent
    strs: list[str] = [awaitable.kind_str, " ["]
    if len(awaitable.items) != 0:
        strs.append("\n")
    chars_count: int = sum(len(s) for s in strs)

    item_indent_str: str = " " * (indent + _PRETTY_PRINT_INDENT_STEP)
    for item in awaitable.items:
        item_str: str = "".join(
            [
                item_indent_str,
                pretty_print(
                    item,
                    indent=indent + _PRETTY_PRINT_INDENT_STEP,
                    char_limit=char_limit - chars_count,
                ),
                ",\n",
            ]
        )
        if chars_count + len(item_str) >= char_limit:
            strs.append(item_indent_str)
            strs.append("...,\n")
            chars_count += len(strs[-2]) + len(strs[-1])
            break
        else:
            strs.append(item_str)
            chars_count += len(item_str)

    strs.append(indent_str + "]")
    return "".join(strs)


def _pretty_print_function_call_awaitable(
    awaitable: FunctionCallAwaitable, indent: int, char_limit: int
) -> str:
    indent_str: str = " " * indent
    strs: list[str] = [
        "Tensorlake Function Call ",
        awaitable.function_name,
        "(",
    ]
    if len(awaitable.args) != 0 or len(awaitable.kwargs) != 0:
        strs.append("\n")
    chars_count: int = sum(len(s) for s in strs)

    arg_indent_str: str = " " * (indent + _PRETTY_PRINT_INDENT_STEP)
    for arg in awaitable.args:
        arg_str: str = "".join(
            [
                arg_indent_str,
                pretty_print(
                    arg,
                    indent=indent + _PRETTY_PRINT_INDENT_STEP,
                    char_limit=char_limit - chars_count,
                ),
                ",\n",
            ]
        )

        if chars_count + len(arg_str) >= char_limit:
            strs.append(arg_indent_str)
            strs.append("...,\n")
            chars_count += len(strs[-2]) + len(strs[-1])
            break
        else:
            strs.append(arg_str)
            chars_count += len(arg_str)

    for arg_key, arg_value in awaitable.kwargs.items():
        kwarg_str: str = "".join(
            [
                arg_indent_str,
                f"{arg_key}=",
                pretty_print(
                    arg_value,
                    indent=indent + _PRETTY_PRINT_INDENT_STEP,
                    char_limit=char_limit - chars_count,
                ),
                ",\n",
            ]
        )

        if chars_count + len(kwarg_str) >= char_limit:
            strs.append(arg_indent_str)
            strs.append("...,\n")
            chars_count += len(strs[-2]) + len(strs[-1])
            break
        else:
            strs.append(kwarg_str)
            chars_count += len(kwarg_str)

    if len(awaitable.args) != 0 or len(awaitable.kwargs) != 0:
        strs.append(indent_str)
        chars_count += len(strs[-1])

    strs.append(")")
    chars_count += len(strs[-1])

    return "".join(strs)


def _pretty_print_reduce_operation_awaitable(
    awaitable: ReduceOperationAwaitable, indent: int, char_limit: int
) -> str:
    indent_str: str = " " * indent
    strs: list[str] = [
        "Tensorlake Reduce Operation of '",
        awaitable.function_name,
        "' over [",
    ]
    if len(awaitable.inputs) != 0:
        strs.append("\n")
    chars_count: int = sum(len(s) for s in strs)

    item_indent_str: str = " " * (indent + _PRETTY_PRINT_INDENT_STEP)
    for input_item in awaitable.inputs:
        input_item_str: str = "".join(
            [
                item_indent_str,
                pretty_print(
                    input_item,
                    indent=indent + _PRETTY_PRINT_INDENT_STEP,
                    char_limit=char_limit - chars_count,
                ),
                ",\n",
            ]
        )

        if chars_count + len(input_item_str) >= char_limit:
            strs.append(item_indent_str)
            strs.append("...,\n")
            chars_count += len(strs[-2]) + len(strs[-1])
            break
        else:
            strs.append(input_item_str)
            chars_count += len(input_item_str)

    if len(awaitable.inputs) != 0:
        strs.append(indent_str)
        chars_count += len(strs[-1])

    strs.append("]")
    chars_count += len(strs[-1])

    return "".join(strs)
