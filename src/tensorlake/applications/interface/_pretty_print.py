from typing import Any

from .futures import (
    FunctionCallFuture,
    Future,
    MapFuture,
    ReduceOperationFuture,
    _InitialMissing,
    _InitialMissingType,
    _TensorlakeFutureWrapper,
    _unwrap_future,
)

# Limit the size of pretty printed Futures in error messages.
# This is because it can be a large object tree and it includes user objects too.
_DEFAULT_PRETTY_PRINT_CHAR_LIMIT = 1000
_PRETTY_PRINT_INDENT_STEP = 2


def pretty_print(
    obj: _TensorlakeFutureWrapper[Future] | Any,
    indent: int = 0,
    char_limit: int = _DEFAULT_PRETTY_PRINT_CHAR_LIMIT,
) -> str:
    """Pretty prints a Future or any other object with a character limit.

    The pretty printed string is very clear and is human readable.
    Doesn't raise any exceptions.
    """
    unwrapped: Future | Any = _unwrap_future(obj)
    if isinstance(unwrapped, MapFuture):
        return _pretty_print_list_future(unwrapped, indent, char_limit)
    elif isinstance(unwrapped, FunctionCallFuture):
        return _pretty_print_function_call_future(unwrapped, indent, char_limit)
    elif isinstance(unwrapped, ReduceOperationFuture):
        return _pretty_print_reduce_operation_future(unwrapped, indent, char_limit)
    else:
        try:
            # i.e. repr() returns "'foo'" instead of "foo".
            obj_str: str = repr(obj)
            if len(obj_str) > char_limit:
                obj_str = obj_str[: char_limit - 3] + "..."
            return obj_str
        except Exception:
            return f"<unprintable object of type {type(obj)}>"


def _pretty_print_list_future(future: MapFuture, indent: int, char_limit: int) -> str:
    prefix: str = f"Tensorlake Map Operation Future of '{future._function_name}' over "

    items: list[_TensorlakeFutureWrapper[Future] | Any] | MapFuture = _unwrap_future(
        future._items
    )
    if isinstance(items, MapFuture):
        return prefix + pretty_print(
            items, indent=indent, char_limit=char_limit - len(prefix)
        )

    indent_str: str = " " * indent
    strs: list[str] = [
        prefix,
        "[",
    ]
    if len(items) != 0:
        strs.append("\n")
    chars_count: int = sum(len(s) for s in strs)

    item_indent_str: str = " " * (indent + _PRETTY_PRINT_INDENT_STEP)
    for item in items:
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


def _pretty_print_function_call_future(
    future: FunctionCallFuture, indent: int, char_limit: int
) -> str:
    indent_str: str = " " * indent
    strs: list[str] = [
        "Tensorlake Function Call Future ",
        future._function_name,
        "(",
    ]
    if len(future._args) != 0 or len(future._kwargs) != 0:
        strs.append("\n")
    chars_count: int = sum(len(s) for s in strs)

    arg_indent_str: str = " " * (indent + _PRETTY_PRINT_INDENT_STEP)
    for arg in future._args:
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

    for arg_key, arg_value in future._kwargs.items():
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

    if len(future._args) != 0 or len(future._kwargs) != 0:
        strs.append(indent_str)
        chars_count += len(strs[-1])

    strs.append(")")
    chars_count += len(strs[-1])

    return "".join(strs)


def _pretty_print_reduce_operation_future(
    future: ReduceOperationFuture, indent: int, char_limit: int
) -> str:
    prefix: str = (
        f"Tensorlake Reduce Operation Future of '{future._function_name}' over "
    )
    initial: Future | Any | _InitialMissingType = _unwrap_future(future._initial)
    has_initial: bool = future._initial is not _InitialMissing

    items: list[_TensorlakeFutureWrapper[Future] | Any] | MapFuture = _unwrap_future(
        future._items
    )
    if isinstance(items, MapFuture):
        return prefix + pretty_print(
            items, indent=indent, char_limit=char_limit - len(prefix)
        )

    indent_str: str = " " * indent
    strs: list[str] = [
        prefix,
        "[",
    ]
    if has_initial or len(items) != 0:
        strs.append("\n")
    chars_count: int = sum(len(s) for s in strs)

    item_indent_str: str = " " * (indent + _PRETTY_PRINT_INDENT_STEP)

    if has_initial:
        initial_str: str = "".join(
            [
                item_indent_str,
                pretty_print(
                    initial,
                    indent=indent + _PRETTY_PRINT_INDENT_STEP,
                    char_limit=char_limit - chars_count,
                ),
                ",\n",
            ]
        )
        strs.append(initial_str)
        chars_count += len(initial_str)

    for input_item in items:
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

    if has_initial or len(items) != 0:
        strs.append(indent_str)
        chars_count += len(strs[-1])

    strs.append("]")
    chars_count += len(strs[-1])

    return "".join(strs)
