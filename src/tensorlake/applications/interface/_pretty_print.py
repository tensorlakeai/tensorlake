from typing import Any

from .futures import (
    FunctionCallFuture,
    Future,
    ListFuture,
    ReduceOperationFuture,
    _FutureListKind,
)

# Limit the size of pretty printed Futures in error messages.
# This is because it can be a large object tree and it includes user objects too.
_DEFAULT_PRETTY_PRINT_CHAR_LIMIT = 1000
_PRETTY_PRINT_INDENT_STEP = 2


def pretty_print(
    obj: Future | Any,
    indent: int = 0,
    char_limit: int = _DEFAULT_PRETTY_PRINT_CHAR_LIMIT,
) -> str:
    """Pretty prints a Future or any other object with a character limit.

    The pretty printed string is very clear and is human readable.
    Doesn't raise any exceptions.
    """
    if isinstance(obj, ListFuture):
        return _pretty_print_list_future(obj, indent, char_limit)
    elif isinstance(obj, FunctionCallFuture):
        return _pretty_print_function_call_future(obj, indent, char_limit)
    elif isinstance(obj, ReduceOperationFuture):
        return _pretty_print_reduce_operation_future(obj, indent, char_limit)
    else:
        try:
            # i.e. repr() returns "'foo'" instead of "foo".
            obj_str: str = repr(obj)
            if len(obj_str) > char_limit:
                obj_str = obj_str[: char_limit - 3] + "..."
            return obj_str
        except Exception:
            return f"<unprintable object of type {type(obj)}>"


def _pretty_print_list_future(future: ListFuture, indent: int, char_limit: int) -> str:
    if future._metadata.kind == _FutureListKind.MAP_OPERATION:
        prefix: str = (
            f"Tensorlake Map Operation Future of '{future._metadata.function_name}' over "
        )
    else:
        prefix: str = f"Tensorlake List Future of "

    if isinstance(future._items, ListFuture):
        return prefix + pretty_print(
            future._items, indent=indent, char_limit=char_limit - len(prefix)
        )

    indent_str: str = " " * indent
    strs: list[str] = [
        prefix,
        "[",
    ]
    if len(future._items) != 0:
        strs.append("\n")
    chars_count: int = sum(len(s) for s in strs)

    item_indent_str: str = " " * (indent + _PRETTY_PRINT_INDENT_STEP)
    for item in future._items:
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
    if isinstance(future._items, ListFuture):
        return prefix + pretty_print(
            future._items, indent=indent, char_limit=char_limit - len(prefix)
        )

    indent_str: str = " " * indent
    strs: list[str] = [
        prefix,
        "[",
    ]
    if len(future._items) != 0:
        strs.append("\n")
    chars_count: int = sum(len(s) for s in strs)

    item_indent_str: str = " " * (indent + _PRETTY_PRINT_INDENT_STEP)
    for input_item in future._items:
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

    if len(future._items) != 0:
        strs.append(indent_str)
        chars_count += len(strs[-1])

    strs.append("]")
    chars_count += len(strs[-1])

    return "".join(strs)
