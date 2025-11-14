import inspect
from dataclasses import dataclass
from typing import Any, Callable

from ..interface.function import Function, _function_name


@dataclass
class FunctionDetails:
    # Name of the function in Tensorlake application.
    name: str
    # Name of the class in user source code. Not None for class methods.
    class_name: str | None
    # Name of the class method in user source code.
    class_method_name: str | None
    # Name by which the module of the function was imported.
    module_import_name: str
    # Source file path where the function is defined.
    source_file_path: str
    # Source file line where the function is defined.
    source_file_line: int


def get_function_details(fn: Callable) -> FunctionDetails:
    """Returns detailed information about the function.

    Doesn't raise any exceptions. Works for invalid functions that fail validation.
    """
    return FunctionDetails(
        name=function_name_in_application(fn),
        class_name=function_class_name(fn),
        class_method_name=function_class_method_name(fn),
        module_import_name=object_module_import_name(fn),
        source_file_path=object_source_file_path(fn),
        source_file_line=object_source_file_line(fn),
    )


@dataclass
class ClassDetails:
    # Name of the class in user source code.
    class_name: str
    # Name by which the module of the class was imported.
    module_import_name: str
    # Source file path where the function is defined.
    source_file_path: str
    # Source file line where the function is defined.
    source_file_line: int


def get_class_details(cls: Any) -> ClassDetails:
    """Returns detailed information about the class.

    Doesn't raise any exceptions. Works for invalid classes that fail validation.
    """
    return ClassDetails(
        class_name=class_name(cls),
        module_import_name=object_module_import_name(cls),
        source_file_path=object_source_file_path(cls),
        source_file_line=object_source_file_line(cls),
    )


def function_name_in_application(fn: Callable) -> str:
    """Returns the name of the function.

    Doesn't raise any exceptions.
    """
    return _function_name(fn)


def class_name(cls: Any) -> str:
    """Returns the name of the class in user source code.

    Doesn't raise any exceptions.
    """
    # NB: needs to produce the same result as function_class_name because we compare them.
    if inspect.isfunction(cls) or inspect.ismethod(cls):
        name: str | None = function_class_name(cls)
        return "<unknown>" if name is None else name
    if inspect.isclass(cls):
        return getattr(cls, "__qualname__", "<unknown>")
    else:
        return "<unknown>"


def function_class_name(fn: Callable) -> str | None:
    """Returns the class name if the function is a class method, None otherwise.

    Doesn't raise any exceptions.
    """
    # This is a reliable heuristic, see https://peps.python.org/pep-3155/.
    # NB: this might not be a function is user passed something else.
    if inspect.isfunction(fn) or inspect.ismethod(fn):
        qualname_parts: list[str] = getattr(fn, "__qualname__", "<unknown>").split(".")
        if len(qualname_parts) == 1:
            # Not a method.
            return None
        elif len(qualname_parts) >= 2 and qualname_parts[-2] == "<locals>":
            # The function is not a method as it's defined inside another function.
            return None
        else:
            # Strip the last part after the last dot which is a method name.
            return ".".join(qualname_parts[:-1])
    if inspect.isclass(fn):
        return None
    else:
        return "<unknown>"


def function_class_method_name(fn: Callable) -> str | None:
    """Returns the class method name if the function is a class method, None otherwise.

    Doesn't raise any exceptions.
    """
    # This is a reliable heuristic, see https://peps.python.org/pep-3155/.
    if function_class_name(fn) is None:
        return None
    else:
        return getattr(fn, "__qualname__", "<unknown>").split(".")[-1]


def is_module_level_function_or_class(object: Callable | Any) -> bool:
    """Returns True if the supplied function or class is defined at module level.

    Module level class or function or method is importable directly from the module namespace.
    It's not defined in a local scope of another function.
    Doesn't raise any exceptions.
    """
    # This is a reliable heuristic, see https://peps.python.org/pep-3155/.
    return "<locals>" not in getattr(object, "__qualname__", "<unknown>")


def object_module_import_name(object: Any) -> str:
    """Returns the name by which the module where the object is defined was imported.

    Doesn't raise any exceptions. Works for invalid functions that fail validation.
    """
    module: Any | None = inspect.getmodule(object)
    if module is None:
        return "<unknown>"
    return module.__name__


def object_source_file_path(object: Any) -> str:
    """Returns the source file path where the object is defined.

    Doesn't raise any exceptions.
    """
    try:
        file_path: str | None = inspect.getsourcefile(object)
        return "<unknown>" if file_path is None else file_path
    except TypeError:
        return "<builtin>"


def object_source_file_line(object: Any) -> int:
    """Returns the source file line where the object is defined.

    Doesn't raise any exceptions. Works for invalid functions that fail validation.
    """

    try:
        return inspect.getsourcelines(object)[1]
    except (OSError, TypeError):
        return -1


def is_application_function(fn: Function) -> bool:
    """Returns True if the function defines an application.

    Doesn't raise any exceptions. Works for invalid functions that fail validation.
    """
    return fn._application_config is not None
