from typing import Any

# Not importing Function and etc here to avoid circular imports.

# A single function or class name can map to multiple Function objects or class objects.
# Validation code checks if the multiple objects are for exactly the same function/class.
# If not, then validation fails.

# function name -> [Function]
_function_registry: dict[str, list[Any]] = {}
# Class name -> [original class]
_class_registry: dict[str, list[Any]] = {}


def register_function(fn_name: str, fn: Any) -> None:
    """Register a Tensorlake Function."""
    global _function_registry

    if fn_name not in _function_registry:
        _function_registry[fn_name] = []
    _function_registry[fn_name].append(fn)


def get_function(name: str) -> Any:
    """Return the function object associated with the given name.

    Raises Exception if it's not registered.
    """
    global _function_registry

    # Returns the latest function definition in user code.
    # This is consistent with how Python works.
    return _function_registry[name][-1]


def has_function(name: str) -> bool:
    global _function_registry
    return name in _function_registry


def get_functions() -> list[Any]:
    """Return all registered functions with unique names.

    All duplicates are resolved according to Python's semantic.
    """
    global _function_registry

    result: list[Any] = []
    for fn_name in _function_registry.keys():
        result.append(get_function(fn_name))
    return result


def get_functions_with_duplicates() -> dict[str, list[Any]]:
    """Return all registered functions including duplicates.

    This method should only be used during application validation.
    """
    global _function_registry

    return _function_registry


def register_class(cls_name: str, cls: Any) -> None:
    global _class_registry

    if cls_name not in _class_registry:
        _class_registry[cls_name] = []
    _class_registry[cls_name].append(cls)


def has_class(cls_name: str) -> bool:
    global _class_registry
    return cls_name in _class_registry


def get_class(cls_name: str) -> Any:
    """Return the class object associated with the given name.

    Raises Exception if it's not registered.
    """
    global _class_registry

    # Returns the latest class definition in user code.
    # This is consistent with how Python works.
    return _class_registry[cls_name][-1]


def get_classes() -> list[Any]:
    """Return all registered classes with unique names.

    All duplicates are resolved according to Python's semantic.
    """
    global _class_registry

    result: list[Any] = []
    for cls_name in _class_registry.keys():
        result.append(get_class(cls_name))
    return result


def get_classes_with_duplicates() -> dict[str, list[Any]]:
    """Return all registered classes including duplicates.

    This method should only be used during application validation.
    """
    global _class_registry

    return _class_registry


_decorators: list[Any] = []


def register_decorator(
    decorator: Any,
) -> None:
    global _decorators

    _decorators.append(decorator)


def get_decorators() -> list[Any]:
    """Returns all registered decorators."""
    global _decorators
    return _decorators
