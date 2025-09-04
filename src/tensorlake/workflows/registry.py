from typing import Any, List

# Not importing Function and etc here to avoid circular imports.

_application: Any | None = None
# function name -> Function
_function_registry: dict[str, Any] = {}
# Class name -> original class
_class_registry: dict[str, Any] = {}


def set_application(application: Any) -> None:
    """Registers the supplied Tensorlake Application.

    Overwrites previously set Application (it's a singleton)."""
    global _application
    if _application is not None:
        # TODO: Figure out how to return error here.
        print("Warning: Overwriting existing Tensorlake Application.")
    _application = application


def get_application() -> Any | None:
    """Returns the registered Tensorlake Application."""
    global _application
    return _application


def register_function(fn_name: str, fn: Any) -> None:
    """Register a Tensorlake Function."""
    global _function_registry

    if fn_name in _function_registry:
        # TODO: Figure out how to return error here.
        print(f"Warning: Tensorlake Function '{fn_name}' already exists.")
    _function_registry[fn_name] = fn


def get_function(name: str) -> Any:
    """Return the function object associated with the given name.

    Raises Exception if it's not registered.
    """
    global _function_registry

    return _function_registry[name]


def has_function(name: str) -> bool:
    global _function_registry
    return name in _function_registry


def get_functions() -> List[Any]:
    """Return all registered functions."""
    global _function_registry

    return list(_function_registry.values())


def register_class(cls_name: str, cls: Any) -> None:
    global _class_registry

    if cls_name in _class_registry:
        # TODO: Figure out how to return error here.
        print(f"Warning: Tensorlake Class '{cls_name}' already exists.")
    _class_registry[cls_name] = cls


def get_class(cls_name: str) -> Any:
    """Return the class object associated with the given name.

    Raises Exception if it's not registered.
    """
    global _class_registry

    return _class_registry[cls_name]
