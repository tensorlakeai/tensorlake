from typing import Any

# Not importing Function and etc here to avoid circular imports.

_package: Any | None = None
# function name -> Function
_function_registry: dict[str, Any] = {}
# Class name -> original class
_class_registry: dict[str, Any] = {}


def set_package(package: Any) -> None:
    """Registers the supplied Tensorlake Package.

    Overwrites previously set Package (it's a singleton)."""
    global _package
    _package = package


def get_package() -> Any | None:
    """Returns the registered Tensorlake Package."""
    global _package
    return _package


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
