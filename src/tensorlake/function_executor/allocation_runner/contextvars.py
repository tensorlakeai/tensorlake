import contextvars

_current_allocation_id_context_var = contextvars.ContextVar("CURRENT_ALLOCATION_ID")


def get_allocation_id_context_variable() -> str:
    """Gets the allocation ID for the current function thread.

    Raises:
        LookupError: If no allocation ID is set in the current context.
    """
    return _current_allocation_id_context_var.get()


def set_allocation_id_context_variable(allocation_id: str) -> None:
    """Sets the allocation ID for the current function thread."""
    _current_allocation_id_context_var.set(allocation_id)
