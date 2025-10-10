from typing import Set

from .interface.function import Function
from .registry import get_functions


def list_secret_names() -> Set[str]:
    secret_names: Set[str] = set()
    for func in get_functions():
        func: Function
        secret_names.update(func._function_config.secrets)

    return secret_names
