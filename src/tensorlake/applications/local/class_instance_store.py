import threading
from typing import Any, Dict

from ..function.function_call import create_self_instance
from ..interface.function import Function


class ClassInstanceStore:
    """Holds class instances for functions.

    Ensures that at most one instance of a class is created.
    """

    def __init__(self) -> None:
        # Class name => instance.
        self._class_instances: Dict[str, Any] = {}
        # Class instance constructor can run for minutes,
        # so we need to ensure that we create at most one instance at a time.
        self._class_instances_locks: Dict[str, threading.Lock] = {}

    def get(self, function: Function) -> Any | None:
        """Get the class instance for the given function.

        If the function is not a class method, returns None.
        """
        fn_class_name: str | None = function._function_config.class_name
        if fn_class_name is None:
            return None

        # No need to lock self._class_instances_lock here because
        # we don't do any IO here so we don't release GIL.
        if fn_class_name not in self._class_instances_locks:
            self._class_instances_locks[fn_class_name] = threading.Lock()

        with self._class_instances_locks[fn_class_name]:
            if fn_class_name not in self._class_instances:
                # NB: This call can take minutes if i.e. a model gets loaded in a GPU.
                self._class_instances[fn_class_name] = create_self_instance(
                    fn_class_name
                )

            return self._class_instances[fn_class_name]

    @classmethod
    def singleton(cls) -> "ClassInstanceStore":
        global _instance
        return _instance


_instance: ClassInstanceStore = ClassInstanceStore()
