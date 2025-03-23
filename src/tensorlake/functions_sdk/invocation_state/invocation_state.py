from typing import Any, Dict, List, Optional


class InvocationState:
    """Abstract interface for Graph invocation state key-value API.

    The API allows to set and get key-value pairs from Indexify functions.
    The key-value pairs are scoped per Graph invocation.
    Each new invocation starts with an empty state (empty set of key-value pairs).
    A value can be any CloudPickleSerializer serializable object."""

    def __init__(self):
        self.timers: Dict[str, float] = {}
        self.counters: Dict[str, int] = {}
        self.next_nodes: List[str] = []

    def set(self, key: str, value: Any) -> None:
        """Set a key-value pair.

        Raises Exception if an error occurred."""
        raise NotImplementedError()

    def get(self, key: str, default: Optional[Any] = None) -> Optional[Any]:
        """Get a value by key. If the key does not exist, return the default value.

        Raises Exception if an error occurred."""
        raise NotImplementedError()

    def timer(self, name: str, value: float):
        self.timers[name] = value

    def counter(self, name: str, value: int = 1):
        self.counters[name] = self.counters.get(name, 0) + value

    def next_node(self, node: str):
        self.next_nodes.append(node)

    def next_nodes(self, nodes: List[str]):
        self.next_nodes.extend(nodes)
