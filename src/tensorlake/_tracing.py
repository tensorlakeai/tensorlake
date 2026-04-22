"""W3C traceparent header generation and traced result wrappers.

Injects a fresh trace context into every request so server-side spans
can be correlated with the initiating SDK call in distributed traces.
No OTLP export is performed — the header is the only telemetry artifact.
"""

from __future__ import annotations

import os
from typing import Generic, Iterator, TypeVar

T = TypeVar("T")


def generate_traceparent() -> str:
    """Return a W3C traceparent header value with a fresh trace ID and span ID.

    Format: ``00-<32 hex>-<16 hex>-01`` (sampled).
    """
    trace_id = os.urandom(16).hex()
    span_id = os.urandom(8).hex()
    return f"00-{trace_id}-{span_id}-01"


def inject_traceparent(headers: dict) -> dict:
    """Return *headers* with a ``traceparent`` key added (or replaced).

    Creates a shallow copy so the caller's dict is not mutated.
    """
    return {**headers, "traceparent": generate_traceparent()}


class Traced(Generic[T]):
    """An operation result paired with its W3C trace ID.

    Attribute access is delegated to the wrapped result so existing code that
    accesses fields directly (e.g. ``process.pid``) continues to work.  The
    raw result is always available as ``.value`` and the trace ID as
    ``.trace_id``.
    """

    def __init__(self, trace_id: str, result: T) -> None:
        self.trace_id = trace_id
        self._result = result

    @property
    def value(self) -> T:
        """The unwrapped operation result."""
        return self._result

    def __getattr__(self, name: str):
        try:
            inner = object.__getattribute__(self, "_result")
        except AttributeError:
            raise AttributeError(name) from None
        return getattr(inner, name)

    def __repr__(self) -> str:
        return f"Traced(trace_id={self.trace_id!r}, result={self._result!r})"


class TracedIterator(Generic[T]):
    """An eagerly-collected sequence of results paired with a W3C trace ID.

    The underlying transport collects all items before returning (the Rust/SSE
    boundary buffers the full response), so iteration is over a pre-fetched
    list.  Iterating yields the individual items directly, so existing
    ``for item in sandbox.follow_stdout(pid):`` loops continue to work.
    """

    def __init__(self, trace_id: str, items: list[T]) -> None:
        self.trace_id = trace_id
        self._items = items

    def __iter__(self) -> Iterator[T]:
        return iter(self._items)

    def __len__(self) -> int:
        return len(self._items)

    def __repr__(self) -> str:
        return f"TracedIterator(trace_id={self.trace_id!r}, items={self._items!r})"
