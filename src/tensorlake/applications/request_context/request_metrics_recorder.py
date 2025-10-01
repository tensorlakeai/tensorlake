from typing import Dict

from ..interface.request_context import RequestMetrics


class RequestMetricsRecorder(RequestMetrics):
    """Concrete implementation of RequestMetrics that records its metrics."""

    def __init__(self):
        self._timers: Dict[str, float] = {}
        self._counters: Dict[str, int] = {}

    @property
    def timers(self) -> Dict[str, float]:
        return self._timers

    def timer(self, name: str, value: float):
        self._timers[name] = value

    @property
    def counters(self) -> Dict[str, int]:
        return self._counters

    def counter(self, name: str, value: int = 1):
        self._counters[name] = self._counters.get(name, 0) + value
