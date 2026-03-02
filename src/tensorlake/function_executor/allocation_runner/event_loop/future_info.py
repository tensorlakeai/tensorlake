import threading
from dataclasses import dataclass, field
from typing import Any

from tensorlake.applications.interface.futures import (
    FunctionCallFuture,
    Future,
)


@dataclass
class FutureInfo:
    # Original Future created by user code or internal Future created by SDK i.e.
    # a future per map function call, or a future per reduce operation step.
    future: Future
    # The future's durable ID.
    # ReduceOp and ListFuture are not visible to Server but we still
    # compute durable IDs because this allows to detect changes not visible
    # to Server and also avoids us using a recursive durable ID compute algorithm.
    durable_id: str
    # Set if this future is ListFuture.
    map_future_output: list[FunctionCallFuture] | None
    # Set if this is reduce operation future. None can be a valid output.
    reduce_future_output: Future | Any | None

    # Set when the function call creation is confirmed by the server.
    function_call_created: threading.Event = field(default_factory=threading.Event)
    # Set when the function call result is delivered by the server.
    function_call_finished: threading.Event = field(default_factory=threading.Event)
