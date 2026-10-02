"""Controlled language-bridge comparison, NOT native WAL or invocation throughput."""

from __future__ import annotations

import asyncio
import json
import platform
import resource
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from tensorlake.function_agent.runner import ProtocolWriter


class SerialProtocolWriter:
    """ProtocolWriter from SDK 317e9d5a, retained only as a benchmark control."""

    def __init__(self, core: DelayedCore, loop: asyncio.AbstractEventLoop):
        self._core = core
        self._loop = loop
        self._lock = threading.Lock()

    def write(self, message: dict[str, Any]) -> None:
        encoded = json.dumps(message, separators=(",", ":"), ensure_ascii=False)

        async def submit_output() -> None:
            await self._core.submit_output(encoded)

        with self._lock:
            asyncio.run_coroutine_threadsafe(submit_output(), self._loop).result()


class DelayedCore:
    def __init__(self, delay_seconds: float) -> None:
        self.delay_seconds = delay_seconds
        self.pending: set[asyncio.Task[Any]] = set()
        self.active = 0
        self.maximum_active = 0
        self.completed = 0

    async def submit_output(self, encoded: str) -> None:
        assert json.loads(encoded)["type"] == "failure"
        task = asyncio.current_task()
        assert task is not None
        self.pending.add(task)
        self.active += 1
        self.maximum_active = max(self.maximum_active, self.active)
        try:
            await asyncio.sleep(self.delay_seconds)
            self.completed += 1
        finally:
            self.active -= 1
            self.pending.discard(task)

    async def stop(self) -> None:
        pending = list(self.pending)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)


async def measure(
    writer_type: type,
    repetition: int,
    *,
    native_delay_seconds: float = 0.01,
    ack_timeout_seconds: float = 5,
) -> dict[str, Any]:
    core = DelayedCore(native_delay_seconds)
    loop = asyncio.get_running_loop()
    writer = writer_type(core, loop)
    before = resource.getrusage(resource.RUSAGE_SELF)
    elapsed = 0.0
    finished_workers: list[threading.Event] = []
    # The fixed worker pool is shared across eight waves. Each wave waits for
    # all 16 threads to be ready before timing gate release through final ACK.
    workers = ThreadPoolExecutor(max_workers=16)
    try:
        for wave in range(8):
            gate = threading.Event()
            ready = threading.Semaphore(0)

            def write(index: int, finished: threading.Event) -> None:
                try:
                    ready.release()
                    if not gate.wait(timeout=5):
                        raise TimeoutError(
                            "benchmark start gate did not open within 5s"
                        )
                    writer.write(
                        {
                            "type": "failure",
                            "attempt_id": f"attempt-{wave}-{index}",
                            "reason": "function_error",
                            "message": "controlled delayed-core output",
                        }
                    )
                finally:
                    finished.set()

            wave_finished = [threading.Event() for _ in range(16)]
            finished_workers.extend(wave_finished)
            outputs = [
                loop.run_in_executor(workers, write, index, finished)
                for index, finished in enumerate(wave_finished)
            ]

            def wait_ready() -> None:
                for _ in outputs:
                    if not ready.acquire(timeout=5):
                        raise TimeoutError(
                            "benchmark workers did not become ready within 5s"
                        )

            try:
                await asyncio.to_thread(wait_ready)
                start = time.perf_counter()
                gate.set()
                await asyncio.wait_for(
                    asyncio.gather(*outputs), timeout=ack_timeout_seconds
                )
                elapsed += time.perf_counter() - start
            finally:
                gate.set()
    finally:
        # Never join native-waiting threads on their owning event-loop thread.
        # Repeated cancellation catches a serial waiter admitted after its
        # predecessor is canceled; cleanup keeps the loop live and is bounded.
        workers.shutdown(wait=False, cancel_futures=True)

        async def drain() -> None:
            while not all(finished.is_set() for finished in finished_workers):
                await core.stop()
                await asyncio.sleep(0.001)

        if not all(finished.is_set() for finished in finished_workers):
            print("Draining fake-native writers after failure (2s bound)", flush=True)
        try:
            await asyncio.wait_for(drain(), timeout=2)
        except asyncio.TimeoutError as error:
            raise RuntimeError("fake-native writer cleanup exceeded 2s") from error
    after = resource.getrusage(resource.RUSAGE_SELF)
    assert core.completed == 128
    assert core.active == 0
    if writer_type is SerialProtocolWriter:
        assert core.maximum_active == 1
    else:
        assert core.maximum_active == 16
        assert len(writer._attempt_locks) == 0
    return {
        "mode": writer_type.__name__,
        "repetition": repetition,
        "outputs": core.completed,
        "waves": 8,
        "concurrency": 16,
        "fake_native_delay_ms": native_delay_seconds * 1_000,
        "maximum_native_in_flight": core.maximum_active,
        "release_to_all_ack_seconds": elapsed,
        "bridge_outputs_per_second": core.completed / elapsed,
        "process_user_seconds": after.ru_utime - before.ru_utime,
        "process_system_seconds": after.ru_stime - before.ru_stime,
        "process_peak_rss_kib": after.ru_maxrss,
    }


async def main() -> None:
    if platform.system() != "Linux":
        raise RuntimeError("This benchmark must run on Linux")
    for repetition in range(1, 4):
        for writer_type in (SerialProtocolWriter, ProtocolWriter):
            print(
                f"Starting bounded bridge comparison {repetition}: {writer_type.__name__}",
                flush=True,
            )
            result = await asyncio.wait_for(
                measure(writer_type, repetition), timeout=30
            )
            print(json.dumps(result, sort_keys=True), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
