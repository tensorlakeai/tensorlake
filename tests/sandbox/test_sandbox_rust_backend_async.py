import json
import unittest
from unittest.mock import AsyncMock

from tensorlake._tracing import Traced, TracedIterator
from tensorlake.sandbox import AsyncSandbox, SandboxConnectionError
from tensorlake.sandbox.exceptions import SandboxError
from tensorlake.sandbox.models import (
    ContainerResourcesInfo,
    OutputMode,
    ProcessStatus,
    ProcessUser,
    SandboxInfo,
    SandboxStatus,
    StdinMode,
)

_TRACE_ID = "00-deadbeefdeadbeefdeadbeefdeadbeef-cafebabecafebabe-01"


def _process_info_dict(pid: int = 101) -> dict:
    return {
        "pid": pid,
        "status": ProcessStatus.RUNNING.value,
        "stdin_writable": True,
        "command": "echo",
        "args": ["hello"],
        "started_at": 1_700_000_000,
    }


class _FakeAsyncRustProxyClient:
    """Async counterpart of the sync test's _FakeRustProxyClient.

    Every Rust binding method on the async path ends in ``_async`` and is
    awaited by AsyncSandbox, so the fakes are plain ``async def`` methods.
    """

    def __init__(self):
        self.start_payload_json: str | None = None
        self.run_payload_json: str | None = None
        self.write_stdin_calls: list[tuple[int, bytes]] = []
        self.close_stdin_calls: list[int] = []
        self.kill_calls: list[int] = []
        self.signal_calls: list[tuple[int, int]] = []
        self.read_file_calls: list[str] = []
        self.write_file_calls: list[tuple[str, bytes]] = []
        self.delete_file_calls: list[str] = []
        self.list_directory_calls: list[str] = []

    def close(self):
        return None

    def base_url(self):
        return "http://localhost:9443"

    async def start_process_json_async(self, payload_json):
        self.start_payload_json = payload_json
        return _TRACE_ID, json.dumps(_process_info_dict())

    async def list_processes_json_async(self):
        return _TRACE_ID, json.dumps({"processes": [_process_info_dict()]})

    async def get_process_json_async(self, *, pid):
        return _TRACE_ID, json.dumps(_process_info_dict(pid))

    async def kill_process_async(self, *, pid):
        self.kill_calls.append(pid)
        return _TRACE_ID

    async def send_signal_json_async(self, *, pid, signal):
        self.signal_calls.append((pid, signal))
        return _TRACE_ID, json.dumps({"success": True})

    async def write_stdin_async(self, *, pid, data):
        self.write_stdin_calls.append((pid, data))
        return _TRACE_ID

    async def close_stdin_async(self, *, pid):
        self.close_stdin_calls.append(pid)
        return _TRACE_ID

    async def get_stdout_json_async(self, *, pid):
        return _TRACE_ID, json.dumps({"pid": pid, "lines": ["a", "b"], "line_count": 2})

    async def get_stderr_json_async(self, *, pid):
        return _TRACE_ID, json.dumps({"pid": pid, "lines": ["err"], "line_count": 1})

    async def get_output_json_async(self, *, pid):
        return _TRACE_ID, json.dumps(
            {"pid": pid, "lines": ["a", "b", "err"], "line_count": 3}
        )

    async def follow_stdout_json_async(self, *, pid):
        return _TRACE_ID, [
            json.dumps({"line": "out", "timestamp": 1_700_000_000, "stream": "stdout"})
        ]

    async def follow_stderr_json_async(self, *, pid):
        return _TRACE_ID, [
            json.dumps({"line": "err", "timestamp": 1_700_000_001, "stream": "stderr"})
        ]

    async def follow_output_json_async(self, *, pid):
        return _TRACE_ID, [
            json.dumps(
                {"line": "hello", "timestamp": 1_700_000_000, "stream": "stdout"}
            )
        ]

    async def run_process_json_async(self, payload_json):
        self.run_payload_json = payload_json
        return _TRACE_ID, [
            json.dumps(
                {"line": "out1", "stream": "stdout", "timestamp": 1_700_000_001}
            ),
            json.dumps(
                {"line": "err1", "stream": "stderr", "timestamp": 1_700_000_002}
            ),
            json.dumps(
                {"line": "out2", "stream": "stdout", "timestamp": 1_700_000_003}
            ),
            json.dumps({"exit_code": 0}),
        ]

    async def read_file_bytes_async(self, *, path):
        self.read_file_calls.append(path)
        return _TRACE_ID, b"file-bytes"

    async def write_file_async(self, *, path, content):
        self.write_file_calls.append((path, content))
        return _TRACE_ID

    async def delete_file_async(self, *, path):
        self.delete_file_calls.append(path)
        return _TRACE_ID

    async def list_directory_json_async(self, *, path):
        self.list_directory_calls.append(path)
        return _TRACE_ID, json.dumps(
            {
                "path": path,
                "entries": [
                    {"name": "a.txt", "is_dir": False, "size": 3},
                    {"name": "sub", "is_dir": True},
                ],
            }
        )

    async def health_json_async(self):
        return _TRACE_ID, json.dumps({"healthy": True})

    async def info_json_async(self):
        return _TRACE_ID, json.dumps(
            {
                "version": "1.2.3",
                "uptime_secs": 42,
                "running_processes": 1,
                "total_processes": 5,
            }
        )


def _make_async_sandbox(fake=None):
    """Return an AsyncSandbox wired to *fake* (or a fresh fake)."""
    client = fake or _FakeAsyncRustProxyClient()
    return (
        AsyncSandbox(
            sandbox_id="sbx-1",
            proxy_url="http://localhost:9443",
            api_key="k",
            _proxy_rust_client=client,
        ),
        client,
    )


def _sandbox_info(status=SandboxStatus.RUNNING, **overrides) -> SandboxInfo:
    fields = {
        "id": "sbx-1",
        "namespace": "default",
        "status": status,
        "resources": ContainerResourcesInfo(
            cpus=1.0, memory_mb=512, ephemeral_disk_mb=1024
        ),
    }
    fields.update(overrides)
    return SandboxInfo(**fields)


class TestAsyncSandboxRustBackend(unittest.IsolatedAsyncioTestCase):
    def test_async_sandbox_accepts_sandbox_name(self):
        sandbox = AsyncSandbox(
            identifier="stable-name",
            proxy_url="http://localhost:9443",
            api_key="k",
            _proxy_rust_client=_FakeAsyncRustProxyClient(),
        )
        self.assertEqual(sandbox._identifier, "stable-name")

    def test_async_sandbox_rejects_conflicting_identifier_aliases(self):
        with self.assertRaisesRegex(SandboxError, "Provide only one of"):
            AsyncSandbox(
                identifier="stable-name",
                sandbox_id="sbx-other",
                proxy_url="http://localhost:9443",
                api_key="k",
                _proxy_rust_client=_FakeAsyncRustProxyClient(),
            )

    def test_async_sandbox_requires_identifier(self):
        with self.assertRaisesRegex(SandboxError, "`identifier` is required"):
            AsyncSandbox(
                proxy_url="http://localhost:9443",
                api_key="k",
                _proxy_rust_client=_FakeAsyncRustProxyClient(),
            )

    async def test_start_process_uses_rust_backend(self):
        # Regression: AsyncSandbox.start_process must build the payload via
        # Sandbox._build_command_payload, not self._build_command_payload —
        # the latter raised AttributeError because AsyncSandbox does not
        # inherit from Sandbox.
        sandbox, fake = _make_async_sandbox()

        process = await sandbox.start_process(
            command="echo",
            args=["hello"],
            stdin_mode=StdinMode.PIPE,
            user=ProcessUser.ROOT,
        )

        self.assertEqual(process.pid, 101)
        self.assertEqual(process.trace_id, _TRACE_ID)
        payload = json.loads(fake.start_payload_json)
        self.assertEqual(payload["command"], "echo")
        self.assertEqual(payload["args"], ["hello"])
        self.assertEqual(payload["stdin_mode"], StdinMode.PIPE.value)
        self.assertEqual(payload["user"], ProcessUser.ROOT.value)

    async def test_start_process_omits_default_modes_from_payload(self):
        sandbox, fake = _make_async_sandbox()

        await sandbox.start_process(command="echo")

        payload = json.loads(fake.start_payload_json)
        self.assertNotIn("stdin_mode", payload)
        self.assertNotIn("stdout_mode", payload)
        self.assertNotIn("stderr_mode", payload)
        self.assertNotIn("user", payload)

    async def test_start_process_serializes_non_default_output_modes(self):
        sandbox, fake = _make_async_sandbox()

        await sandbox.start_process(
            command="echo",
            stdout_mode=OutputMode.DISCARD,
            stderr_mode=OutputMode.DISCARD,
        )

        payload = json.loads(fake.start_payload_json)
        self.assertEqual(payload["stdout_mode"], OutputMode.DISCARD.value)
        self.assertEqual(payload["stderr_mode"], OutputMode.DISCARD.value)

    async def test_list_processes_returns_traced_iterator(self):
        sandbox, _ = _make_async_sandbox()

        processes = await sandbox.list_processes()

        self.assertIsInstance(processes, TracedIterator)
        self.assertEqual(processes.trace_id, _TRACE_ID)
        items = list(processes)
        self.assertEqual([p.pid for p in items], [101])

    async def test_get_process_uses_rust_backend(self):
        sandbox, _ = _make_async_sandbox()

        traced = await sandbox.get_process(101)

        self.assertEqual(traced.pid, 101)
        self.assertEqual(traced.trace_id, _TRACE_ID)

    async def test_run_uses_streaming_endpoint(self):
        sandbox, fake = _make_async_sandbox()

        result = await sandbox.run("echo", args=["hello"], user="root")

        payload = json.loads(fake.run_payload_json)
        self.assertEqual(payload["command"], "echo")
        self.assertEqual(payload["args"], ["hello"])
        self.assertEqual(payload["user"], "root")

        self.assertEqual(result.stdout, "out1\nout2")
        self.assertEqual(result.stderr, "err1")
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.trace_id, _TRACE_ID)

    async def test_run_threads_timeout_through_payload(self):
        sandbox, fake = _make_async_sandbox()

        await sandbox.run("sleep", args=["1"], timeout=2.5)

        payload = json.loads(fake.run_payload_json)
        self.assertEqual(payload["timeout"], 2.5)

    async def test_run_signal_maps_to_negative_exit_code(self):
        class _SignaledFake(_FakeAsyncRustProxyClient):
            async def run_process_json_async(self, payload_json):
                return _TRACE_ID, [json.dumps({"signal": 9})]

        sandbox, _ = _make_async_sandbox(_SignaledFake())

        result = await sandbox.run("sleep", args=["100"])

        self.assertEqual(result.exit_code, -9)

    async def test_run_raises_when_stream_has_no_exit_event(self):
        class _MissingExit(_FakeAsyncRustProxyClient):
            async def run_process_json_async(self, payload_json):
                return _TRACE_ID, []

        sandbox, _ = _make_async_sandbox(_MissingExit())

        with self.assertRaisesRegex(
            SandboxConnectionError, "stream ended without an exit event"
        ):
            await sandbox.run("echo", args=["hello"])

    async def test_run_ignores_unknown_event_kinds(self):
        # Belt-and-braces: the parser should skip anything that is neither a
        # line nor an exit event without falling over.
        class _NoisyFake(_FakeAsyncRustProxyClient):
            async def run_process_json_async(self, payload_json):
                return _TRACE_ID, [
                    json.dumps({"hello": "world"}),
                    json.dumps({"line": "ok", "stream": "stdout", "timestamp": 1}),
                    json.dumps({"exit_code": 0}),
                ]

        sandbox, _ = _make_async_sandbox(_NoisyFake())

        result = await sandbox.run("echo")

        self.assertEqual(result.stdout, "ok")
        self.assertEqual(result.exit_code, 0)

    async def test_write_stdin_forwards_bytes(self):
        sandbox, fake = _make_async_sandbox()

        traced = await sandbox.write_stdin(101, b"hello")

        self.assertEqual(fake.write_stdin_calls, [(101, b"hello")])
        self.assertEqual(traced.trace_id, _TRACE_ID)

    async def test_close_stdin_forwards_pid(self):
        sandbox, fake = _make_async_sandbox()

        await sandbox.close_stdin(101)

        self.assertEqual(fake.close_stdin_calls, [101])

    async def test_kill_process_forwards_pid(self):
        sandbox, fake = _make_async_sandbox()

        await sandbox.kill_process(101)

        self.assertEqual(fake.kill_calls, [101])

    async def test_send_signal_forwards_pid_and_signal(self):
        sandbox, fake = _make_async_sandbox()

        traced = await sandbox.send_signal(101, 15)

        self.assertEqual(fake.signal_calls, [(101, 15)])
        self.assertTrue(traced.success)

    async def test_get_stdout_returns_traced_response(self):
        sandbox, _ = _make_async_sandbox()

        traced = await sandbox.get_stdout(101)

        self.assertEqual(traced.lines, ["a", "b"])
        self.assertEqual(traced.line_count, 2)

    async def test_get_stderr_returns_traced_response(self):
        sandbox, _ = _make_async_sandbox()

        traced = await sandbox.get_stderr(101)

        self.assertEqual(traced.lines, ["err"])

    async def test_get_output_returns_traced_response(self):
        sandbox, _ = _make_async_sandbox()

        traced = await sandbox.get_output(101)

        self.assertEqual(traced.line_count, 3)

    async def test_follow_stdout_returns_traced_iterator(self):
        sandbox, _ = _make_async_sandbox()

        events = await sandbox.follow_stdout(101)

        self.assertIsInstance(events, TracedIterator)
        self.assertEqual(events.trace_id, _TRACE_ID)
        items = list(events)
        self.assertEqual([e.line for e in items], ["out"])

    async def test_follow_stderr_returns_traced_iterator(self):
        sandbox, _ = _make_async_sandbox()

        events = await sandbox.follow_stderr(101)
        items = list(events)

        self.assertEqual([e.line for e in items], ["err"])

    async def test_follow_output_returns_traced_iterator(self):
        sandbox, _ = _make_async_sandbox()

        events = await sandbox.follow_output(101)
        items = list(events)

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].line, "hello")
        self.assertEqual(items[0].stream, "stdout")

    async def test_read_file_returns_bytes(self):
        sandbox, fake = _make_async_sandbox()

        traced = await sandbox.read_file("/tmp/x")

        self.assertEqual(traced.value, b"file-bytes")
        self.assertEqual(fake.read_file_calls, ["/tmp/x"])

    async def test_write_file_forwards_path_and_content(self):
        sandbox, fake = _make_async_sandbox()

        await sandbox.write_file("/tmp/x", b"abc")

        self.assertEqual(fake.write_file_calls, [("/tmp/x", b"abc")])

    async def test_delete_file_forwards_path(self):
        sandbox, fake = _make_async_sandbox()

        await sandbox.delete_file("/tmp/x")

        self.assertEqual(fake.delete_file_calls, ["/tmp/x"])

    async def test_list_directory_returns_entries(self):
        sandbox, fake = _make_async_sandbox()

        traced = await sandbox.list_directory("/tmp")

        self.assertEqual(fake.list_directory_calls, ["/tmp"])
        self.assertEqual(traced.path, "/tmp")
        self.assertEqual([e.name for e in traced.entries], ["a.txt", "sub"])

    async def test_health_returns_traced_health(self):
        sandbox, _ = _make_async_sandbox()

        traced = await sandbox.health()

        self.assertTrue(traced.healthy)
        self.assertEqual(traced.trace_id, _TRACE_ID)

    async def test_daemon_info_returns_traced_info(self):
        sandbox, _ = _make_async_sandbox()

        traced = await sandbox.daemon_info()

        self.assertEqual(traced.version, "1.2.3")
        self.assertEqual(traced.running_processes, 1)

    async def test_health_maps_connection_error(self):
        class FakeRustError(Exception):
            pass

        class _FailingFake(_FakeAsyncRustProxyClient):
            async def health_json_async(self):
                raise FakeRustError(("connection", 503, "dial tcp timeout"))

        import tensorlake.sandbox.sandbox as sandbox_module

        previous = sandbox_module.RustCloudSandboxClientError
        try:
            sandbox_module.RustCloudSandboxClientError = FakeRustError
            sandbox, _ = _make_async_sandbox(_FailingFake())

            with self.assertRaises(SandboxConnectionError):
                await sandbox.health()
        finally:
            sandbox_module.RustCloudSandboxClientError = previous

    async def test_status_requires_lifecycle_client(self):
        sandbox, _ = _make_async_sandbox()
        with self.assertRaises(SandboxError):
            await sandbox.status()

    async def test_status_fetches_via_lifecycle_client(self):
        sandbox, _ = _make_async_sandbox()
        sandbox._lifecycle_client = AsyncMock()
        sandbox._lifecycle_client.get = AsyncMock(
            return_value=Traced(
                _TRACE_ID, _sandbox_info(status=SandboxStatus.SUSPENDED)
            )
        )

        status = await sandbox.status()

        self.assertEqual(status, SandboxStatus.SUSPENDED)
        sandbox._lifecycle_client.get.assert_awaited_once_with("sbx-1")

    async def test_update_calls_lifecycle_client(self):
        sandbox, _ = _make_async_sandbox()
        sandbox._cached_info = _sandbox_info()
        sandbox._lifecycle_client = AsyncMock()
        sandbox._lifecycle_client.update_sandbox = AsyncMock(
            return_value=Traced(
                _TRACE_ID,
                _sandbox_info(
                    name="renamed",
                    exposed_ports=[8080],
                    allow_unauthenticated_access=True,
                ),
            )
        )

        traced = await sandbox.update(
            name="renamed",
            allow_unauthenticated_access=True,
            exposed_ports=[8080],
        )

        sandbox._lifecycle_client.update_sandbox.assert_awaited_once_with(
            "sbx-1",
            name="renamed",
            allow_unauthenticated_access=True,
            exposed_ports=[8080],
        )
        self.assertEqual(traced.name, "renamed")

    async def test_update_requires_lifecycle_client(self):
        sandbox, _ = _make_async_sandbox()
        with self.assertRaises(SandboxError):
            await sandbox.update(name="anything")

    async def test_terminate_closes_proxy_and_deletes_via_lifecycle(self):
        sandbox, _ = _make_async_sandbox()
        sandbox._owns_sandbox = True
        lifecycle = AsyncMock()
        lifecycle.delete = AsyncMock(return_value=Traced(_TRACE_ID, None))
        sandbox._lifecycle_client = lifecycle

        await sandbox.terminate()

        lifecycle.delete.assert_awaited_once_with("sbx-1")
        # Lifecycle reference is cleared after terminate so subsequent
        # operations cannot accidentally re-delete.
        self.assertIsNone(sandbox._lifecycle_client)
        self.assertFalse(sandbox._owns_sandbox)

    async def test_aexit_terminates_when_owned(self):
        sandbox, _ = _make_async_sandbox()
        sandbox._owns_sandbox = True
        lifecycle = AsyncMock()
        lifecycle.delete = AsyncMock(return_value=Traced(_TRACE_ID, None))
        sandbox._lifecycle_client = lifecycle

        async with sandbox:
            pass

        lifecycle.delete.assert_awaited_once_with("sbx-1")

    async def test_aexit_only_closes_when_not_owned(self):
        sandbox, _ = _make_async_sandbox()
        sandbox._lifecycle_client = AsyncMock()
        sandbox._lifecycle_client.delete = AsyncMock()

        async with sandbox:
            pass

        sandbox._lifecycle_client.delete.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
