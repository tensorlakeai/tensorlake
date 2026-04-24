import json
import unittest

from tensorlake._tracing import TracedIterator
from tensorlake.sandbox import Sandbox, SandboxConnectionError
from tensorlake.sandbox.models import StdinMode

_TRACE_ID = "00-deadbeefdeadbeefdeadbeefdeadbeef-cafebabecafebabe-01"


class _FakeRustProxyClient:
    def __init__(self):
        self.start_payload_json = None
        self.run_payload_json = None

    def close(self):
        return None

    def base_url(self):
        return "http://localhost:9443"

    def start_process_json(self, payload_json):
        self.start_payload_json = payload_json
        return _TRACE_ID, json.dumps(
            {
                "pid": 101,
                "status": "running",
                "stdin_writable": True,
                "command": "echo",
                "args": ["hello"],
                "started_at": 1_700_000_000,
            }
        )

    def list_processes_json(self):
        return _TRACE_ID, json.dumps(
            {
                "processes": [
                    {
                        "pid": 101,
                        "status": "running",
                        "stdin_writable": True,
                        "command": "echo",
                        "args": ["hello"],
                        "started_at": 1_700_000_000,
                    }
                ]
            }
        )

    def follow_output_json(self, pid):
        assert pid == 101
        return _TRACE_ID, [
            json.dumps(
                {
                    "line": "hello",
                    "timestamp": 1_700_000_000,
                    "stream": "stdout",
                }
            )
        ]

    def run_process_json(self, payload_json):
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

    def health_json(self):
        return _TRACE_ID, json.dumps({"healthy": True})


def _make_sandbox(fake=None):
    """Return a Sandbox wired to *fake* (or a fresh _FakeRustProxyClient)."""
    client = fake or _FakeRustProxyClient()
    return Sandbox(
        sandbox_id="sbx-1",
        proxy_url="http://localhost:9443",
        api_key="k",
        _proxy_rust_client=client,
    ), client


class TestSandboxRustBackend(unittest.TestCase):
    def test_sandbox_accepts_sandbox_name(self):
        sandbox, _ = _make_sandbox()
        # Rename so the identifier is set from `identifier=` kwarg.
        sandbox2 = Sandbox(
            identifier="stable-name",
            proxy_url="http://localhost:9443",
            api_key="k",
            _proxy_rust_client=_FakeRustProxyClient(),
        )
        self.assertEqual(sandbox2._identifier, "stable-name")

    def test_start_process_uses_rust_backend(self):
        sandbox, fake = _make_sandbox()

        process = sandbox.start_process(
            command="echo",
            args=["hello"],
            stdin_mode=StdinMode.PIPE,
        )

        self.assertEqual(process.pid, 101)
        payload = json.loads(fake.start_payload_json)
        self.assertEqual(payload["command"], "echo")
        self.assertEqual(payload["args"], ["hello"])
        self.assertEqual(payload["stdin_mode"], "pipe")

    def test_list_processes_uses_rust_backend(self):
        sandbox, _ = _make_sandbox()

        processes = sandbox.list_processes()
        items = list(processes)

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].pid, 101)
        self.assertEqual(items[0].status, "running")

    def test_list_processes_returns_traced_iterator(self):
        sandbox, _ = _make_sandbox()

        processes = sandbox.list_processes()

        self.assertIsInstance(processes, TracedIterator)
        self.assertEqual(processes.trace_id, _TRACE_ID)

    def test_list_processes_is_iterable(self):
        sandbox, _ = _make_sandbox()

        processes = sandbox.list_processes()

        pids = [p.pid for p in processes]
        self.assertEqual(pids, [101])

    def test_follow_output_uses_rust_backend(self):
        sandbox, _ = _make_sandbox()

        events = list(sandbox.follow_output(101))

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].line, "hello")
        self.assertEqual(events[0].stream, "stdout")

    def test_follow_output_returns_traced_iterator(self):
        sandbox, _ = _make_sandbox()

        events = sandbox.follow_output(101)

        self.assertIsInstance(events, TracedIterator)
        self.assertEqual(events.trace_id, _TRACE_ID)

    def test_run_uses_streaming_endpoint(self):
        sandbox, fake = _make_sandbox()

        result = sandbox.run("echo", args=["hello"])

        payload = json.loads(fake.run_payload_json)
        self.assertEqual(payload["command"], "echo")
        self.assertEqual(payload["args"], ["hello"])

        self.assertEqual(result.stdout, "out1\nout2")
        self.assertEqual(result.stderr, "err1")
        self.assertEqual(result.exit_code, 0)

    def test_run_signal_maps_to_negative_exit_code(self):
        class _SignaledFakeClient(_FakeRustProxyClient):
            def run_process_json(self, payload_json):
                return _TRACE_ID, [json.dumps({"signal": 9})]

        sandbox, _ = _make_sandbox(_SignaledFakeClient())

        result = sandbox.run("sleep", args=["100"])

        self.assertEqual(result.exit_code, -9)

    def test_run_raises_when_stream_has_no_exit_event(self):
        class _MissingExitFakeClient(_FakeRustProxyClient):
            def run_process_json(self, payload_json):
                return _TRACE_ID, []

        sandbox, _ = _make_sandbox(_MissingExitFakeClient())

        with self.assertRaisesRegex(
            SandboxConnectionError, "stream ended without an exit event"
        ):
            sandbox.run("echo", args=["hello"])

    def test_health_maps_connection_error(self):
        class FakeRustError(Exception):
            pass

        class _FailingRustProxyClient(_FakeRustProxyClient):
            def health_json(self):
                raise FakeRustError(("connection", 503, "dial tcp timeout"))

        import tensorlake.sandbox.sandbox as sandbox_module

        previous = sandbox_module.RustCloudSandboxClientError
        try:
            sandbox_module.RustCloudSandboxClientError = FakeRustError
            sandbox, _ = _make_sandbox(_FailingRustProxyClient())

            with self.assertRaises(SandboxConnectionError):
                sandbox.health()
        finally:
            sandbox_module.RustCloudSandboxClientError = previous


if __name__ == "__main__":
    unittest.main()
