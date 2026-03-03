import json
import unittest

from tensorlake.sandbox import Sandbox, SandboxConnectionError
from tensorlake.sandbox.models import StdinMode


class _FakeRustProxyClient:
    def __init__(self):
        self.start_payload_json = None

    def close(self):
        return None

    def base_url(self):
        return "http://localhost:9443"

    def start_process_json(self, payload_json):
        self.start_payload_json = payload_json
        return json.dumps(
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
        return json.dumps(
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
        return [
            json.dumps(
                {
                    "line": "hello",
                    "timestamp": 1_700_000_000,
                    "stream": "stdout",
                }
            )
        ]

    def health_json(self):
        return json.dumps({"healthy": True})


class TestSandboxRustBackend(unittest.TestCase):
    def test_start_process_uses_rust_backend(self):
        sandbox = Sandbox(
            sandbox_id="sbx-1", proxy_url="http://localhost:9443", api_key="k"
        )
        fake = _FakeRustProxyClient()
        sandbox._rust_client = fake
        sandbox._base_url = fake.base_url()

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
        sandbox = Sandbox(
            sandbox_id="sbx-1", proxy_url="http://localhost:9443", api_key="k"
        )
        sandbox._rust_client = _FakeRustProxyClient()

        processes = sandbox.list_processes()

        self.assertEqual(len(processes), 1)
        self.assertEqual(processes[0].pid, 101)
        self.assertEqual(processes[0].status, "running")

    def test_follow_output_uses_rust_backend(self):
        sandbox = Sandbox(
            sandbox_id="sbx-1", proxy_url="http://localhost:9443", api_key="k"
        )
        sandbox._rust_client = _FakeRustProxyClient()

        events = list(sandbox.follow_output(101))

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].line, "hello")
        self.assertEqual(events[0].stream, "stdout")

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
            sandbox = Sandbox(
                sandbox_id="sbx-1",
                proxy_url="http://localhost:9443",
                api_key="k",
            )
            sandbox._rust_client = _FailingRustProxyClient()

            with self.assertRaises(SandboxConnectionError):
                sandbox.health()
        finally:
            sandbox_module.RustCloudSandboxClientError = previous


if __name__ == "__main__":
    unittest.main()
