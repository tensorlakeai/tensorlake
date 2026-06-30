import json
import unittest
from unittest.mock import MagicMock

from tensorlake._tracing import Traced, TracedIterator
from tensorlake.sandbox import Sandbox, SandboxConnectionError
from tensorlake.sandbox.exceptions import SandboxError
from tensorlake.sandbox.models import (
    ContainerResourcesInfo,
    ProcessUserSpec,
    SandboxInfo,
    SandboxStatus,
    StdinMode,
)

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
        payload = json.loads(payload_json)
        response = {
            "handle": 7,
            "pid": 101,
            "status": "running",
            "stdin_writable": True,
            "command": "echo",
            "args": ["hello"],
            "started_at": 1_700_000_000,
        }
        if payload.get("name"):
            response["managed"] = {
                "id": "managed-1",
                "name": payload["name"],
                "status": "running",
                "restart_count": 0,
                "restart": payload.get("restart")
                or {
                    "policy": "on_failure",
                    "initial_backoff_ms": 500,
                    "max_backoff_ms": 30000,
                },
                "health_check": payload.get("health_check"),
                "health_status": "starting",
                "consecutive_health_failures": 0,
            }
        return _TRACE_ID, json.dumps(response)

    def restart_process_json(self, process):
        assert process == "101"
        return _TRACE_ID, json.dumps(
            {
                "handle": 8,
                "pid": 101,
                "status": "running",
                "stdin_writable": True,
                "command": "echo",
                "args": ["hello"],
                "started_at": 1_700_000_000,
                "managed": {
                    "id": "managed-1",
                    "name": "web",
                    "status": "running",
                    "restart_count": 1,
                    "restart": {
                        "policy": "always",
                        "initial_backoff_ms": 500,
                        "max_backoff_ms": 30000,
                    },
                    "health_status": "healthy",
                    "consecutive_health_failures": 0,
                },
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

    def follow_output_json(self, process):
        assert process == "101"
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
    return (
        Sandbox(
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
            user="root",
        )

        self.assertEqual(process.pid, 101)
        payload = json.loads(fake.start_payload_json)
        self.assertEqual(payload["command"], "echo")
        self.assertEqual(payload["args"], ["hello"])
        self.assertEqual(payload["stdin_mode"], "pipe")
        self.assertEqual(payload["user"], "root")

    def test_start_process_serializes_uid_gid_user_spec(self):
        sandbox, fake = _make_sandbox()

        sandbox.start_process(
            command="echo",
            user=ProcessUserSpec(uid=1000, gid=1000),
        )

        payload = json.loads(fake.start_payload_json)
        self.assertEqual(payload["user"], {"uid": 1000, "gid": 1000})

    def test_start_process_omits_user_by_default(self):
        sandbox, fake = _make_sandbox()

        sandbox.start_process(command="echo")

        payload = json.loads(fake.start_payload_json)
        # No user requested -> field omitted so the sandbox resolves the
        # image's configured user (image USER, falling back to root).
        self.assertNotIn("user", payload)

    def test_start_process_serializes_managed_options(self):
        sandbox, fake = _make_sandbox()

        process = sandbox.start_process(
            command="python",
            args=["app.py"],
            name="web",
            restart={
                "policy": "always",
                "max_restarts": 10,
                "initial_backoff_ms": 250,
            },
            health_check={
                "type": "http",
                "port": 8000,
                "path": "/health",
                "interval_ms": 5000,
            },
        )

        payload = json.loads(fake.start_payload_json)
        self.assertEqual(payload["name"], "web")
        self.assertEqual(payload["restart"]["policy"], "always")
        self.assertEqual(payload["restart"]["max_restarts"], 10)
        self.assertEqual(payload["restart"]["initial_backoff_ms"], 250)
        self.assertEqual(payload["health_check"]["type"], "http")
        self.assertEqual(payload["health_check"]["port"], 8000)
        self.assertEqual(payload["health_check"]["path"], "/health")
        self.assertEqual(payload["health_check"]["interval_ms"], 5000)
        self.assertEqual(process.handle, 7)
        self.assertIsNotNone(process.managed)
        self.assertEqual(process.managed.name, "web")

    def test_restart_process_uses_rust_backend(self):
        sandbox, _ = _make_sandbox()

        process = sandbox.restart_process(101)

        self.assertEqual(process.pid, 101)
        self.assertEqual(process.handle, 8)
        self.assertIsNotNone(process.managed)
        self.assertEqual(process.managed.restart_count, 1)

    def test_start_process_rejects_invalid_user_spec_mapping(self):
        sandbox, _ = _make_sandbox()

        with self.assertRaisesRegex(SandboxError, "invalid process user spec"):
            sandbox.start_process(command="echo", user={"uid": "not-an-int"})

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

        result = sandbox.run("echo", args=["hello"], user="root")

        payload = json.loads(fake.run_payload_json)
        self.assertEqual(payload["command"], "echo")
        self.assertEqual(payload["args"], ["hello"])
        self.assertEqual(payload["user"], "root")

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

    def test_name_property_fetches_via_lifecycle_client(self):
        sandbox, _ = _make_sandbox()
        sandbox._lifecycle_client = MagicMock()
        sandbox._lifecycle_client.get.return_value = Traced(
            _TRACE_ID, _sandbox_info(name="my-sandbox")
        )

        self.assertEqual(sandbox.name, "my-sandbox")
        self.assertEqual(sandbox.name, "my-sandbox")
        sandbox._lifecycle_client.get.assert_called_once_with("sbx-1")

    def test_name_raises_without_lifecycle_client(self):
        sandbox, _ = _make_sandbox()
        with self.assertRaises(SandboxError):
            _ = sandbox.name

    def test_status_property_fetches_live(self):
        sandbox, _ = _make_sandbox()
        sandbox._lifecycle_client = MagicMock()
        sandbox._lifecycle_client.get.side_effect = [
            Traced(_TRACE_ID, _sandbox_info(status=SandboxStatus.RUNNING)),
            Traced(_TRACE_ID, _sandbox_info(status=SandboxStatus.SUSPENDED)),
        ]

        self.assertEqual(sandbox.status, SandboxStatus.RUNNING)
        self.assertEqual(sandbox.status, SandboxStatus.SUSPENDED)
        self.assertEqual(sandbox._lifecycle_client.get.call_count, 2)

    def test_status_raises_without_lifecycle_client(self):
        sandbox, _ = _make_sandbox()
        with self.assertRaises(SandboxError):
            _ = sandbox.status

    def test_status_prefers_canonical_sandbox_id_over_original_identifier(self):
        sandbox, _ = _make_sandbox()
        sandbox._identifier = "old-name"
        sandbox._sandbox_id = "sbx-1"
        sandbox._lifecycle_client = MagicMock()
        sandbox._lifecycle_client.get.return_value = Traced(
            _TRACE_ID, _sandbox_info(status=SandboxStatus.RUNNING)
        )

        self.assertEqual(sandbox.status, SandboxStatus.RUNNING)
        sandbox._lifecycle_client.get.assert_called_once_with("sbx-1")

    def test_update_calls_lifecycle_client_and_refreshes_name(self):
        sandbox, _ = _make_sandbox()
        sandbox._cached_info = _sandbox_info(name="old-name")
        sandbox._lifecycle_client = MagicMock()
        sandbox._lifecycle_client.update_sandbox.return_value = Traced(
            _TRACE_ID,
            _sandbox_info(
                name="renamed",
                exposed_ports=[8080],
                allow_unauthenticated_access=True,
            ),
        )

        traced = sandbox.update(
            name="renamed",
            allow_unauthenticated_access=True,
            exposed_ports=[8080],
        )

        sandbox._lifecycle_client.update_sandbox.assert_called_once_with(
            "sbx-1",
            name="renamed",
            allow_unauthenticated_access=True,
            exposed_ports=[8080],
        )
        self.assertEqual(traced.name, "renamed")
        self.assertEqual(sandbox.name, "renamed")

    def test_update_prefers_canonical_sandbox_id_over_original_identifier(self):
        sandbox, _ = _make_sandbox()
        sandbox._identifier = "old-name"
        sandbox._sandbox_id = "sbx-1"
        sandbox._cached_info = _sandbox_info(name="old-name")
        sandbox._lifecycle_client = MagicMock()
        sandbox._lifecycle_client.update_sandbox.return_value = Traced(
            _TRACE_ID, _sandbox_info(name="new-name")
        )

        sandbox.update(name="new-name")

        sandbox._lifecycle_client.update_sandbox.assert_called_once_with(
            "sbx-1",
            name="new-name",
            allow_unauthenticated_access=None,
            exposed_ports=None,
        )

    def test_update_raises_without_lifecycle_client(self):
        sandbox, _ = _make_sandbox()
        with self.assertRaises(SandboxError):
            sandbox.update(name="anything")

    def test_name_setter_delegates_to_update(self):
        sandbox, _ = _make_sandbox()
        sandbox._cached_info = _sandbox_info(name="old-env")
        sandbox._lifecycle_client = MagicMock()
        sandbox._lifecycle_client.update_sandbox.return_value = Traced(
            _TRACE_ID, _sandbox_info(name="new-env")
        )

        sandbox.name = "new-env"

        sandbox._lifecycle_client.update_sandbox.assert_called_once_with(
            "sbx-1",
            name="new-env",
            allow_unauthenticated_access=None,
            exposed_ports=None,
        )
        self.assertEqual(sandbox.name, "new-env")

    def test_name_setter_rejects_empty_string(self):
        sandbox, _ = _make_sandbox()
        sandbox._lifecycle_client = MagicMock()
        with self.assertRaises(SandboxError):
            sandbox.name = ""
        sandbox._lifecycle_client.update_sandbox.assert_not_called()

    def test_sandbox_info_accepts_suspending_status(self):
        info = _sandbox_info(status=SandboxStatus.SUSPENDING)
        self.assertEqual(info.status, SandboxStatus.SUSPENDING)

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
