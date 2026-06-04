import json
import unittest
from unittest.mock import patch

from tensorlake.sandbox import (
    PoolInUseError,
    SandboxNotFoundError,
    SnapshotStatus,
    SnapshotType,
    SnapshotWaitCondition,
    _defaults,
)
from tensorlake.sandbox.client import SandboxClient
from tensorlake.sandbox.exceptions import SandboxError


class _FakeRustProxyClient:
    def __init__(self, base_url: str):
        self._base_url = base_url

    def base_url(self):
        return self._base_url


class _FakeRustClient:
    def __init__(self):
        self.create_request_json = None
        self.update_request_json = None
        self.last_get_sandbox_id = None
        self.create_snapshot_calls: list[tuple[str, str | None]] = []
        self.suspend_calls: list[str] = []
        self.resume_calls: list[str] = []
        self.connect_proxy_calls: list[dict] = []

    def close(self):
        return None

    def suspend_sandbox(self, sandbox_id):
        self.suspend_calls.append(sandbox_id)

    def resume_sandbox(self, sandbox_id):
        self.resume_calls.append(sandbox_id)

    def connect_proxy(self, *, proxy_url, sandbox_id, routing_hint=None):
        self.connect_proxy_calls.append(
            {
                "proxy_url": proxy_url,
                "sandbox_id": sandbox_id,
                "routing_hint": routing_hint,
            }
        )
        return _FakeRustProxyClient(proxy_url)

    def create_snapshot(self, sandbox_id, snapshot_type=None):
        self.create_snapshot_calls.append((sandbox_id, snapshot_type))
        return (
            "trace-create",
            '{"snapshot_id":"snap-1","status":"in_progress"}',
        )

    def get_snapshot_json(self, snapshot_id):
        return (
            "trace-get",
            '{"id":"snap-1","namespace":"default","sandbox_id":"sbx-1",'
            '"base_image":"python:3.12","status":"completed",'
            '"snapshot_type":"filesystem",'
            '"snapshot_uri":"s3://snap-1.tar.zst"}',
        )

    def create_sandbox(self, request_json):
        self.create_request_json = request_json
        return ("trace-create-sandbox", '{"sandbox_id":"sbx-1","status":"pending"}')

    def list_sandboxes_json(self):
        return (
            "trace-list-sandboxes",
            """
{
  "sandboxes": [
    {
      "id": "sbx-1",
      "namespace": "default",
      "status": "running",
      "resources": {
        "cpus": 1.0,
        "memory_mb": 512,
        "ephemeral_disk_mb": 1024
      }
    }
  ]
}
""",
        )

    def get_sandbox_json(self, sandbox_id):
        self.last_get_sandbox_id = sandbox_id
        return (
            "trace-get-sandbox",
            """
{
  "id": "sbx-1",
  "namespace": "default",
  "status": "running",
  "resources": {
    "cpus": 1.0,
    "memory_mb": 512,
    "ephemeral_disk_mb": 1024
  },
  "allow_unauthenticated_access": false,
  "exposed_ports": [8080],
  "ingress_endpoint": "https://sandbox.us-east-1.aws.tensorlake.ai",
  "sandbox_url": "https://sbx-1.sandbox.tensorlake.ai"
}
""",
        )

    def update_sandbox(self, sandbox_id, request_json):
        self.last_get_sandbox_id = sandbox_id
        self.update_request_json = request_json
        payload = json.loads(request_json)
        response = {
            "id": sandbox_id,
            "namespace": "default",
            "status": "running",
            "resources": {
                "cpus": 1.0,
                "memory_mb": 512,
                "ephemeral_disk_mb": 1024,
            },
            "name": payload.get("name"),
            "allow_unauthenticated_access": payload.get(
                "allow_unauthenticated_access", False
            ),
            "exposed_ports": payload.get("exposed_ports", []),
            "ingress_endpoint": "https://sandbox.us-east-1.aws.tensorlake.ai",
            "sandbox_url": f"https://{sandbox_id}.sandbox.tensorlake.ai",
        }
        return ("trace-update-sandbox", json.dumps(response))


class _FakeProxyClient:
    def base_url(self):
        return "https://sandbox.tensorlake.ai"

    def close(self):
        return None


class _RecordingCreateRustClient:
    instances = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.create_calls = 0
        self.delete_calls: list[str] = []
        type(self).instances.append(self)

    def close(self):
        return None

    def create_sandbox(self, request_json):
        self.create_calls += 1
        return (
            "trace-create-sandbox",
            '{"sandbox_id":"sbx-1","status":"running"}',
        )

    def connect_proxy(self, *, proxy_url, sandbox_id, routing_hint=None):
        return _FakeProxyClient()

    def delete_sandbox(self, *, sandbox_id):
        self.delete_calls.append(sandbox_id)
        return "trace-delete"


class TestSandboxClientRustBackend(unittest.TestCase):
    def setUp(self):
        _RecordingCreateRustClient.instances = []

    def test_constructor_passes_default_request_timeout_to_rust_backend(self):
        class _RecordingRustClient:
            kwargs = None

            def __init__(self, **kwargs):
                type(self).kwargs = kwargs

            def close(self):
                return None

        with patch(
            "tensorlake.sandbox.client.RustCloudSandboxClient",
            _RecordingRustClient,
        ):
            SandboxClient(api_url="http://localhost:8900", api_key="k", _internal=True)

        self.assertEqual(
            _RecordingRustClient.kwargs["request_timeout_sec"],
            _defaults.DEFAULT_HTTP_TIMEOUT_SEC,
        )

    def test_constructor_passes_custom_request_timeout_to_rust_backend(self):
        class _RecordingRustClient:
            kwargs = None

            def __init__(self, **kwargs):
                type(self).kwargs = kwargs

            def close(self):
                return None

        with patch(
            "tensorlake.sandbox.client.RustCloudSandboxClient",
            _RecordingRustClient,
        ):
            SandboxClient(
                api_url="http://localhost:8900",
                api_key="k",
                request_timeout=123.0,
                _internal=True,
            )

        self.assertEqual(_RecordingRustClient.kwargs["request_timeout_sec"], 123.0)

    def test_create_and_connect_uses_per_call_request_timeout_for_initial_create(self):
        with patch(
            "tensorlake.sandbox.client.RustCloudSandboxClient",
            _RecordingCreateRustClient,
        ):
            client = SandboxClient(
                api_url="http://localhost:8900",
                api_key="k",
                request_timeout=300.0,
                _internal=True,
            )
            client.create_and_connect(image="python:3.11", request_timeout=10.0)

        self.assertEqual(len(_RecordingCreateRustClient.instances), 2)
        self.assertEqual(
            _RecordingCreateRustClient.instances[0].kwargs["request_timeout_sec"],
            300.0,
        )
        self.assertEqual(
            _RecordingCreateRustClient.instances[1].kwargs["request_timeout_sec"],
            10.0,
        )
        self.assertEqual(_RecordingCreateRustClient.instances[1].create_calls, 1)

    def test_sandbox_create_uses_startup_timeout_for_initial_create(self):
        from tensorlake.sandbox.sandbox import Sandbox

        with patch(
            "tensorlake.sandbox.client.RustCloudSandboxClient",
            _RecordingCreateRustClient,
        ):
            Sandbox.create(
                image="python:3.11",
                api_url="http://localhost:8900",
                api_key="k",
                startup_timeout=10.0,
            )

        self.assertEqual(len(_RecordingCreateRustClient.instances), 1)
        self.assertEqual(
            _RecordingCreateRustClient.instances[0].kwargs["request_timeout_sec"],
            10.0,
        )
        self.assertEqual(_RecordingCreateRustClient.instances[0].create_calls, 1)

    def test_sandbox_create_uses_default_request_timeout_when_unspecified(self):
        from tensorlake.sandbox.sandbox import Sandbox

        with patch(
            "tensorlake.sandbox.client.RustCloudSandboxClient",
            _RecordingCreateRustClient,
        ):
            Sandbox.create(
                image="python:3.11",
                api_url="http://localhost:8900",
                api_key="k",
            )

        self.assertEqual(len(_RecordingCreateRustClient.instances), 1)
        self.assertEqual(
            _RecordingCreateRustClient.instances[0].kwargs["request_timeout_sec"],
            _defaults.DEFAULT_HTTP_TIMEOUT_SEC,
        )
        self.assertEqual(_RecordingCreateRustClient.instances[0].create_calls, 1)

    def test_sandbox_proxy_client_does_not_receive_default_request_timeout(self):
        from tensorlake.sandbox.sandbox import Sandbox

        class _RecordingProxyRustClient:
            kwargs = None

            def __init__(self, **kwargs):
                type(self).kwargs = kwargs

            def base_url(self):
                return "https://sbx-1.sandbox.tensorlake.ai"

        with patch(
            "tensorlake.sandbox.sandbox.RustCloudSandboxProxyClient",
            _RecordingProxyRustClient,
        ):
            Sandbox(
                identifier="sbx-1",
                proxy_url="https://sandbox.tensorlake.ai",
                api_key="k",
            )

        self.assertNotIn("request_timeout_sec", _RecordingProxyRustClient.kwargs)

    def test_sandbox_proxy_client_receives_explicit_request_timeout(self):
        from tensorlake.sandbox.sandbox import Sandbox

        class _RecordingProxyRustClient:
            kwargs = None

            def __init__(self, **kwargs):
                type(self).kwargs = kwargs

            def base_url(self):
                return "https://sbx-1.sandbox.tensorlake.ai"

        with patch(
            "tensorlake.sandbox.sandbox.RustCloudSandboxProxyClient",
            _RecordingProxyRustClient,
        ):
            Sandbox(
                identifier="sbx-1",
                proxy_url="https://sandbox.tensorlake.ai",
                api_key="k",
                request_timeout=10.0,
            )

        self.assertEqual(
            _RecordingProxyRustClient.kwargs["request_timeout_sec"],
            10.0,
        )

    def test_sandbox_connect_forwards_explicit_proxy_request_timeout(self):
        from tensorlake.sandbox.sandbox import Sandbox

        class _RecordingRustClient:
            connect_proxy_kwargs = None

            def __init__(self, **kwargs):
                self.kwargs = kwargs

            def close(self):
                return None

            def connect_proxy(self, **kwargs):
                type(self).connect_proxy_kwargs = kwargs
                return _FakeProxyClient()

        with patch(
            "tensorlake.sandbox.client.RustCloudSandboxClient",
            _RecordingRustClient,
        ):
            sandbox = Sandbox.connect(
                "sbx-1",
                api_url="http://localhost:8900",
                api_key="k",
                proxy_url="https://sandbox.tensorlake.ai",
                request_timeout=10.0,
            )
            sandbox.close()

        self.assertEqual(
            _RecordingRustClient.connect_proxy_kwargs["request_timeout_sec"],
            10.0,
        )

    def test_sandbox_connect_does_not_forward_default_proxy_request_timeout(self):
        from tensorlake.sandbox.sandbox import Sandbox

        class _RecordingRustClient:
            connect_proxy_kwargs = None

            def __init__(self, **kwargs):
                self.kwargs = kwargs

            def close(self):
                return None

            def connect_proxy(self, **kwargs):
                type(self).connect_proxy_kwargs = kwargs
                return _FakeProxyClient()

        with patch(
            "tensorlake.sandbox.client.RustCloudSandboxClient",
            _RecordingRustClient,
        ):
            sandbox = Sandbox.connect(
                "sbx-1",
                api_url="http://localhost:8900",
                api_key="k",
                proxy_url="https://sandbox.tensorlake.ai",
            )
            sandbox.close()

        self.assertNotIn(
            "request_timeout_sec",
            _RecordingRustClient.connect_proxy_kwargs,
        )

    def test_create_and_connect_deletes_sandbox_from_timeout_response(self):
        class _TimeoutCreateRustClient(_RecordingCreateRustClient):
            def create_sandbox(self, request_json):
                self.create_calls += 1
                return (
                    "trace-create-sandbox",
                    '{"sandbox_id":"sbx-timeout","status":"timeout"}',
                )

        with patch(
            "tensorlake.sandbox.client.RustCloudSandboxClient",
            _TimeoutCreateRustClient,
        ):
            client = SandboxClient(
                api_url="http://localhost:8900",
                api_key="k",
                request_timeout=300.0,
                _internal=True,
            )
            with self.assertRaisesRegex(SandboxError, "did not start"):
                client.create_and_connect(image="python:3.11", request_timeout=10.0)

        self.assertEqual(len(_TimeoutCreateRustClient.instances), 2)
        self.assertEqual(
            _TimeoutCreateRustClient.instances[1].delete_calls,
            ["sbx-timeout"],
        )

    def test_connect_accepts_sandbox_name(self):
        client = SandboxClient(api_url="http://localhost:8900", api_key="k")

        with patch("tensorlake.sandbox.sandbox.Sandbox") as sandbox_cls:
            sandbox_cls.return_value.sandbox_id = "stable-name"

            sandbox = client.connect(
                "stable-name", proxy_url="https://sandbox.tensorlake.ai"
            )

            self.assertEqual(sandbox.sandbox_id, "stable-name")

    def test_connect_rejects_conflicting_identifier_aliases(self):
        client = SandboxClient(api_url="http://localhost:8900", api_key="k")

        with self.assertRaisesRegex(SandboxError, "Provide only one of"):
            client.connect("stable-name", sandbox_id="sbx-123")

    def test_connect_resolves_ingress_endpoint_when_proxy_url_omitted(self):
        client = SandboxClient(api_url="https://api.tensorlake.ai", api_key="k")
        fake = _FakeRustClient()
        client._rust_client = fake

        sandbox = client.connect("stable-name")

        self.assertEqual(fake.last_get_sandbox_id, "stable-name")
        self.assertEqual(
            fake.connect_proxy_calls,
            [
                {
                    "proxy_url": "https://sandbox.us-east-1.aws.tensorlake.ai",
                    "sandbox_id": "sbx-1",
                    "routing_hint": None,
                }
            ],
        )
        self.assertEqual(sandbox.sandbox_id, "sbx-1")

    def test_create_uses_rust_backend(self):
        client = SandboxClient(api_url="http://localhost:8900", api_key="k")
        fake = _FakeRustClient()
        client._rust_client = fake

        response = client.create(image="python:3.11", cpus=2.0)

        self.assertEqual(response.sandbox_id, "sbx-1")
        request_json = json.loads(fake.create_request_json)
        self.assertEqual(request_json["image"], "python:3.11")
        self.assertEqual(request_json["resources"]["cpus"], 2.0)
        self.assertEqual(request_json["resources"]["memory_mb"], 1024)

    def test_create_sends_disk_override_when_provided(self):
        client = SandboxClient(api_url="http://localhost:8900", api_key="k")
        fake = _FakeRustClient()
        client._rust_client = fake

        client.create(disk_mb=25 * 1024)

        request_json = json.loads(fake.create_request_json)
        self.assertEqual(request_json["resources"]["disk_mb"], 25 * 1024)
        self.assertNotIn("ephemeral_disk_mb", request_json["resources"])

    def test_create_and_connect_raises_error_details_from_startup_failure(self):
        class _StartupFailureRustClient(_FakeRustClient):
            def get_sandbox_json(self, sandbox_id):
                self.last_get_sandbox_id = sandbox_id
                return (
                    "trace-get-sandbox",
                    """
{
  "id": "sbx-1",
  "namespace": "default",
  "status": "terminated",
  "resources": {
    "cpus": 1.0,
    "memory_mb": 512,
    "ephemeral_disk_mb": 1024
  },
  "error_details": {
    "message": "failed to pull image tensorlake/missing-image"
  }
}
""",
                )

        client = SandboxClient(api_url="http://localhost:8900", api_key="k")
        client._rust_client = _StartupFailureRustClient()

        with self.assertRaisesRegex(
            SandboxError,
            "terminated during startup: failed to pull image tensorlake/missing-image",
        ):
            client.create_and_connect(image="tensorlake/missing-image")

    def test_create_and_connect_uses_ingress_endpoint_from_running_response(self):
        class _RunningRustClient(_FakeRustClient):
            def create_sandbox(self, request_json):
                self.create_request_json = request_json
                return (
                    "trace-create-sandbox",
                    json.dumps(
                        {
                            "sandbox_id": "sbx-1",
                            "status": "running",
                            "routing_hint": "hint-1",
                            "ingress_endpoint": "https://sandbox.us-east-1.aws.tensorlake.ai",
                        }
                    ),
                )

        client = SandboxClient(api_url="https://api.tensorlake.ai", api_key="k")
        fake = _RunningRustClient()
        client._rust_client = fake

        sandbox = client.create_and_connect(image="python:3.11")

        self.assertEqual(sandbox.sandbox_id, "sbx-1")
        self.assertEqual(
            fake.connect_proxy_calls,
            [
                {
                    "proxy_url": "https://sandbox.us-east-1.aws.tensorlake.ai",
                    "sandbox_id": "sbx-1",
                    "routing_hint": "hint-1",
                }
            ],
        )

    def test_list_uses_rust_backend(self):
        client = SandboxClient(api_url="http://localhost:8900", api_key="k")
        client._rust_client = _FakeRustClient()

        sandboxes = client.list()
        sandboxes_list = list(sandboxes)

        self.assertEqual(len(sandboxes_list), 1)
        self.assertEqual(sandboxes_list[0].sandbox_id, "sbx-1")
        self.assertEqual(sandboxes_list[0].status, "running")

    def test_update_sandbox_sends_port_access_fields(self):
        client = SandboxClient(api_url="http://localhost:8900", api_key="k")
        fake = _FakeRustClient()
        client._rust_client = fake

        info = client.update_sandbox(
            "sbx-1",
            name="renamed",
            allow_unauthenticated_access=True,
            exposed_ports=[8081, 8080, 8081],
        )

        request_json = json.loads(fake.update_request_json)
        self.assertEqual(request_json["name"], "renamed")
        self.assertTrue(request_json["allow_unauthenticated_access"])
        self.assertEqual(request_json["exposed_ports"], [8080, 8081])
        self.assertTrue(info.allow_unauthenticated_access)
        self.assertEqual(info.exposed_ports, [8080, 8081])

    def test_get_port_access_reads_current_settings(self):
        client = SandboxClient(api_url="http://localhost:8900", api_key="k")
        client._rust_client = _FakeRustClient()

        access = client.get_port_access("sbx-1")

        self.assertFalse(access.allow_unauthenticated_access)
        self.assertEqual(access.exposed_ports, [8080])
        self.assertEqual(
            access.ingress_endpoint,
            "https://sandbox.us-east-1.aws.tensorlake.ai",
        )
        self.assertEqual(access.sandbox_url, "https://sbx-1.sandbox.tensorlake.ai")

    def test_expose_ports_merges_existing_ports(self):
        client = SandboxClient(api_url="http://localhost:8900", api_key="k")
        fake = _FakeRustClient()
        client._rust_client = fake

        info = client.expose_ports(
            "sbx-1",
            [8081, 8080],
            allow_unauthenticated_access=True,
        )

        request_json = json.loads(fake.update_request_json)
        self.assertEqual(request_json["exposed_ports"], [8080, 8081])
        self.assertTrue(request_json["allow_unauthenticated_access"])
        self.assertEqual(info.exposed_ports, [8080, 8081])

    def test_unexpose_ports_removes_ports_and_disables_public_access_when_empty(self):
        client = SandboxClient(api_url="http://localhost:8900", api_key="k")
        fake = _FakeRustClient()
        client._rust_client = fake

        info = client.unexpose_ports("sbx-1", [8080])

        request_json = json.loads(fake.update_request_json)
        self.assertEqual(request_json["exposed_ports"], [])
        self.assertFalse(request_json["allow_unauthenticated_access"])
        self.assertEqual(info.exposed_ports, [])

    def test_port_management_rejects_reserved_management_port(self):
        client = SandboxClient(api_url="http://localhost:8900", api_key="k")
        client._rust_client = _FakeRustClient()

        with self.assertRaisesRegex(SandboxError, "reserved for sandbox management"):
            client.expose_ports("sbx-1", [9501])

    def test_suspend_calls_rust_backend(self):
        client = SandboxClient(api_url="http://localhost:8900", api_key="k")
        fake = _FakeRustClient()
        client._rust_client = fake

        client.suspend("my-env", wait=False)

        self.assertEqual(fake.suspend_calls, ["my-env"])
        self.assertEqual(fake.resume_calls, [])

    def test_resume_calls_rust_backend(self):
        client = SandboxClient(api_url="http://localhost:8900", api_key="k")
        fake = _FakeRustClient()
        client._rust_client = fake

        client.resume("my-env", wait=False)

        self.assertEqual(fake.resume_calls, ["my-env"])
        self.assertEqual(fake.suspend_calls, [])

    def test_suspend_maps_404_to_sandbox_not_found(self):
        class FakeRustError(Exception):
            pass

        class _NotFoundRustClient:
            def close(self):
                return None

            def suspend_sandbox(self, sandbox_id):
                raise FakeRustError(
                    ("remote_api", 404, f"sandbox {sandbox_id} not found")
                )

        import tensorlake.sandbox.client as sandbox_client_module

        previous = sandbox_client_module.RustCloudSandboxClientError
        try:
            sandbox_client_module.RustCloudSandboxClientError = FakeRustError
            client = SandboxClient(api_url="http://localhost:8900", api_key="k")
            client._rust_client = _NotFoundRustClient()

            with self.assertRaises(SandboxNotFoundError):
                client.suspend("missing")
        finally:
            sandbox_client_module.RustCloudSandboxClientError = previous

    def test_resume_maps_404_to_sandbox_not_found(self):
        class FakeRustError(Exception):
            pass

        class _NotFoundRustClient:
            def close(self):
                return None

            def resume_sandbox(self, sandbox_id):
                raise FakeRustError(
                    ("remote_api", 404, f"sandbox {sandbox_id} not found")
                )

        import tensorlake.sandbox.client as sandbox_client_module

        previous = sandbox_client_module.RustCloudSandboxClientError
        try:
            sandbox_client_module.RustCloudSandboxClientError = FakeRustError
            client = SandboxClient(api_url="http://localhost:8900", api_key="k")
            client._rust_client = _NotFoundRustClient()

            with self.assertRaises(SandboxNotFoundError):
                client.resume("missing")
        finally:
            sandbox_client_module.RustCloudSandboxClientError = previous

    def test_get_maps_404_to_sandbox_not_found(self):
        class FakeRustError(Exception):
            pass

        class _NotFoundRustClient:
            def close(self):
                return None

            def get_sandbox_json(self, sandbox_id):
                raise FakeRustError(
                    ("remote_api", 404, f"sandbox {sandbox_id} not found")
                )

        import tensorlake.sandbox.client as sandbox_client_module

        previous = sandbox_client_module.RustCloudSandboxClientError
        try:
            sandbox_client_module.RustCloudSandboxClientError = FakeRustError
            client = SandboxClient(api_url="http://localhost:8900", api_key="k")
            client._rust_client = _NotFoundRustClient()

            with self.assertRaises(SandboxNotFoundError):
                client.get("missing")
        finally:
            sandbox_client_module.RustCloudSandboxClientError = previous

    def test_delete_pool_maps_409_to_pool_in_use(self):
        class FakeRustError(Exception):
            pass

        class _PoolInUseRustClient:
            def close(self):
                return None

            def delete_pool(self, pool_id):
                raise FakeRustError(("remote_api", 409, f"pool {pool_id} is in use"))

        import tensorlake.sandbox.client as sandbox_client_module

        previous = sandbox_client_module.RustCloudSandboxClientError
        try:
            sandbox_client_module.RustCloudSandboxClientError = FakeRustError
            client = SandboxClient(api_url="http://localhost:8900", api_key="k")
            client._rust_client = _PoolInUseRustClient()

            with self.assertRaises(PoolInUseError):
                client.delete_pool("pool-1")
        finally:
            sandbox_client_module.RustCloudSandboxClientError = previous

    def test_snapshot_threads_snapshot_type_to_rust_backend(self):
        client = SandboxClient(api_url="http://localhost:8900", api_key="k")
        fake = _FakeRustClient()
        client._rust_client = fake

        info = client.snapshot_and_wait(
            "sbx-1",
            snapshot_type=SnapshotType.FILESYSTEM,
        )

        self.assertEqual(len(fake.create_snapshot_calls), 1)
        sandbox_id, snapshot_type = fake.create_snapshot_calls[0]
        self.assertEqual(sandbox_id, "sbx-1")
        self.assertEqual(snapshot_type, "filesystem")
        self.assertEqual(info.snapshot_id, "snap-1")
        self.assertEqual(info.snapshot_type, SnapshotType.FILESYSTEM)

    def test_snapshot_and_wait_returns_on_local_ready_by_default(self):
        class _LocalReadyRustClient(_FakeRustClient):
            def get_snapshot_json(self, snapshot_id):
                return (
                    "trace-get",
                    '{"id":"snap-1","namespace":"default","sandbox_id":"sbx-1",'
                    '"base_image":"python:3.12","status":"local_ready"}',
                )

        client = SandboxClient(api_url="http://localhost:8900", api_key="k")
        client._rust_client = _LocalReadyRustClient()

        info = client.snapshot_and_wait("sbx-1")

        self.assertEqual(info.status, SnapshotStatus.LOCAL_READY)
        self.assertIsNone(info.snapshot_uri)

    def test_snapshot_and_wait_can_wait_for_completed(self):
        class _SnapshotSequenceRustClient(_FakeRustClient):
            def __init__(self):
                super().__init__()
                self.get_calls = 0

            def get_snapshot_json(self, snapshot_id):
                self.get_calls += 1
                status = "local_ready" if self.get_calls == 1 else "completed"
                return (
                    "trace-get",
                    '{"id":"snap-1","namespace":"default","sandbox_id":"sbx-1",'
                    f'"base_image":"python:3.12","status":"{status}",'
                    '"snapshot_uri":"s3://snap-1.tar.zst"}',
                )

        client = SandboxClient(api_url="http://localhost:8900", api_key="k")
        fake = _SnapshotSequenceRustClient()
        client._rust_client = fake

        info = client.snapshot_and_wait(
            "sbx-1",
            poll_interval=0,
            wait_until=SnapshotWaitCondition.COMPLETED,
        )

        self.assertEqual(info.status, SnapshotStatus.COMPLETED)
        self.assertEqual(fake.get_calls, 2)

    def test_snapshot_omits_snapshot_type_when_none(self):
        client = SandboxClient(api_url="http://localhost:8900", api_key="k")
        fake = _FakeRustClient()
        client._rust_client = fake

        client.snapshot_and_wait("sbx-1")

        self.assertEqual(len(fake.create_snapshot_calls), 1)
        _, snapshot_type = fake.create_snapshot_calls[0]
        self.assertIsNone(snapshot_type)


if __name__ == "__main__":
    unittest.main()
