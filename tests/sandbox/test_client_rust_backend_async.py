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
from tensorlake.sandbox.async_client import AsyncSandboxClient
from tensorlake.sandbox.exceptions import SandboxError


class _FakeAsyncRustProxyClient:
    def __init__(self, base_url: str):
        self._base_url = base_url

    def base_url(self):
        return self._base_url


class _FakeAsyncRustClient:
    def __init__(self):
        self.create_request_json: str = ""
        self.update_request_json: str = ""
        self.last_get_sandbox_id: str = ""
        self.create_snapshot_calls: list[tuple[str, str | None]] = []
        self.copy_calls: list[tuple[str, int]] = []
        self.suspend_calls: list[str] = []
        self.resume_calls: list[str] = []
        self.connect_proxy_calls: list[dict] = []

    def close(self):
        return None

    def connect_proxy(self, *, proxy_url, sandbox_id, routing_hint=None):
        self.connect_proxy_calls.append(
            {
                "proxy_url": proxy_url,
                "sandbox_id": sandbox_id,
                "routing_hint": routing_hint,
            }
        )
        return _FakeAsyncRustProxyClient(proxy_url)

    def select_sandbox_proxy_url(
        self,
        *,
        sandbox_id,
        sandbox_url=None,
        ingress_endpoint=None,
        explicit_proxy_url=None,
    ):
        if sandbox_url:
            return sandbox_url
        if explicit_proxy_url:
            return explicit_proxy_url
        raise RuntimeError(
            "server response did not include sandbox_url; refusing to derive a proxy URL"
        )

    async def suspend_sandbox_async(self, *, sandbox_id):
        self.suspend_calls.append(sandbox_id)
        return "trace-suspend"

    async def resume_sandbox_async(self, *, sandbox_id):
        self.resume_calls.append(sandbox_id)
        return "trace-resume"

    async def create_snapshot_async(self, *, sandbox_id, snapshot_type=None):
        self.create_snapshot_calls.append((sandbox_id, snapshot_type))
        return (
            "trace-create",
            '{"snapshot_id":"snap-1","status":"in_progress"}',
        )

    async def get_snapshot_json_async(self, *, snapshot_id):
        return (
            "trace-get",
            '{"id":"snap-1","namespace":"default","sandbox_id":"sbx-1",'
            '"base_image":"python:3.12","status":"completed",'
            '"snapshot_type":"filesystem",'
            '"snapshot_uri":"s3://snap-1.tar.zst"}',
        )

    async def create_sandbox_async(self, *, request_json):
        self.create_request_json = request_json
        return (
            "trace-create-sandbox",
            '{"sandbox_id":"sbx-1","status":"pending"}',
        )

    async def copy_sandbox_async(self, *, sandbox_id, times):
        self.copy_calls.append((sandbox_id, times))
        return (
            "trace-copy-sandbox",
            json.dumps(
                {
                    "source_sandbox_id": sandbox_id,
                    "sandboxes": [
                        {"sandbox_id": "copy-1", "status": "running"},
                        {
                            "sandbox_id": "copy-2",
                            "status": "failed",
                            "reason": "no capacity",
                        },
                    ],
                }
            ),
        )

    async def list_sandboxes_json_async(self):
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

    async def get_sandbox_json_async(self, *, sandbox_id) -> tuple[str, str]:
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

    async def update_sandbox_async(self, *, sandbox_id, request_json):
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

    async def create_sandbox_async(self, *, request_json):
        self.create_calls += 1
        return (
            "trace-create-sandbox",
            '{"sandbox_id":"sbx-1","status":"running",'
            '"sandbox_url":"https://sbx-1.sandbox.tensorlake.ai"}',
        )

    def select_sandbox_proxy_url(
        self,
        *,
        sandbox_id,
        sandbox_url=None,
        ingress_endpoint=None,
        explicit_proxy_url=None,
    ):
        if sandbox_url:
            return sandbox_url
        if explicit_proxy_url:
            return explicit_proxy_url
        raise RuntimeError(
            "server response did not include sandbox_url; refusing to derive a proxy URL"
        )

    def connect_proxy(self, *, proxy_url, sandbox_id, routing_hint=None):
        return _FakeProxyClient()

    async def delete_sandbox_async(self, *, sandbox_id):
        self.delete_calls.append(sandbox_id)
        return "trace-delete"


def _make_client(fake: object | None = None) -> AsyncSandboxClient:
    client = AsyncSandboxClient(
        api_url="http://localhost:8900", api_key="k", _internal=True
    )
    client._rust_client = fake if fake is not None else _FakeAsyncRustClient()
    return client


class _StatusSequenceRustClient(_FakeAsyncRustClient):
    """Returns each status from `statuses` on successive get calls; the last
    status repeats once exhausted."""

    def __init__(self, statuses: list[str], returned_sandbox_id: str | None = None):
        super().__init__()
        self._statuses = statuses
        self._returned_sandbox_id = returned_sandbox_id
        self.get_calls = 0

    async def get_sandbox_json_async(self, *, sandbox_id) -> tuple[str, str]:
        self.last_get_sandbox_id = sandbox_id
        idx = min(self.get_calls, len(self._statuses) - 1)
        self.get_calls += 1
        returned_sandbox_id = self._returned_sandbox_id or sandbox_id
        return (
            "trace-get-sandbox",
            json.dumps(
                {
                    "id": returned_sandbox_id,
                    "namespace": "default",
                    "status": self._statuses[idx],
                    "resources": {
                        "cpus": 1.0,
                        "memory_mb": 512,
                        "ephemeral_disk_mb": 1024,
                    },
                    "routing_hint": "hint-2",
                    "sandbox_url": f"https://{returned_sandbox_id}.sandbox.tensorlake.ai",
                }
            ),
        )


class TestAsyncSandboxClientRustBackend(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        _RecordingCreateRustClient.instances = []

    async def test_constructor_passes_default_request_timeout_to_rust_backend(self):
        class _RecordingRustClient:
            kwargs = None

            def __init__(self, **kwargs):
                type(self).kwargs = kwargs

            def close(self):
                return None

        with patch(
            "tensorlake.sandbox.async_client.RustCloudSandboxClient",
            _RecordingRustClient,
        ):
            AsyncSandboxClient(
                api_url="http://localhost:8900", api_key="k", _internal=True
            )

        self.assertEqual(
            _RecordingRustClient.kwargs["request_timeout_sec"],
            _defaults.DEFAULT_HTTP_TIMEOUT_SEC,
        )

    async def test_constructor_passes_custom_request_timeout_to_rust_backend(self):
        class _RecordingRustClient:
            kwargs = None

            def __init__(self, **kwargs):
                type(self).kwargs = kwargs

            def close(self):
                return None

        with patch(
            "tensorlake.sandbox.async_client.RustCloudSandboxClient",
            _RecordingRustClient,
        ):
            AsyncSandboxClient(
                api_url="http://localhost:8900",
                api_key="k",
                request_timeout=123.0,
                _internal=True,
            )

        self.assertEqual(_RecordingRustClient.kwargs["request_timeout_sec"], 123.0)

    async def test_create_and_connect_uses_per_call_request_timeout_for_initial_create(
        self,
    ):
        with patch(
            "tensorlake.sandbox.async_client.RustCloudSandboxClient",
            _RecordingCreateRustClient,
        ):
            client = AsyncSandboxClient(
                api_url="http://localhost:8900",
                api_key="k",
                request_timeout=300.0,
                _internal=True,
            )
            await client.create_and_connect(image="python:3.11", request_timeout=10.0)

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

    async def test_sandbox_create_uses_startup_timeout_for_initial_create(self):
        from tensorlake.sandbox.async_sandbox import AsyncSandbox

        with patch(
            "tensorlake.sandbox.async_client.RustCloudSandboxClient",
            _RecordingCreateRustClient,
        ):
            await AsyncSandbox.create(
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

    async def test_sandbox_create_uses_default_request_timeout_when_unspecified(self):
        from tensorlake.sandbox.async_sandbox import AsyncSandbox

        with patch(
            "tensorlake.sandbox.async_client.RustCloudSandboxClient",
            _RecordingCreateRustClient,
        ):
            await AsyncSandbox.create(
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

    async def test_sandbox_proxy_client_does_not_receive_default_request_timeout(self):
        from tensorlake.sandbox.async_sandbox import AsyncSandbox

        class _RecordingProxyRustClient:
            kwargs = None

            def __init__(self, **kwargs):
                type(self).kwargs = kwargs

            def base_url(self):
                return "https://sbx-1.sandbox.tensorlake.ai"

        with patch(
            "tensorlake.sandbox.async_sandbox.RustCloudSandboxProxyClient",
            _RecordingProxyRustClient,
        ):
            AsyncSandbox(
                identifier="sbx-1",
                proxy_url="https://sandbox.tensorlake.ai",
                api_key="k",
            )

        self.assertNotIn("request_timeout_sec", _RecordingProxyRustClient.kwargs)

    async def test_sandbox_proxy_client_receives_explicit_request_timeout(self):
        from tensorlake.sandbox.async_sandbox import AsyncSandbox

        class _RecordingProxyRustClient:
            kwargs = None

            def __init__(self, **kwargs):
                type(self).kwargs = kwargs

            def base_url(self):
                return "https://sbx-1.sandbox.tensorlake.ai"

        with patch(
            "tensorlake.sandbox.async_sandbox.RustCloudSandboxProxyClient",
            _RecordingProxyRustClient,
        ):
            AsyncSandbox(
                identifier="sbx-1",
                proxy_url="https://sandbox.tensorlake.ai",
                api_key="k",
                request_timeout=10.0,
            )

        self.assertEqual(
            _RecordingProxyRustClient.kwargs["request_timeout_sec"],
            10.0,
        )

    async def test_sandbox_connect_forwards_explicit_proxy_request_timeout(self):
        from tensorlake.sandbox.async_sandbox import AsyncSandbox

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
            "tensorlake.sandbox.async_client.RustCloudSandboxClient",
            _RecordingRustClient,
        ):
            sandbox = await AsyncSandbox.connect(
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

    async def test_sandbox_connect_does_not_forward_default_proxy_request_timeout(self):
        from tensorlake.sandbox.async_sandbox import AsyncSandbox

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
            "tensorlake.sandbox.async_client.RustCloudSandboxClient",
            _RecordingRustClient,
        ):
            sandbox = await AsyncSandbox.connect(
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

    async def test_create_and_connect_deletes_sandbox_from_timeout_response(self):
        class _TimeoutCreateRustClient(_RecordingCreateRustClient):
            async def create_sandbox_async(self, *, request_json):
                self.create_calls += 1
                return (
                    "trace-create-sandbox",
                    '{"sandbox_id":"sbx-timeout","status":"timeout"}',
                )

        with patch(
            "tensorlake.sandbox.async_client.RustCloudSandboxClient",
            _TimeoutCreateRustClient,
        ):
            client = AsyncSandboxClient(
                api_url="http://localhost:8900",
                api_key="k",
                request_timeout=300.0,
                _internal=True,
            )
            with self.assertRaisesRegex(SandboxError, "did not start"):
                await client.create_and_connect(
                    image="python:3.11",
                    request_timeout=10.0,
                )

        self.assertEqual(len(_TimeoutCreateRustClient.instances), 2)
        self.assertEqual(
            _TimeoutCreateRustClient.instances[1].delete_calls,
            ["sbx-timeout"],
        )

    async def test_connect_accepts_sandbox_name(self):
        client = _make_client()

        with patch(
            "tensorlake.sandbox.async_sandbox.AsyncSandbox"
        ) as async_sandbox_cls:
            async_sandbox_cls.return_value.sandbox_id = "stable-name"

            sandbox = await client.connect(
                "stable-name", proxy_url="https://sandbox.tensorlake.ai"
            )

            self.assertEqual(sandbox.sandbox_id, "stable-name")

    async def test_connect_rejects_conflicting_identifier_aliases(self):
        client = _make_client()

        with self.assertRaisesRegex(SandboxError, "Provide only one of"):
            await client.connect("stable-name", sandbox_id="sbx-123")

    async def test_connect_prefers_server_sandbox_url_when_proxy_url_omitted(self):
        fake = _FakeAsyncRustClient()
        client = _make_client(fake)

        sandbox = await client.connect("stable-name")

        self.assertEqual(fake.last_get_sandbox_id, "stable-name")
        self.assertEqual(
            fake.connect_proxy_calls,
            [
                {
                    "proxy_url": "https://sbx-1.sandbox.tensorlake.ai",
                    "sandbox_id": "sbx-1",
                    "routing_hint": None,
                }
            ],
        )
        self.assertEqual(sandbox.sandbox_id, "sbx-1")

    async def test_connect_uses_env_proxy_override_when_server_url_missing(self):
        class _NoSandboxUrlRustClient(_FakeAsyncRustClient):
            async def get_sandbox_json_async(self, *, sandbox_id):
                self.last_get_sandbox_id = sandbox_id
                return (
                    "trace-get-sandbox",
                    json.dumps(
                        {
                            "id": "sbx-1",
                            "namespace": "default",
                            "status": "running",
                            "resources": {
                                "cpus": 1.0,
                                "memory_mb": 512,
                                "ephemeral_disk_mb": 1024,
                            },
                        }
                    ),
                )

        fake = _NoSandboxUrlRustClient()
        client = _make_client(fake)

        with patch.dict(
            "os.environ",
            {"TENSORLAKE_SANDBOX_PROXY_URL": "https://override.example.com"},
        ):
            sandbox = await client.connect("stable-name")

        self.assertEqual(fake.last_get_sandbox_id, "stable-name")
        self.assertEqual(sandbox.sandbox_id, "sbx-1")
        self.assertEqual(
            fake.connect_proxy_calls,
            [
                {
                    "proxy_url": "https://override.example.com",
                    "sandbox_id": "sbx-1",
                    "routing_hint": None,
                }
            ],
        )

    async def test_create_uses_rust_backend(self):
        fake = _FakeAsyncRustClient()
        client = _make_client(fake)

        response = await client.create(image="python:3.11", cpus=2.0)

        self.assertEqual(response.sandbox_id, "sbx-1")
        request_json = json.loads(fake.create_request_json)
        self.assertEqual(request_json["image"], "python:3.11")
        self.assertEqual(request_json["resources"]["cpus"], 2.0)
        self.assertEqual(request_json["resources"]["memory_mb"], 1024)

    async def test_create_sends_disk_override_when_provided(self):
        fake = _FakeAsyncRustClient()
        client = _make_client(fake)

        await client.create(disk_mb=25 * 1024)

        request_json = json.loads(fake.create_request_json)
        self.assertEqual(request_json["resources"]["disk_mb"], 25 * 1024)
        self.assertNotIn("ephemeral_disk_mb", request_json["resources"])

    async def test_create_and_connect_raises_error_details_from_startup_failure(self):
        class _StartupFailureRustClient(_FakeAsyncRustClient):
            async def get_sandbox_json_async(self, *, sandbox_id):
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

            async def delete_sandbox_async(self, *, sandbox_id):
                return "trace-delete"

        client = _make_client(_StartupFailureRustClient())

        with self.assertRaisesRegex(
            SandboxError,
            "terminated during startup: failed to pull image tensorlake/missing-image",
        ):
            await client.create_and_connect(image="tensorlake/missing-image")

    async def test_create_and_connect_uses_sandbox_url_from_running_response(self):
        class _RunningRustClient(_FakeAsyncRustClient):
            async def create_sandbox_async(self, *, request_json):
                self.create_request_json = request_json
                return (
                    "trace-create-sandbox",
                    json.dumps(
                        {
                            "sandbox_id": "sbx-1",
                            "status": "running",
                            "routing_hint": "hint-1",
                            "ingress_endpoint": "https://sandbox.us-east-1.aws.tensorlake.ai",
                            "sandbox_url": "https://sbx-1.sandbox.gcp-use4.tensorlake.ai",
                        }
                    ),
                )

        fake = _RunningRustClient()
        client = _make_client(fake)

        sandbox = await client.create_and_connect(image="python:3.11")

        self.assertEqual(sandbox.sandbox_id, "sbx-1")
        self.assertEqual(
            fake.connect_proxy_calls,
            [
                {
                    "proxy_url": "https://sbx-1.sandbox.gcp-use4.tensorlake.ai",
                    "sandbox_id": "sbx-1",
                    "routing_hint": "hint-1",
                }
            ],
        )

    async def test_create_and_connect_uses_env_proxy_override_when_server_url_missing(
        self,
    ):
        class _RunningRustClient(_FakeAsyncRustClient):
            async def create_sandbox_async(self, *, request_json):
                self.create_request_json = request_json
                return (
                    "trace-create-sandbox",
                    json.dumps(
                        {
                            "sandbox_id": "sbx-1",
                            "status": "running",
                        }
                    ),
                )

        fake = _RunningRustClient()
        client = _make_client(fake)

        with patch.dict(
            "os.environ",
            {"TENSORLAKE_SANDBOX_PROXY_URL": "https://override.example.com"},
        ):
            sandbox = await client.create_and_connect(image="python:3.11")

        self.assertEqual(sandbox.sandbox_id, "sbx-1")
        self.assertEqual(
            fake.connect_proxy_calls,
            [
                {
                    "proxy_url": "https://override.example.com",
                    "sandbox_id": "sbx-1",
                    "routing_hint": None,
                }
            ],
        )

    async def test_create_and_connect_polls_until_running(self):
        fake = _StatusSequenceRustClient(["pending", "running"])
        client = _make_client(fake)

        with patch(
            "tensorlake.sandbox.async_sandbox.AsyncSandbox"
        ) as async_sandbox_cls:
            async_sandbox_cls.return_value.sandbox_id = "sbx-1"

            sandbox = await client.create_and_connect(image="python:3.11")

            self.assertEqual(sandbox.sandbox_id, "sbx-1")
            self.assertGreaterEqual(fake.get_calls, 2)
            self.assertEqual(len(fake.connect_proxy_calls), 1)

    async def test_create_and_connect_uses_canonical_id_from_polled_running_response(
        self,
    ):
        fake = _StatusSequenceRustClient(
            ["running"], returned_sandbox_id="sbx-canonical"
        )
        client = _make_client(fake)

        sandbox = await client.create_and_connect(image="python:3.11")

        self.assertEqual(fake.last_get_sandbox_id, "sbx-1")
        self.assertEqual(sandbox.sandbox_id, "sbx-canonical")
        self.assertEqual(
            fake.connect_proxy_calls,
            [
                {
                    "proxy_url": "https://sbx-canonical.sandbox.tensorlake.ai",
                    "sandbox_id": "sbx-canonical",
                    "routing_hint": "hint-2",
                }
            ],
        )

    async def test_list_uses_rust_backend(self):
        client = _make_client()

        sandboxes = await client.list()
        sandboxes_list = list(sandboxes)

        self.assertEqual(len(sandboxes_list), 1)
        self.assertEqual(sandboxes_list[0].sandbox_id, "sbx-1")
        self.assertEqual(sandboxes_list[0].status, "running")

    async def test_copy_uses_rust_backend_and_allows_partial_failures(self):
        fake = _FakeAsyncRustClient()
        client = _make_client(fake)

        response = await client.copy("sbx-1", times=2)

        self.assertEqual(response.trace_id, "trace-copy-sandbox")
        self.assertEqual(fake.copy_calls, [("sbx-1", 2)])
        self.assertEqual(response.source_sandbox_id, "sbx-1")
        self.assertEqual(response.sandboxes[0].sandbox_id, "copy-1")
        self.assertEqual(response.sandboxes[0].status, "running")
        self.assertEqual(response.sandboxes[1].status, "failed")
        self.assertEqual(response.sandboxes[1].reason, "no capacity")

    async def test_copy_rejects_invalid_times(self):
        client = _make_client()

        with self.assertRaisesRegex(SandboxError, "times must be a positive integer"):
            await client.copy("sbx-1", times=0)

    async def test_update_sandbox_sends_port_access_fields(self):
        fake = _FakeAsyncRustClient()
        client = _make_client(fake)

        info = await client.update_sandbox(
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

    async def test_get_port_access_reads_current_settings(self):
        client = _make_client()

        access = await client.get_port_access("sbx-1")

        self.assertFalse(access.allow_unauthenticated_access)
        self.assertEqual(access.exposed_ports, [8080])
        self.assertEqual(
            access.ingress_endpoint,
            "https://sandbox.us-east-1.aws.tensorlake.ai",
        )
        self.assertEqual(access.sandbox_url, "https://sbx-1.sandbox.tensorlake.ai")

    async def test_expose_ports_merges_existing_ports(self):
        fake = _FakeAsyncRustClient()
        client = _make_client(fake)

        info = await client.expose_ports(
            "sbx-1",
            [8081, 8080],
            allow_unauthenticated_access=True,
        )

        request_json = json.loads(fake.update_request_json)
        self.assertEqual(request_json["exposed_ports"], [8080, 8081])
        self.assertTrue(request_json["allow_unauthenticated_access"])
        self.assertEqual(info.exposed_ports, [8080, 8081])

    async def test_unexpose_ports_removes_ports_and_disables_public_access_when_empty(
        self,
    ):
        fake = _FakeAsyncRustClient()
        client = _make_client(fake)

        info = await client.unexpose_ports("sbx-1", [8080])

        request_json = json.loads(fake.update_request_json)
        self.assertEqual(request_json["exposed_ports"], [])
        self.assertFalse(request_json["allow_unauthenticated_access"])
        self.assertEqual(info.exposed_ports, [])

    async def test_port_management_rejects_reserved_management_port(self):
        client = _make_client()

        with self.assertRaisesRegex(SandboxError, "reserved for sandbox management"):
            await client.expose_ports("sbx-1", [9501])

    async def test_suspend_calls_rust_backend(self):
        fake = _FakeAsyncRustClient()
        client = _make_client(fake)

        await client.suspend("my-env", wait=False)

        self.assertEqual(fake.suspend_calls, ["my-env"])
        self.assertEqual(fake.resume_calls, [])

    async def test_resume_calls_rust_backend(self):
        fake = _FakeAsyncRustClient()
        client = _make_client(fake)

        await client.resume("my-env", wait=False)

        self.assertEqual(fake.resume_calls, ["my-env"])
        self.assertEqual(fake.suspend_calls, [])

    async def test_suspend_polls_until_suspended(self):
        fake = _StatusSequenceRustClient(["running", "suspended"])
        client = _make_client(fake)

        await client.suspend("sbx-1", wait=True, timeout=2.0, poll_interval=0.01)

        self.assertEqual(fake.suspend_calls, ["sbx-1"])
        self.assertGreaterEqual(fake.get_calls, 2)

    async def test_resume_polls_until_running(self):
        fake = _StatusSequenceRustClient(["suspended", "running"])
        client = _make_client(fake)

        await client.resume("sbx-1", wait=True, timeout=2.0, poll_interval=0.01)

        self.assertEqual(fake.resume_calls, ["sbx-1"])
        self.assertGreaterEqual(fake.get_calls, 2)

    async def test_suspend_maps_404_to_sandbox_not_found(self):
        class FakeRustError(Exception):
            pass

        class _NotFoundRustClient:
            def close(self):
                return None

            async def suspend_sandbox_async(self, *, sandbox_id):
                raise FakeRustError(
                    ("remote_api", 404, f"sandbox {sandbox_id} not found")
                )

        import tensorlake.sandbox.client as sandbox_client_module

        previous = sandbox_client_module.RustCloudSandboxClientError
        try:
            sandbox_client_module.RustCloudSandboxClientError = FakeRustError
            client = _make_client(_NotFoundRustClient())

            with self.assertRaises(SandboxNotFoundError):
                await client.suspend("missing")
        finally:
            sandbox_client_module.RustCloudSandboxClientError = previous

    async def test_resume_maps_404_to_sandbox_not_found(self):
        class FakeRustError(Exception):
            pass

        class _NotFoundRustClient:
            def close(self):
                return None

            async def resume_sandbox_async(self, *, sandbox_id):
                raise FakeRustError(
                    ("remote_api", 404, f"sandbox {sandbox_id} not found")
                )

        import tensorlake.sandbox.client as sandbox_client_module

        previous = sandbox_client_module.RustCloudSandboxClientError
        try:
            sandbox_client_module.RustCloudSandboxClientError = FakeRustError
            client = _make_client(_NotFoundRustClient())

            with self.assertRaises(SandboxNotFoundError):
                await client.resume("missing")
        finally:
            sandbox_client_module.RustCloudSandboxClientError = previous

    async def test_get_maps_404_to_sandbox_not_found(self):
        class FakeRustError(Exception):
            pass

        class _NotFoundRustClient:
            def close(self):
                return None

            async def get_sandbox_json_async(self, *, sandbox_id):
                raise FakeRustError(
                    ("remote_api", 404, f"sandbox {sandbox_id} not found")
                )

        import tensorlake.sandbox.client as sandbox_client_module

        previous = sandbox_client_module.RustCloudSandboxClientError
        try:
            sandbox_client_module.RustCloudSandboxClientError = FakeRustError
            client = _make_client(_NotFoundRustClient())

            with self.assertRaises(SandboxNotFoundError):
                await client.get("missing")
        finally:
            sandbox_client_module.RustCloudSandboxClientError = previous

    async def test_delete_pool_maps_409_to_pool_in_use(self):
        class FakeRustError(Exception):
            pass

        class _PoolInUseRustClient:
            def close(self):
                return None

            async def delete_pool_async(self, *, pool_id):
                raise FakeRustError(("remote_api", 409, f"pool {pool_id} is in use"))

        import tensorlake.sandbox.client as sandbox_client_module

        previous = sandbox_client_module.RustCloudSandboxClientError
        try:
            sandbox_client_module.RustCloudSandboxClientError = FakeRustError
            client = _make_client(_PoolInUseRustClient())

            with self.assertRaises(PoolInUseError):
                await client.delete_pool("pool-1")
        finally:
            sandbox_client_module.RustCloudSandboxClientError = previous

    async def test_snapshot_threads_snapshot_type_to_rust_backend(self):
        fake = _FakeAsyncRustClient()
        client = _make_client(fake)

        info = await client.snapshot_and_wait(
            "sbx-1",
            snapshot_type=SnapshotType.FILESYSTEM,
        )

        self.assertEqual(len(fake.create_snapshot_calls), 1)
        sandbox_id, snapshot_type = fake.create_snapshot_calls[0]
        self.assertEqual(sandbox_id, "sbx-1")
        self.assertEqual(snapshot_type, "filesystem")
        self.assertEqual(info.snapshot_id, "snap-1")
        self.assertEqual(info.snapshot_type, SnapshotType.FILESYSTEM)

    async def test_snapshot_and_wait_returns_on_local_ready_by_default(self):
        class _LocalReadyRustClient(_FakeAsyncRustClient):
            async def get_snapshot_json_async(self, *, snapshot_id):
                return (
                    "trace-get",
                    '{"id":"snap-1","namespace":"default","sandbox_id":"sbx-1",'
                    '"base_image":"python:3.12","status":"local_ready"}',
                )

        client = _make_client(_LocalReadyRustClient())

        info = await client.snapshot_and_wait("sbx-1")

        self.assertEqual(info.status, SnapshotStatus.LOCAL_READY)
        self.assertIsNone(info.snapshot_uri)

    async def test_snapshot_and_wait_can_wait_for_completed(self):
        class _SnapshotSequenceRustClient(_FakeAsyncRustClient):
            def __init__(self):
                super().__init__()
                self.get_calls = 0

            async def get_snapshot_json_async(self, *, snapshot_id):
                self.get_calls += 1
                status = "local_ready" if self.get_calls == 1 else "completed"
                return (
                    "trace-get",
                    '{"id":"snap-1","namespace":"default","sandbox_id":"sbx-1",'
                    f'"base_image":"python:3.12","status":"{status}",'
                    '"snapshot_uri":"s3://snap-1.tar.zst"}',
                )

        fake = _SnapshotSequenceRustClient()
        client = _make_client(fake)

        info = await client.snapshot_and_wait(
            "sbx-1",
            poll_interval=0,
            wait_until=SnapshotWaitCondition.COMPLETED,
        )

        self.assertEqual(info.status, SnapshotStatus.COMPLETED)
        self.assertEqual(fake.get_calls, 2)

    async def test_snapshot_omits_snapshot_type_when_none(self):
        fake = _FakeAsyncRustClient()
        client = _make_client(fake)

        await client.snapshot_and_wait("sbx-1")

        self.assertEqual(len(fake.create_snapshot_calls), 1)
        _, snapshot_type = fake.create_snapshot_calls[0]
        self.assertIsNone(snapshot_type)


if __name__ == "__main__":
    unittest.main()
