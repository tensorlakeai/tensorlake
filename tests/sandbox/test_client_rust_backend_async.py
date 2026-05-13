import json
import unittest
from unittest.mock import patch

from tensorlake.sandbox import (
    PoolInUseError,
    SandboxNotFoundError,
    SnapshotStatus,
    SnapshotType,
    SnapshotWaitCondition,
)
from tensorlake.sandbox.async_client import AsyncSandboxClient
from tensorlake.sandbox.exceptions import SandboxError


class _FakeAsyncRustClient:
    def __init__(self):
        self.create_request_json: str = ""
        self.update_request_json: str = ""
        self.last_get_sandbox_id: str = ""
        self.create_snapshot_calls: list[tuple[str, str | None]] = []
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
        return None

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
      },
      "secret_names": []
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
  "secret_names": [],
  "allow_unauthenticated_access": false,
  "exposed_ports": [8080],
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
            "secret_names": [],
            "name": payload.get("name"),
            "allow_unauthenticated_access": payload.get(
                "allow_unauthenticated_access", False
            ),
            "exposed_ports": payload.get("exposed_ports", []),
            "sandbox_url": f"https://{sandbox_id}.sandbox.tensorlake.ai",
        }
        return ("trace-update-sandbox", json.dumps(response))


def _make_client(fake: object | None = None) -> AsyncSandboxClient:
    client = AsyncSandboxClient(
        api_url="http://localhost:8900", api_key="k", _internal=True
    )
    client._rust_client = fake if fake is not None else _FakeAsyncRustClient()
    return client


class _StatusSequenceRustClient(_FakeAsyncRustClient):
    """Returns each status from `statuses` on successive get calls; the last
    status repeats once exhausted."""

    def __init__(self, statuses: list[str]):
        super().__init__()
        self._statuses = statuses
        self.get_calls = 0

    async def get_sandbox_json_async(self, *, sandbox_id) -> tuple[str, str]:
        self.last_get_sandbox_id = sandbox_id
        idx = min(self.get_calls, len(self._statuses) - 1)
        self.get_calls += 1
        return (
            "trace-get-sandbox",
            json.dumps(
                {
                    "id": sandbox_id,
                    "namespace": "default",
                    "status": self._statuses[idx],
                    "resources": {
                        "cpus": 1.0,
                        "memory_mb": 512,
                        "ephemeral_disk_mb": 1024,
                    },
                    "secret_names": [],
                }
            ),
        )


class TestAsyncSandboxClientRustBackend(unittest.IsolatedAsyncioTestCase):
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
  "secret_names": [],
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

    async def test_list_uses_rust_backend(self):
        client = _make_client()

        sandboxes = await client.list()
        sandboxes_list = list(sandboxes)

        self.assertEqual(len(sandboxes_list), 1)
        self.assertEqual(sandboxes_list[0].sandbox_id, "sbx-1")
        self.assertEqual(sandboxes_list[0].status, "running")

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
