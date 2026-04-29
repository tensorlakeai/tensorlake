import json
import unittest
from unittest.mock import patch

from tensorlake.sandbox import (
    PoolInUseError,
    SandboxNotFoundError,
    SnapshotType,
)
from tensorlake.sandbox.client import SandboxClient
from tensorlake.sandbox.exceptions import SandboxError


class _FakeRustClient:
    def __init__(self):
        self.create_request_json = None
        self.update_request_json = None
        self.last_get_sandbox_id = None
        self.create_snapshot_calls: list[tuple[str, str | None]] = []
        self.suspend_calls: list[str] = []
        self.resume_calls: list[str] = []

    def close(self):
        return None

    def suspend_sandbox(self, sandbox_id):
        self.suspend_calls.append(sandbox_id)

    def resume_sandbox(self, sandbox_id):
        self.resume_calls.append(sandbox_id)

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
      },
      "secret_names": []
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
  "secret_names": [],
  "allow_unauthenticated_access": false,
  "exposed_ports": [8080],
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
            "secret_names": [],
            "name": payload.get("name"),
            "allow_unauthenticated_access": payload.get(
                "allow_unauthenticated_access", False
            ),
            "exposed_ports": payload.get("exposed_ports", []),
            "sandbox_url": f"https://{sandbox_id}.sandbox.tensorlake.ai",
        }
        return ("trace-update-sandbox", json.dumps(response))


class TestSandboxClientRustBackend(unittest.TestCase):
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
  "secret_names": [],
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

            def delete_pool(self, pool_id, force=False):
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

    def test_snapshot_omits_snapshot_type_when_none(self):
        client = SandboxClient(api_url="http://localhost:8900", api_key="k")
        fake = _FakeRustClient()
        client._rust_client = fake

        client.snapshot_and_wait("sbx-1")

        self.assertEqual(len(fake.create_snapshot_calls), 1)
        _, snapshot_type = fake.create_snapshot_calls[0]
        self.assertIsNone(snapshot_type)

    def test_create_pool_sends_new_proxy_and_network_fields(self):
        from tensorlake.sandbox import NetworkConfig

        captured: dict = {}

        class _PoolFake:
            def close(self):
                return None

            def create_pool(self, request_json):
                captured["create"] = request_json
                return ("trace-create-pool", '{"pool_id":"pool-1","namespace":"default"}')

        client = SandboxClient(api_url="http://localhost:8900", api_key="k")
        client._rust_client = _PoolFake()

        client.create_pool(
            image="alpine",
            cpus=0.5,
            memory_mb=512,
            ephemeral_disk_mb=0,
            allow_unauthenticated_access=True,
            exposed_ports=[8080],
            network=NetworkConfig(
                allow_internet_access=False,
                allow_out=["10.0.0.0/8"],
                deny_out=[],
            ),
        )

        body = json.loads(captured["create"])
        self.assertEqual(body["allow_unauthenticated_access"], True)
        self.assertEqual(body["exposed_ports"], [8080])
        self.assertEqual(body["network"]["allow_internet_access"], False)
        self.assertEqual(body["network"]["allow_out"], ["10.0.0.0/8"])

    def test_update_pool_patch_merges_with_current_state(self):
        captured: dict = {}

        class _PoolFake:
            def close(self):
                return None

            def get_pool_json(self, pool_id):
                captured["get"] = pool_id
                response = {
                    "id": pool_id,
                    "namespace": "default",
                    "image": "alpine",
                    "resources": {
                        "cpus": 0.5,
                        "memory_mb": 256,
                        "ephemeral_disk_mb": 1024,
                    },
                    "secret_names": [],
                    "timeout_secs": 60,
                    "entrypoint": ["bash"],
                    "max_containers": 10,
                    "warm_containers": 3,
                    "allow_unauthenticated_access": True,
                    "exposed_ports": [8080],
                    "network_policy": {
                        "allow_internet_access": False,
                        "allow_out": ["10.0.0.0/8"],
                        "deny_out": [],
                    },
                }
                return ("trace-get-pool", json.dumps(response))

            def update_pool(self, pool_id, request_json):
                captured["update"] = request_json
                response = {
                    "id": pool_id,
                    "namespace": "default",
                    "image": "alpine",
                    "resources": {"cpus": 0.5, "memory_mb": 256, "ephemeral_disk_mb": 1024},
                    "warm_containers": 5,
                }
                return ("trace-update-pool", json.dumps(response))

        client = SandboxClient(api_url="http://localhost:8900", api_key="k")
        client._rust_client = _PoolFake()

        client.update_pool(pool_id="pool-1", warm_containers=5)

        body = json.loads(captured["update"])
        self.assertEqual(body["image"], "alpine")
        self.assertEqual(body["resources"]["cpus"], 0.5)
        self.assertEqual(body["resources"]["memory_mb"], 256)
        self.assertEqual(body["timeout_secs"], 60)
        self.assertEqual(body["entrypoint"], ["bash"])
        self.assertEqual(body["max_containers"], 10)
        self.assertEqual(body["warm_containers"], 5)
        self.assertEqual(body["allow_unauthenticated_access"], True)
        self.assertEqual(body["exposed_ports"], [8080])
        self.assertEqual(body["network"]["allow_out"], ["10.0.0.0/8"])
        self.assertNotIn("network_policy", body)

    def test_delete_pool_threads_force_flag(self):
        captured: dict = {}

        class _PoolFake:
            def close(self):
                return None

            def delete_pool(self, pool_id, force=False):
                captured["pool_id"] = pool_id
                captured["force"] = force
                return "trace-delete-pool"

        client = SandboxClient(api_url="http://localhost:8900", api_key="k")
        client._rust_client = _PoolFake()

        client.delete_pool("pool-1", force=True)

        self.assertEqual(captured["pool_id"], "pool-1")
        self.assertEqual(captured["force"], True)


if __name__ == "__main__":
    unittest.main()
