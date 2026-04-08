import json
import unittest
from unittest.mock import patch

from tensorlake.sandbox import (
    PoolInUseError,
    SandboxNotFoundError,
    SnapshotContentMode,
)
from tensorlake.sandbox.client import SandboxClient
from tensorlake.sandbox.exceptions import SandboxError


class _FakeRustClient:
    def __init__(self):
        self.create_request_json = None
        self.update_request_json = None
        self.last_get_sandbox_id = None
        self.create_snapshot_calls: list[tuple[str, str | None]] = []

    def close(self):
        return None

    def create_snapshot(self, sandbox_id, content_mode=None):
        self.create_snapshot_calls.append((sandbox_id, content_mode))
        return '{"snapshot_id":"snap-1","status":"in_progress"}'

    def get_snapshot_json(self, snapshot_id):
        return (
            '{"id":"snap-1","namespace":"default","sandbox_id":"sbx-1",'
            '"base_image":"python:3.12","status":"completed",'
            '"snapshot_uri":"s3://snap-1.tar.zst"}'
        )

    def create_sandbox(self, request_json):
        self.create_request_json = request_json
        return '{"sandbox_id":"sbx-1","status":"pending"}'

    def list_sandboxes_json(self):
        return """
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
"""

    def get_sandbox_json(self, sandbox_id):
        self.last_get_sandbox_id = sandbox_id
        return """
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
"""

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
        return json.dumps(response)


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

    def test_list_uses_rust_backend(self):
        client = SandboxClient(api_url="http://localhost:8900", api_key="k")
        client._rust_client = _FakeRustClient()

        sandboxes = client.list()

        self.assertEqual(len(sandboxes), 1)
        self.assertEqual(sandboxes[0].sandbox_id, "sbx-1")
        self.assertEqual(sandboxes[0].status, "running")

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

    def test_snapshot_threads_content_mode_to_rust_backend(self):
        client = SandboxClient(api_url="http://localhost:8900", api_key="k")
        fake = _FakeRustClient()
        client._rust_client = fake

        info = client.snapshot_and_wait(
            "sbx-1",
            content_mode=SnapshotContentMode.FILESYSTEM_ONLY,
        )

        self.assertEqual(len(fake.create_snapshot_calls), 1)
        sandbox_id, content_mode = fake.create_snapshot_calls[0]
        self.assertEqual(sandbox_id, "sbx-1")
        self.assertEqual(content_mode, "filesystem_only")
        self.assertEqual(info.snapshot_id, "snap-1")

    def test_snapshot_omits_content_mode_when_none(self):
        client = SandboxClient(api_url="http://localhost:8900", api_key="k")
        fake = _FakeRustClient()
        client._rust_client = fake

        client.snapshot_and_wait("sbx-1")

        self.assertEqual(len(fake.create_snapshot_calls), 1)
        _, content_mode = fake.create_snapshot_calls[0]
        self.assertIsNone(content_mode)


if __name__ == "__main__":
    unittest.main()
