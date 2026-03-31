import json
import unittest

from tensorlake.sandbox import PoolInUseError, SandboxNotFoundError
from tensorlake.sandbox.client import SandboxClient


class _FakeRustClient:
    def __init__(self):
        self.create_request_json = None

    def close(self):
        return None

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


class TestSandboxClientRustBackend(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
