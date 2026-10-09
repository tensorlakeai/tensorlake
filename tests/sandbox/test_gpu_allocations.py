"""GPU response identifiers and request inference across public SDK paths."""

import json
import unittest
from unittest.mock import patch

from pydantic import ValidationError

from tensorlake.sandbox import (
    AsyncSandbox,
    AsyncSandboxClient,
    GpuAllocation,
    GpuModel,
    GpuRequest,
    GPUResources,
    Sandbox,
    SandboxClient,
    SandboxError,
)

from .test_client_rust_backend import _FakeRustClient
from .test_client_rust_backend_async import _FakeAsyncRustClient
from .test_pool_gpu import INVALID_ALLOCATIONS, VALID_ALLOCATIONS


class ResponseBackend:
    def __init__(self, field, model):
        self.resources = {
            "cpus": 1,
            "memory_mb": 1024,
            "disk_mb": 20480,
            field: [{"count": 2, "model": model}],
        }

    def close(self):
        pass

    def get_sandbox_json(self, sandbox_id):
        return "trace-get", json.dumps(
            {
                "id": sandbox_id,
                "namespace": "default",
                "status": "running",
                "resources": self.resources,
            }
        )

    def list_sandboxes_json(self):
        _, sandbox = self.get_sandbox_json("sandbox-1")
        return "trace-list", json.dumps({"sandboxes": [json.loads(sandbox)]})

    def get_pool_json(self, pool_id):
        return "trace-get", json.dumps(
            {
                "pool_id": pool_id,
                "namespace": "default",
                "image": "tensorlake-cas/ubuntu-minimal",
                "resources": self.resources,
            }
        )

    def list_pools_json(self):
        _, pool = self.get_pool_json("pool-1")
        return "trace-list", json.dumps({"pools": [json.loads(pool)]})

    def update_pool(self, pool_id, request_json):
        return self.get_pool_json(pool_id)

    async def get_sandbox_json_async(self, sandbox_id):
        return self.get_sandbox_json(sandbox_id)

    async def list_sandboxes_json_async(self):
        return self.list_sandboxes_json()

    async def get_pool_json_async(self, pool_id):
        return self.get_pool_json(pool_id)

    async def list_pools_json_async(self):
        return self.list_pools_json()

    async def update_pool_async(self, pool_id, request_json):
        return self.update_pool(pool_id, request_json)


class TestGpuRequestModels(unittest.TestCase):
    def test_requests_keep_the_closed_model_enum_and_positive_count(self):
        self.assertIs(GPUResources, GpuRequest)
        self.assertIs(GpuRequest(count=1, model="H100").model, GpuModel.H100)
        for model in ("H100-PCIe-80GB", "future-exact-gpu-model"):
            with self.subTest(model=model), self.assertRaises(ValidationError):
                GpuRequest(count=1, model=model)
        with self.assertRaises(ValidationError):
            GpuRequest(count=0, model=GpuModel.H100)


class TestGpuAllocations(unittest.TestCase):
    def test_sandbox_create_infers_one_gpu_from_a_model(self):
        backend = _FakeRustClient()
        with patch(
            "tensorlake.sandbox.client.RustCloudSandboxClient", return_value=backend
        ):
            sandbox = Sandbox.create(api_key="test-key", gpu_model="H100")
        self.addCleanup(sandbox.close)
        resources = json.loads(backend.create_request_json)["resources"]
        self.assertEqual(resources["gpus"], [{"count": 1, "model": "H100"}])

    def test_get_list_and_pool_update_preserve_exact_identifiers(self):
        for field in ("gpus", "gpu_configs"):
            for model in ("H100-PCIe-80GB", "future-exact-gpu-model"):
                with self.subTest(field=field, model=model):
                    client = SandboxClient.for_localhost()
                    client._rust_client = ResponseBackend(field, model)
                    self.addCleanup(client.close)
                    responses = [
                        client.get("sandbox-1"),
                        *client.list(),
                        client.get_pool("pool-1"),
                        *client.list_pools(),
                        client.update_pool(
                            "pool-1",
                            image="tensorlake-cas/ubuntu-minimal",
                            gpus=2,
                            gpu_model="H100",
                        ),
                    ]
                    for response in responses:
                        self.assertEqual(
                            response.resources.gpu_configs,
                            [GpuAllocation(count=2, model=model)],
                        )
                        self.assertEqual(
                            response.resources.model_dump()["gpu_configs"],
                            [{"count": 2, "model": model}],
                        )

    def test_create_and_connect_share_gpu_allocation_inference(self):
        for method, mode in (
            ("create", {}),
            ("create", {"wait": False}),
            ("create_and_connect", {}),
        ):
            for allocation, expected in VALID_ALLOCATIONS:
                with self.subTest(method=method, mode=mode, allocation=allocation):
                    client = SandboxClient.for_localhost()
                    backend = _FakeRustClient()
                    client._rust_client = backend
                    self.addCleanup(client.close)
                    getattr(client, method)(**allocation, **mode)
                    resources = json.loads(backend.create_request_json)["resources"]
                    self.assertEqual(resources.get("gpus"), expected)

    def test_invalid_allocations_are_rejected_before_create_or_connect(self):
        for method, mode in (
            ("create", {}),
            ("create", {"wait": False}),
            ("create_and_connect", {}),
        ):
            for allocation in INVALID_ALLOCATIONS:
                with self.subTest(method=method, mode=mode, allocation=allocation):
                    client = SandboxClient.for_localhost()
                    backend = _FakeRustClient()
                    client._rust_client = backend
                    self.addCleanup(client.close)
                    with self.assertRaises(SandboxError):
                        getattr(client, method)(**allocation, **mode)
                    self.assertIsNone(backend.create_request_json)


class TestAsyncGpuAllocations(unittest.IsolatedAsyncioTestCase):
    async def test_sandbox_create_infers_one_gpu_from_a_model(self):
        backend = _FakeAsyncRustClient()
        with patch(
            "tensorlake.sandbox.async_client.RustCloudSandboxClient",
            return_value=backend,
        ):
            sandbox = await AsyncSandbox.create(api_key="test-key", gpu_model="H100")
        self.addCleanup(sandbox.close)
        resources = json.loads(backend.create_request_json)["resources"]
        self.assertEqual(resources["gpus"], [{"count": 1, "model": "H100"}])

    async def test_get_list_and_pool_update_preserve_exact_identifiers(self):
        for field in ("gpus", "gpu_configs"):
            for model in ("H100-PCIe-80GB", "future-exact-gpu-model"):
                with self.subTest(field=field, model=model):
                    client = AsyncSandboxClient.for_localhost()
                    client._rust_client = ResponseBackend(field, model)
                    self.addCleanup(client.close)
                    responses = [
                        await client.get("sandbox-1"),
                        *(await client.list()),
                        await client.get_pool("pool-1"),
                        *(await client.list_pools()),
                        await client.update_pool(
                            "pool-1",
                            image="tensorlake-cas/ubuntu-minimal",
                            gpus=2,
                            gpu_model="H100",
                        ),
                    ]
                    for response in responses:
                        self.assertEqual(
                            response.resources.gpu_configs,
                            [GpuAllocation(count=2, model=model)],
                        )

    async def test_create_and_connect_share_gpu_allocation_inference(self):
        for method, mode in (
            ("create", {}),
            ("create", {"wait": False}),
            ("create_and_connect", {}),
        ):
            for allocation, expected in VALID_ALLOCATIONS:
                with self.subTest(method=method, mode=mode, allocation=allocation):
                    client = AsyncSandboxClient.for_localhost()
                    backend = _FakeAsyncRustClient()
                    client._rust_client = backend
                    self.addCleanup(client.close)
                    await getattr(client, method)(**allocation, **mode)
                    resources = json.loads(backend.create_request_json)["resources"]
                    self.assertEqual(resources.get("gpus"), expected)

    async def test_invalid_allocations_are_rejected_before_create_or_connect(self):
        for method, mode in (
            ("create", {}),
            ("create", {"wait": False}),
            ("create_and_connect", {}),
        ):
            for allocation in INVALID_ALLOCATIONS:
                with self.subTest(method=method, mode=mode, allocation=allocation):
                    client = AsyncSandboxClient.for_localhost()
                    backend = _FakeAsyncRustClient()
                    client._rust_client = backend
                    self.addCleanup(client.close)
                    with self.assertRaises(SandboxError):
                        await getattr(client, method)(**allocation, **mode)
                    self.assertFalse(backend.create_request_json)
