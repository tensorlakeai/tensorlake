"""GPU pool requests use the same allocation and validation as sandbox creates."""

import json
import unittest

from tensorlake.sandbox import (
    AsyncSandboxClient,
    GpuModel,
    GpuRequest,
    SandboxClient,
    SandboxError,
)


class PoolBackend:
    def __init__(self):
        self.requests = []

    def close(self):
        pass

    def create_pool(self, request_json):
        self.requests.append(json.loads(request_json))
        return "trace-create", '{"pool_id":"pool-1","namespace":"default"}'

    def update_pool(self, pool_id, request_json):
        request = json.loads(request_json)
        self.requests.append(request)
        return "trace-update", json.dumps(
            {"pool_id": pool_id, "namespace": "default", **request}
        )

    async def create_pool_async(self, request_json):
        return self.create_pool(request_json)

    async def update_pool_async(self, pool_id, request_json):
        return self.update_pool(pool_id, request_json)


VALID_ALLOCATIONS = [
    ({}, None),
    ({"gpus": 1}, [{"count": 1, "model": "A10"}]),
    *[
        ({"gpus": 2, "gpu_model": model}, [{"count": 2, "model": model.value}])
        for model in GpuModel
    ],
    (
        {"gpu": GpuRequest(count=1, model=GpuModel.H100)},
        [{"count": 1, "model": "H100"}],
    ),
]
INVALID_ALLOCATIONS = [
    {"gpus": 0},
    {"gpus": -1},
    {"gpus": True},
    {"gpus": 1.5},
    {"gpus": 1, "gpu_model": "V100"},
    {"gpu": GpuRequest(count=1, model=GpuModel.H100), "gpus": 1},
    {"gpu": GpuRequest(count=1, model=GpuModel.H100), "gpu_model": "A10"},
]


def assert_request(test, request, expected):
    test.assertEqual(request["resources"]["disk_mb"], 20 * 1024)
    test.assertEqual(request["warm_containers"], 1)
    if expected is None:
        test.assertNotIn("gpus", request["resources"])
    else:
        test.assertEqual(request["resources"]["gpus"], expected)


class TestGpuPools(unittest.TestCase):
    def setUp(self):
        self.backend = PoolBackend()
        self.client = SandboxClient(api_url="http://localhost:8900", _internal=True)
        self.client._rust_client = self.backend
        self.addCleanup(self.client.close)

    def test_create_and_update_send_gpu_allocations(self):
        for operation in (self.client.create_pool, self.client.update_pool):
            pool = {"pool_id": "pool-1"} if operation == self.client.update_pool else {}
            for allocation, expected in VALID_ALLOCATIONS:
                with self.subTest(operation=operation.__name__, allocation=allocation):
                    operation(
                        image="tensorlake-cas/ubuntu-minimal",
                        disk_mb=20 * 1024,
                        warm_containers=1,
                        **pool,
                        **allocation,
                    )
                    assert_request(self, self.backend.requests[-1], expected)

    def test_invalid_allocations_fail_before_native_request(self):
        for operation in (self.client.create_pool, self.client.update_pool):
            pool = {"pool_id": "pool-1"} if operation == self.client.update_pool else {}
            for allocation in INVALID_ALLOCATIONS:
                with self.subTest(operation=operation.__name__, allocation=allocation):
                    with self.assertRaises(SandboxError):
                        operation(
                            image="tensorlake-cas/ubuntu-minimal", **pool, **allocation
                        )
        self.assertEqual(self.backend.requests, [])


class TestAsyncGpuPools(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.backend = PoolBackend()
        self.client = AsyncSandboxClient(
            api_url="http://localhost:8900", _internal=True
        )
        self.client._rust_client = self.backend
        self.addCleanup(self.client.close)

    async def test_create_and_update_send_gpu_allocations(self):
        for operation in (self.client.create_pool, self.client.update_pool):
            pool = {"pool_id": "pool-1"} if operation == self.client.update_pool else {}
            for allocation, expected in VALID_ALLOCATIONS:
                with self.subTest(operation=operation.__name__, allocation=allocation):
                    await operation(
                        image="tensorlake-cas/ubuntu-minimal",
                        disk_mb=20 * 1024,
                        warm_containers=1,
                        **pool,
                        **allocation,
                    )
                    assert_request(self, self.backend.requests[-1], expected)

    async def test_invalid_allocations_fail_before_native_request(self):
        for operation in (self.client.create_pool, self.client.update_pool):
            pool = {"pool_id": "pool-1"} if operation == self.client.update_pool else {}
            for allocation in INVALID_ALLOCATIONS:
                with self.subTest(operation=operation.__name__, allocation=allocation):
                    with self.assertRaises(SandboxError):
                        await operation(
                            image="tensorlake-cas/ubuntu-minimal", **pool, **allocation
                        )
        self.assertEqual(self.backend.requests, [])
