"""GPU pool requests use the same allocation and validation as sandbox creates."""

import json
import unittest

from tensorlake.sandbox import (
    AsyncSandboxClient,
    ContainerResourcesInfo,
    GpuAllocation,
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
        self.save_pool(json.loads(request_json))
        return "trace-create", '{"pool_id":"pool-1","namespace":"default"}'

    def update_pool(self, pool_id, request_json):
        request = json.loads(request_json)
        self.save_pool(request)
        return "trace-update", json.dumps(self.pool)

    def save_pool(self, request):
        self.requests.append(request)
        resources = dict(request["resources"])
        resources.setdefault("disk_mb", 10 * 1024)
        resources["gpu_configs"] = resources.pop("gpus", None)
        self.pool = {
            "pool_id": "pool-1",
            "namespace": "default",
            **request,
            "resources": resources,
        }

    def get_pool_json(self, pool_id):
        return "trace-get", json.dumps(self.pool)

    def list_pools_json(self):
        return "trace-list", json.dumps({"pools": [self.pool]})

    async def create_pool_async(self, request_json):
        return self.create_pool(request_json)

    async def update_pool_async(self, pool_id, request_json):
        return self.update_pool(pool_id, request_json)

    async def get_pool_json_async(self, pool_id):
        return self.get_pool_json(pool_id)

    async def list_pools_json_async(self):
        return self.list_pools_json()


VALID_ALLOCATIONS = [
    ({}, None),
    ({"gpus": 1}, [{"count": 1, "model": "A10"}]),
    ({"gpu_model": "H100"}, [{"count": 1, "model": "H100"}]),
    ({"gpu_model": GpuModel.L40}, [{"count": 1, "model": "L40"}]),
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
    {"gpu_model": "V100"},
    {"gpu_model": ""},
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

    def test_pool_responses_retain_gpu_allocation_for_read_modify_write(self):
        self.client.create_pool(
            image="tensorlake-cas/ubuntu-minimal", gpus=2, gpu_model="H100"
        )
        fetched = self.client.get_pool("pool-1")
        listed = list(self.client.list_pools())
        expected = [GpuAllocation(count=2, model="H100")]
        self.assertEqual(fetched.resources.gpu_configs, expected)
        self.assertEqual(listed[0].resources.gpu_configs, expected)
        updated = self.client.update_pool(
            pool_id=fetched.pool_id,
            image=fetched.image,
            gpus=fetched.resources.gpu_configs[0].count,
            gpu_model=fetched.resources.gpu_configs[0].model,
        )
        self.assertEqual(updated.resources.gpu_configs, expected)
        self.assertEqual(
            self.backend.requests[-1]["resources"]["gpus"],
            [{"count": 2, "model": "H100"}],
        )

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

    async def test_pool_responses_retain_gpu_allocation_for_read_modify_write(self):
        await self.client.create_pool(
            image="tensorlake-cas/ubuntu-minimal", gpus=2, gpu_model="H100"
        )
        fetched = await self.client.get_pool("pool-1")
        listed = list(await self.client.list_pools())
        expected = [GpuAllocation(count=2, model="H100")]
        self.assertEqual(fetched.resources.gpu_configs, expected)
        self.assertEqual(listed[0].resources.gpu_configs, expected)
        updated = await self.client.update_pool(
            pool_id=fetched.pool_id,
            image=fetched.image,
            gpus=fetched.resources.gpu_configs[0].count,
            gpu_model=fetched.resources.gpu_configs[0].model,
        )
        self.assertEqual(updated.resources.gpu_configs, expected)
        self.assertEqual(
            self.backend.requests[-1]["resources"]["gpus"],
            [{"count": 2, "model": "H100"}],
        )

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


class TestPoolResponseResources(unittest.TestCase):
    def test_gpu_allocation_survives_deserialization_and_serialization(self):
        for model in ("H100", "H100-PCIe-80GB", "future-exact-gpu-model"):
            allocation = [{"count": 2, "model": model}]
            for field in ("gpu_configs", "gpus"):
                with self.subTest(model=model, field=field):
                    resources = ContainerResourcesInfo.model_validate(
                        {
                            "cpus": 1,
                            "memory_mb": 1024,
                            "disk_mb": 20480,
                            field: allocation,
                        }
                    )
                    self.assertEqual(
                        resources.gpu_configs, [GpuAllocation(count=2, model=model)]
                    )
                    self.assertEqual(
                        json.loads(resources.model_dump_json())["gpu_configs"],
                        allocation,
                    )

    def test_cpu_resources_accept_omitted_null_and_empty_gpu_allocations(self):
        for gpu in ({}, {"gpu_configs": None}, {"gpu_configs": []}):
            with self.subTest(gpu=gpu):
                resources = ContainerResourcesInfo.model_validate(
                    {"cpus": 1, "memory_mb": 1024, "disk_mb": 20480, **gpu}
                )
                self.assertFalse(resources.gpu_configs)
