"""Offline tests for the shared-file-systems feature.

These tests mock the Rust backend (``_rust_client``) and the platform
``CloudApiClient`` so they run without a live server or a built native module.
"""

import json
import os
import unittest
from unittest.mock import patch

from tensorlake.sandbox import (
    SharedFileSystem,
    SharedFileSystemMount,
    create_shared_file_system,
    delete_shared_file_system,
    list_shared_file_systems,
)
from tensorlake.sandbox.async_client import AsyncSandboxClient
from tensorlake.sandbox.client import SandboxClient
from tensorlake.sandbox.exceptions import SandboxError
from tensorlake.sandbox.models import CreateSandboxRequest, CreateSandboxResources


def _sandbox_info_json(file_systems: list[dict]) -> str:
    return json.dumps(
        {
            "id": "sbx-1",
            "namespace": "default",
            "status": "running",
            "resources": {"cpus": 1.0, "memory_mb": 512, "ephemeral_disk_mb": 1024},
            "file_systems": file_systems,
        }
    )


class _FakeRustClient:
    def __init__(self):
        self.attach_calls: list[tuple[str, str, str]] = []
        self.detach_calls: list[tuple[str, str]] = []
        self.create_request_json: str | None = None

    def close(self):
        return None

    def attach_shared_file_system(self, *, sandbox_id, file_system_id, mount_path):
        self.attach_calls.append((sandbox_id, file_system_id, mount_path))
        return (
            "trace-attach",
            _sandbox_info_json(
                [{"file_system_id": file_system_id, "mount_path": mount_path}]
            ),
        )

    def detach_shared_file_system(self, *, sandbox_id, mount_path):
        self.detach_calls.append((sandbox_id, mount_path))
        return ("trace-detach", _sandbox_info_json([]))

    def create_sandbox(self, request_json):
        self.create_request_json = request_json
        return ("trace-create", '{"sandbox_id":"sbx-1","status":"pending"}')


class _FakeAsyncRustClient:
    def __init__(self):
        self.attach_calls: list[tuple[str, str, str]] = []

    def close(self):
        return None

    async def attach_shared_file_system_async(
        self, *, sandbox_id, file_system_id, mount_path
    ):
        self.attach_calls.append((sandbox_id, file_system_id, mount_path))
        return (
            "trace-attach",
            _sandbox_info_json(
                [{"file_system_id": file_system_id, "mount_path": mount_path}]
            ),
        )


def _sync_client(fake: _FakeRustClient) -> SandboxClient:
    with (
        patch("tensorlake.sandbox.client._RUST_SANDBOX_CLIENT_AVAILABLE", True),
        patch("tensorlake.sandbox.client.RustCloudSandboxClient", return_value=fake),
    ):
        return SandboxClient(
            api_url="http://localhost:8900", api_key="k", _internal=True
        )


def _async_client(fake: _FakeAsyncRustClient) -> AsyncSandboxClient:
    with (
        patch("tensorlake.sandbox.async_client._RUST_SANDBOX_CLIENT_AVAILABLE", True),
        patch(
            "tensorlake.sandbox.async_client.RustCloudSandboxClient",
            return_value=fake,
        ),
    ):
        return AsyncSandboxClient(
            api_url="http://localhost:8900", api_key="k", _internal=True
        )


class _FakeCloudApiClient:
    def __init__(self):
        self.create_args: tuple | None = None
        self.list_args: tuple | None = None
        self.delete_args: tuple | None = None
        self.closed = False

    def close(self):
        self.closed = True

    def create_shared_file_system(self, org, project, name, description):
        self.create_args = (org, project, name, description)
        return json.dumps(
            {
                "id": "file_system_abc",
                "name": name,
                "description": description,
                "region": "us-east-1",
                "status": "ready",
                "createdAt": "2026-06-25T00:00:00Z",
                "updatedAt": "2026-06-25T00:00:00Z",
            }
        )

    def list_shared_file_systems(self, org, project):
        self.list_args = (org, project)
        return json.dumps(
            [
                {
                    "id": "file_system_abc",
                    "name": "skills",
                    "region": "us-east-1",
                    "status": "ready",
                    "createdAt": "2026-06-25T00:00:00Z",
                    "updatedAt": "2026-06-25T00:00:00Z",
                }
            ]
        )

    def delete_shared_file_system(self, org, project, file_system_id):
        self.delete_args = (org, project, file_system_id)


class TestSharedFileSystemModels(unittest.TestCase):
    def test_shared_file_system_parses_camel_case_response(self):
        fs = SharedFileSystem.model_validate_json(
            json.dumps(
                {
                    "id": "file_system_abc",
                    "name": "skills",
                    "region": "us-east-1",
                    "status": "ready",
                    "createdAt": "2026-06-25T00:00:00Z",
                    "updatedAt": "2026-06-25T01:00:00Z",
                }
            )
        )
        self.assertEqual(fs.id, "file_system_abc")
        self.assertEqual(fs.name, "skills")
        self.assertEqual(fs.region, "us-east-1")
        self.assertEqual(fs.created_at, "2026-06-25T00:00:00Z")
        self.assertEqual(fs.updated_at, "2026-06-25T01:00:00Z")

    def test_shared_file_system_mount_round_trips_snake_case(self):
        mount = SharedFileSystemMount(
            file_system_id="file_system_abc", mount_path="/mnt/skills"
        )
        self.assertEqual(
            json.loads(mount.model_dump_json()),
            {"file_system_id": "file_system_abc", "mount_path": "/mnt/skills"},
        )

    def test_create_request_serializes_shared_file_systems_to_wire_key(self):
        request = CreateSandboxRequest(
            resources=CreateSandboxResources(cpus=1.0, memory_mb=1024),
            shared_file_systems=[
                SharedFileSystemMount(
                    file_system_id="file_system_abc", mount_path="/mnt/skills"
                )
            ],
        )
        payload = json.loads(request.model_dump_json(by_alias=True, exclude_none=True))
        self.assertEqual(
            payload["file_systems"],
            [{"file_system_id": "file_system_abc", "mount_path": "/mnt/skills"}],
        )
        self.assertNotIn("shared_file_systems", payload)

    def test_create_request_omits_shared_file_systems_when_absent(self):
        request = CreateSandboxRequest(
            resources=CreateSandboxResources(cpus=1.0, memory_mb=1024),
        )
        payload = json.loads(request.model_dump_json(by_alias=True, exclude_none=True))
        self.assertNotIn("file_systems", payload)
        self.assertNotIn("shared_file_systems", payload)


class TestSandboxClientSharedFileSystems(unittest.TestCase):
    def test_attach_shared_file_system(self):
        fake = _FakeRustClient()
        client = _sync_client(fake)

        traced = client.attach_shared_file_system(
            "sbx-1", "file_system_abc", "/mnt/skills"
        )

        self.assertEqual(
            fake.attach_calls, [("sbx-1", "file_system_abc", "/mnt/skills")]
        )
        self.assertEqual(traced.trace_id, "trace-attach")
        self.assertEqual(
            traced.value.shared_file_systems,
            [
                SharedFileSystemMount(
                    file_system_id="file_system_abc", mount_path="/mnt/skills"
                )
            ],
        )

    def test_detach_shared_file_system(self):
        fake = _FakeRustClient()
        client = _sync_client(fake)

        traced = client.detach_shared_file_system("sbx-1", "/mnt/skills")

        self.assertEqual(fake.detach_calls, [("sbx-1", "/mnt/skills")])
        self.assertEqual(traced.trace_id, "trace-detach")
        self.assertEqual(traced.value.shared_file_systems, [])

    def test_create_threads_shared_file_systems(self):
        fake = _FakeRustClient()
        client = _sync_client(fake)

        client.create(
            image="python:3.11",
            shared_file_systems=[
                SharedFileSystemMount(
                    file_system_id="file_system_abc", mount_path="/mnt/skills"
                )
            ],
        )

        payload = json.loads(fake.create_request_json)
        self.assertEqual(
            payload["file_systems"],
            [{"file_system_id": "file_system_abc", "mount_path": "/mnt/skills"}],
        )


class TestAsyncSandboxClientSharedFileSystems(unittest.IsolatedAsyncioTestCase):
    async def test_attach_shared_file_system(self):
        fake = _FakeAsyncRustClient()
        client = _async_client(fake)

        traced = await client.attach_shared_file_system(
            "sbx-1", "file_system_abc", "/mnt/skills"
        )

        self.assertEqual(
            fake.attach_calls, [("sbx-1", "file_system_abc", "/mnt/skills")]
        )
        self.assertEqual(traced.trace_id, "trace-attach")
        self.assertEqual(
            traced.value.shared_file_systems,
            [
                SharedFileSystemMount(
                    file_system_id="file_system_abc", mount_path="/mnt/skills"
                )
            ],
        )


class TestSharedFileSystemRegistry(unittest.TestCase):
    def setUp(self):
        self._env = patch.dict(
            os.environ,
            {
                "TENSORLAKE_API_KEY": "k",
                "TENSORLAKE_ORGANIZATION_ID": "org-1",
                "TENSORLAKE_PROJECT_ID": "proj-1",
            },
            clear=False,
        )
        self._env.start()

    def tearDown(self):
        self._env.stop()

    def test_create_shared_file_system(self):
        fake = _FakeCloudApiClient()
        with patch(
            "tensorlake.sandbox.shared_file_system._cloud_api_client",
            return_value=fake,
        ):
            fs = create_shared_file_system(
                "skills", description="shared skills volume"
            )

        self.assertEqual(
            fake.create_args, ("org-1", "proj-1", "skills", "shared skills volume")
        )
        self.assertEqual(fs.id, "file_system_abc")
        self.assertEqual(fs.name, "skills")
        self.assertTrue(fake.closed)

    def test_list_shared_file_systems(self):
        fake = _FakeCloudApiClient()
        with patch(
            "tensorlake.sandbox.shared_file_system._cloud_api_client",
            return_value=fake,
        ):
            result = list_shared_file_systems()

        self.assertEqual(fake.list_args, ("org-1", "proj-1"))
        self.assertEqual([f.id for f in result], ["file_system_abc"])
        self.assertTrue(fake.closed)

    def test_delete_shared_file_system(self):
        fake = _FakeCloudApiClient()
        with patch(
            "tensorlake.sandbox.shared_file_system._cloud_api_client",
            return_value=fake,
        ):
            delete_shared_file_system("file_system_abc")

        self.assertEqual(fake.delete_args, ("org-1", "proj-1", "file_system_abc"))
        self.assertTrue(fake.closed)

    def test_missing_project_context_raises(self):
        with patch.dict(os.environ, {"TENSORLAKE_ORGANIZATION_ID": ""}, clear=False):
            with self.assertRaises(SandboxError):
                list_shared_file_systems()

    def test_create_requires_non_empty_name(self):
        with self.assertRaises(TypeError):
            create_shared_file_system("")


if __name__ == "__main__":
    unittest.main()
