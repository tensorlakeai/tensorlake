"""Offline tests for the file-systems feature.

These tests mock the Rust backend (``_rust_client``) and the platform
``CloudApiClient`` so they run without a live server or a built native module.
"""

import json
import os
import unittest
from unittest.mock import patch

from tensorlake.sandbox import (
    FileSystem,
    FileSystemMount,
    create_file_system,
    delete_file_system,
    list_file_systems,
)
from tensorlake.sandbox.async_client import AsyncSandboxClient
from tensorlake.sandbox.client import SandboxClient
from tensorlake.sandbox.exceptions import SandboxError
from tensorlake.sandbox.models import (
    ClaimSandboxRequest,
    CreateSandboxRequest,
    CreateSandboxResources,
)


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
        self.claim_calls: list[dict[str, str]] = []

    def close(self):
        return None

    def attach_file_system(
        self,
        *,
        sandbox_id,
        file_system_id,
        mount_path,
        read_only,
        prefetch,
        snapshot_id,
    ):
        self.attach_calls.append(
            (sandbox_id, file_system_id, mount_path, read_only, prefetch, snapshot_id)
        )
        mount = {"file_system_id": file_system_id, "mount_path": mount_path}
        if read_only:
            mount["read_only"] = True
        if prefetch:
            mount["prefetch"] = True
        if snapshot_id is not None:
            mount["snapshot_id"] = snapshot_id
        return ("trace-attach", _sandbox_info_json([mount]))

    def detach_file_system(self, *, sandbox_id, mount_path):
        self.detach_calls.append((sandbox_id, mount_path))
        return ("trace-detach", _sandbox_info_json([]))

    def create_sandbox(self, request_json):
        self.create_request_json = request_json
        return ("trace-create", '{"sandbox_id":"sbx-1","status":"pending"}')

    def claim_sandbox(self, **kwargs):
        self.claim_calls.append(kwargs)
        return ("trace-claim", '{"sandbox_id":"sbx-1","status":"pending"}')


class _FakeAsyncRustClient:
    def __init__(self):
        self.attach_calls: list[tuple[str, str, str]] = []
        self.claim_calls: list[dict[str, str]] = []

    def close(self):
        return None

    async def attach_file_system_async(
        self,
        *,
        sandbox_id,
        file_system_id,
        mount_path,
        read_only,
        prefetch,
        snapshot_id,
    ):
        self.attach_calls.append(
            (sandbox_id, file_system_id, mount_path, read_only, prefetch, snapshot_id)
        )
        mount = {"file_system_id": file_system_id, "mount_path": mount_path}
        if read_only:
            mount["read_only"] = True
        if prefetch:
            mount["prefetch"] = True
        if snapshot_id is not None:
            mount["snapshot_id"] = snapshot_id
        return ("trace-attach", _sandbox_info_json([mount]))

    async def claim_sandbox_async(self, **kwargs):
        self.claim_calls.append(kwargs)
        return ("trace-claim", '{"sandbox_id":"sbx-1","status":"pending"}')


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

    def create_file_system(self, org, project, name, description):
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

    def list_file_systems(self, org, project):
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

    def delete_file_system(self, org, project, file_system_id):
        self.delete_args = (org, project, file_system_id)


class TestFileSystemModels(unittest.TestCase):
    def test_file_system_parses_camel_case_response(self):
        fs = FileSystem.model_validate_json(
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

    def test_file_system_mount_round_trips_snake_case(self):
        mount = FileSystemMount(
            file_system_id="file_system_abc", mount_path="/mnt/skills"
        )
        self.assertEqual(
            json.loads(mount.model_dump_json()),
            {"file_system_id": "file_system_abc", "mount_path": "/mnt/skills"},
        )

    def test_file_system_mount_omits_false_mount_modes(self):
        mount = FileSystemMount(
            file_system_id="file_system_abc",
            mount_path="/mnt/skills",
            read_only=False,
            prefetch=False,
        )
        self.assertEqual(
            json.loads(mount.model_dump_json()),
            {"file_system_id": "file_system_abc", "mount_path": "/mnt/skills"},
        )

    def test_file_system_mount_serializes_true_mount_modes(self):
        mount = FileSystemMount(
            file_system_id="file_system_abc",
            mount_path="/mnt/skills",
            read_only=True,
            prefetch=True,
        )
        self.assertEqual(
            json.loads(mount.model_dump_json()),
            {
                "file_system_id": "file_system_abc",
                "mount_path": "/mnt/skills",
                "read_only": True,
                "prefetch": True,
            },
        )

    def test_file_system_mount_parses_absent_and_present_mount_modes(self):
        absent = FileSystemMount.model_validate(
            {"file_system_id": "file_system_abc", "mount_path": "/mnt/skills"}
        )
        self.assertFalse(absent.read_only)
        self.assertFalse(absent.prefetch)

        present = FileSystemMount.model_validate(
            {
                "file_system_id": "file_system_abc",
                "mount_path": "/mnt/skills",
                "read_only": True,
                "prefetch": True,
            }
        )
        self.assertTrue(present.read_only)
        self.assertTrue(present.prefetch)

    def test_file_system_mount_serializes_snapshot_pin_only_when_set(self):
        unpinned = FileSystemMount(
            file_system_id="file_system_abc",
            mount_path="/mnt/skills",
            read_only=True,
        )
        self.assertNotIn("snapshot_id", json.loads(unpinned.model_dump_json()))

        pinned = FileSystemMount(
            file_system_id="file_system_abc",
            mount_path="/mnt/skills",
            read_only=True,
            snapshot_id="0abc123def",
        )
        self.assertEqual(
            json.loads(pinned.model_dump_json()),
            {
                "file_system_id": "file_system_abc",
                "mount_path": "/mnt/skills",
                "read_only": True,
                "snapshot_id": "0abc123def",
            },
        )

    def test_file_system_mount_parses_absent_and_present_snapshot_pin(self):
        absent = FileSystemMount.model_validate(
            {"file_system_id": "file_system_abc", "mount_path": "/mnt/skills"}
        )
        self.assertIsNone(absent.snapshot_id)

        present = FileSystemMount.model_validate(
            {
                "file_system_id": "file_system_abc",
                "mount_path": "/mnt/skills",
                "read_only": True,
                "snapshot_id": "0abc123def",
            }
        )
        self.assertEqual(present.snapshot_id, "0abc123def")

    def test_create_request_serializes_file_systems_to_wire_key(self):
        request = CreateSandboxRequest(
            resources=CreateSandboxResources(cpus=1.0, memory_mb=1024),
            file_systems=[
                FileSystemMount(
                    file_system_id="file_system_abc", mount_path="/mnt/skills"
                )
            ],
        )
        payload = json.loads(request.model_dump_json(by_alias=True, exclude_none=True))
        self.assertEqual(
            payload["file_systems"],
            [{"file_system_id": "file_system_abc", "mount_path": "/mnt/skills"}],
        )

    def test_create_request_omits_file_systems_when_absent(self):
        request = CreateSandboxRequest(
            resources=CreateSandboxResources(cpus=1.0, memory_mb=1024),
        )
        payload = json.loads(request.model_dump_json(by_alias=True, exclude_none=True))
        self.assertNotIn("file_systems", payload)

    def test_create_request_omits_false_mount_modes_and_keeps_true(self):
        request = CreateSandboxRequest(
            resources=CreateSandboxResources(cpus=1.0, memory_mb=1024),
            file_systems=[
                FileSystemMount(
                    file_system_id="file_system_abc",
                    mount_path="/mnt/skills",
                    read_only=True,
                ),
                FileSystemMount(
                    file_system_id="file_system_def",
                    mount_path="/mnt/data",
                    prefetch=True,
                ),
                FileSystemMount(
                    file_system_id="file_system_ghi", mount_path="/mnt/plain"
                ),
            ],
        )
        payload = json.loads(request.model_dump_json(by_alias=True, exclude_none=True))
        self.assertEqual(
            payload["file_systems"],
            [
                {
                    "file_system_id": "file_system_abc",
                    "mount_path": "/mnt/skills",
                    "read_only": True,
                },
                {
                    "file_system_id": "file_system_def",
                    "mount_path": "/mnt/data",
                    "prefetch": True,
                },
                {"file_system_id": "file_system_ghi", "mount_path": "/mnt/plain"},
            ],
        )

    def test_claim_request_serializes_file_systems_to_wire_key(self):
        request = ClaimSandboxRequest(
            file_systems=[
                FileSystemMount(
                    file_system_id="file_system_abc", mount_path="/mnt/skills"
                )
            ]
        )
        payload = json.loads(request.model_dump_json(by_alias=True, exclude_none=True))
        self.assertEqual(
            payload,
            {
                "file_systems": [
                    {
                        "file_system_id": "file_system_abc",
                        "mount_path": "/mnt/skills",
                    }
                ]
            },
        )


class TestSandboxClientFileSystems(unittest.TestCase):
    def test_attach_file_system(self):
        fake = _FakeRustClient()
        client = _sync_client(fake)

        traced = client.attach_file_system("sbx-1", "file_system_abc", "/mnt/skills")

        self.assertEqual(
            fake.attach_calls,
            [("sbx-1", "file_system_abc", "/mnt/skills", False, False, None)],
        )
        self.assertEqual(traced.trace_id, "trace-attach")
        self.assertEqual(
            traced.value.file_systems,
            [
                FileSystemMount(
                    file_system_id="file_system_abc", mount_path="/mnt/skills"
                )
            ],
        )

    def test_attach_file_system_threads_mount_modes(self):
        fake = _FakeRustClient()
        client = _sync_client(fake)

        traced = client.attach_file_system(
            "sbx-1",
            "file_system_abc",
            "/mnt/skills",
            read_only=True,
            prefetch=True,
        )

        self.assertEqual(
            fake.attach_calls,
            [("sbx-1", "file_system_abc", "/mnt/skills", True, True, None)],
        )
        self.assertEqual(
            traced.value.file_systems,
            [
                FileSystemMount(
                    file_system_id="file_system_abc",
                    mount_path="/mnt/skills",
                    read_only=True,
                    prefetch=True,
                )
            ],
        )

    def test_attach_file_system_threads_snapshot_pin(self):
        fake = _FakeRustClient()
        client = _sync_client(fake)

        traced = client.attach_file_system(
            "sbx-1",
            "file_system_abc",
            "/mnt/skills",
            read_only=True,
            snapshot_id="0abc123def",
        )

        self.assertEqual(
            fake.attach_calls,
            [("sbx-1", "file_system_abc", "/mnt/skills", True, False, "0abc123def")],
        )
        self.assertEqual(
            traced.value.file_systems,
            [
                FileSystemMount(
                    file_system_id="file_system_abc",
                    mount_path="/mnt/skills",
                    read_only=True,
                    snapshot_id="0abc123def",
                )
            ],
        )

    def test_attach_file_system_rejects_snapshot_pin_without_read_only(self):
        fake = _FakeRustClient()
        client = _sync_client(fake)

        with self.assertRaisesRegex(ValueError, "snapshot-pinned mounts are read-only"):
            client.attach_file_system(
                "sbx-1",
                "file_system_abc",
                "/mnt/skills",
                snapshot_id="0abc123def",
            )
        self.assertEqual(fake.attach_calls, [])

    def test_detach_file_system(self):
        fake = _FakeRustClient()
        client = _sync_client(fake)

        traced = client.detach_file_system("sbx-1", "/mnt/skills")

        self.assertEqual(fake.detach_calls, [("sbx-1", "/mnt/skills")])
        self.assertEqual(traced.trace_id, "trace-detach")
        self.assertEqual(traced.value.file_systems, [])

    def test_create_threads_file_systems(self):
        fake = _FakeRustClient()
        client = _sync_client(fake)

        client.create(
            image="python:3.11",
            file_systems=[
                FileSystemMount(
                    file_system_id="file_system_abc", mount_path="/mnt/skills"
                )
            ],
        )

        payload = json.loads(fake.create_request_json)
        self.assertEqual(
            payload["file_systems"],
            [{"file_system_id": "file_system_abc", "mount_path": "/mnt/skills"}],
        )

    def test_create_body_omits_false_and_includes_true_mount_modes(self):
        fake = _FakeRustClient()
        client = _sync_client(fake)

        client.create(
            image="python:3.11",
            file_systems=[
                FileSystemMount(
                    file_system_id="file_system_abc",
                    mount_path="/mnt/skills",
                    read_only=True,
                    prefetch=True,
                ),
                FileSystemMount(
                    file_system_id="file_system_def", mount_path="/mnt/data"
                ),
            ],
        )

        payload = json.loads(fake.create_request_json)
        self.assertEqual(
            payload["file_systems"],
            [
                {
                    "file_system_id": "file_system_abc",
                    "mount_path": "/mnt/skills",
                    "read_only": True,
                    "prefetch": True,
                },
                {"file_system_id": "file_system_def", "mount_path": "/mnt/data"},
            ],
        )

    def test_create_body_includes_snapshot_pin_and_rejects_writable_pin(self):
        fake = _FakeRustClient()
        client = _sync_client(fake)

        client.create(
            image="python:3.11",
            file_systems=[
                FileSystemMount(
                    file_system_id="file_system_abc",
                    mount_path="/mnt/skills",
                    read_only=True,
                    snapshot_id="0abc123def",
                ),
            ],
        )
        payload = json.loads(fake.create_request_json)
        self.assertEqual(
            payload["file_systems"],
            [
                {
                    "file_system_id": "file_system_abc",
                    "mount_path": "/mnt/skills",
                    "read_only": True,
                    "snapshot_id": "0abc123def",
                }
            ],
        )

        with self.assertRaisesRegex(ValueError, "snapshot-pinned mounts are read-only"):
            client.create(
                image="python:3.11",
                file_systems=[
                    FileSystemMount(
                        file_system_id="file_system_abc",
                        mount_path="/mnt/skills",
                        snapshot_id="0abc123def",
                    ),
                ],
            )

    def test_claim_threads_file_systems(self):
        fake = _FakeRustClient()
        client = _sync_client(fake)

        client.claim(
            "pool-1",
            file_systems=[
                FileSystemMount(
                    file_system_id="file_system_abc", mount_path="/mnt/skills"
                )
            ],
        )

        self.assertEqual(fake.claim_calls[0]["pool_id"], "pool-1")
        self.assertEqual(
            json.loads(fake.claim_calls[0]["request_json"]),
            {
                "file_systems": [
                    {
                        "file_system_id": "file_system_abc",
                        "mount_path": "/mnt/skills",
                    }
                ]
            },
        )

    def test_claim_body_omits_false_and_includes_true_mount_modes(self):
        fake = _FakeRustClient()
        client = _sync_client(fake)

        client.claim(
            "pool-1",
            file_systems=[
                FileSystemMount(
                    file_system_id="file_system_abc",
                    mount_path="/mnt/skills",
                    read_only=True,
                ),
                FileSystemMount(
                    file_system_id="file_system_def",
                    mount_path="/mnt/data",
                    prefetch=True,
                ),
            ],
        )

        self.assertEqual(
            json.loads(fake.claim_calls[0]["request_json"]),
            {
                "file_systems": [
                    {
                        "file_system_id": "file_system_abc",
                        "mount_path": "/mnt/skills",
                        "read_only": True,
                    },
                    {
                        "file_system_id": "file_system_def",
                        "mount_path": "/mnt/data",
                        "prefetch": True,
                    },
                ]
            },
        )

    def test_claim_body_includes_snapshot_pin_and_rejects_writable_pin(self):
        fake = _FakeRustClient()
        client = _sync_client(fake)

        client.claim(
            "pool-1",
            file_systems=[
                FileSystemMount(
                    file_system_id="file_system_abc",
                    mount_path="/mnt/skills",
                    read_only=True,
                    snapshot_id="0abc123def",
                ),
            ],
        )
        self.assertEqual(
            json.loads(fake.claim_calls[0]["request_json"]),
            {
                "file_systems": [
                    {
                        "file_system_id": "file_system_abc",
                        "mount_path": "/mnt/skills",
                        "read_only": True,
                        "snapshot_id": "0abc123def",
                    }
                ]
            },
        )

        with self.assertRaisesRegex(ValueError, "snapshot-pinned mounts are read-only"):
            client.claim(
                "pool-1",
                file_systems=[
                    FileSystemMount(
                        file_system_id="file_system_abc",
                        mount_path="/mnt/skills",
                        snapshot_id="0abc123def",
                    ),
                ],
            )
        self.assertEqual(len(fake.claim_calls), 1)

    def test_claim_without_file_systems_keeps_bodyless_native_call(self):
        fake = _FakeRustClient()
        client = _sync_client(fake)

        client.claim("pool-1")

        self.assertEqual(fake.claim_calls, [{"pool_id": "pool-1"}])


class TestAsyncSandboxClientFileSystems(unittest.IsolatedAsyncioTestCase):
    async def test_attach_file_system(self):
        fake = _FakeAsyncRustClient()
        client = _async_client(fake)

        traced = await client.attach_file_system(
            "sbx-1", "file_system_abc", "/mnt/skills"
        )

        self.assertEqual(
            fake.attach_calls,
            [("sbx-1", "file_system_abc", "/mnt/skills", False, False, None)],
        )
        self.assertEqual(traced.trace_id, "trace-attach")
        self.assertEqual(
            traced.value.file_systems,
            [
                FileSystemMount(
                    file_system_id="file_system_abc", mount_path="/mnt/skills"
                )
            ],
        )

    async def test_attach_file_system_threads_mount_modes(self):
        fake = _FakeAsyncRustClient()
        client = _async_client(fake)

        traced = await client.attach_file_system(
            "sbx-1",
            "file_system_abc",
            "/mnt/skills",
            read_only=True,
            prefetch=True,
        )

        self.assertEqual(
            fake.attach_calls,
            [("sbx-1", "file_system_abc", "/mnt/skills", True, True, None)],
        )
        self.assertEqual(
            traced.value.file_systems,
            [
                FileSystemMount(
                    file_system_id="file_system_abc",
                    mount_path="/mnt/skills",
                    read_only=True,
                    prefetch=True,
                )
            ],
        )

    async def test_attach_file_system_threads_snapshot_pin(self):
        fake = _FakeAsyncRustClient()
        client = _async_client(fake)

        traced = await client.attach_file_system(
            "sbx-1",
            "file_system_abc",
            "/mnt/skills",
            read_only=True,
            snapshot_id="0abc123def",
        )

        self.assertEqual(
            fake.attach_calls,
            [("sbx-1", "file_system_abc", "/mnt/skills", True, False, "0abc123def")],
        )
        self.assertEqual(
            traced.value.file_systems,
            [
                FileSystemMount(
                    file_system_id="file_system_abc",
                    mount_path="/mnt/skills",
                    read_only=True,
                    snapshot_id="0abc123def",
                )
            ],
        )

    async def test_attach_file_system_rejects_snapshot_pin_without_read_only(self):
        fake = _FakeAsyncRustClient()
        client = _async_client(fake)

        with self.assertRaisesRegex(ValueError, "snapshot-pinned mounts are read-only"):
            await client.attach_file_system(
                "sbx-1",
                "file_system_abc",
                "/mnt/skills",
                snapshot_id="0abc123def",
            )
        self.assertEqual(fake.attach_calls, [])

    async def test_claim_threads_file_systems(self):
        fake = _FakeAsyncRustClient()
        client = _async_client(fake)

        await client.claim(
            "pool-1",
            file_systems=[
                FileSystemMount(
                    file_system_id="file_system_abc", mount_path="/mnt/skills"
                )
            ],
        )

        self.assertEqual(fake.claim_calls[0]["pool_id"], "pool-1")
        self.assertEqual(
            json.loads(fake.claim_calls[0]["request_json"])["file_systems"],
            [{"file_system_id": "file_system_abc", "mount_path": "/mnt/skills"}],
        )

    async def test_claim_rejects_writable_snapshot_pin(self):
        fake = _FakeAsyncRustClient()
        client = _async_client(fake)

        with self.assertRaisesRegex(ValueError, "snapshot-pinned mounts are read-only"):
            await client.claim(
                "pool-1",
                file_systems=[
                    FileSystemMount(
                        file_system_id="file_system_abc",
                        mount_path="/mnt/skills",
                        snapshot_id="0abc123def",
                    ),
                ],
            )
        self.assertEqual(fake.claim_calls, [])


class TestFileSystemRegistry(unittest.TestCase):
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

    def test_create_file_system(self):
        fake = _FakeCloudApiClient()
        with patch(
            "tensorlake.sandbox.file_system._cloud_api_client",
            return_value=fake,
        ):
            fs = create_file_system("skills", description="skills volume")

        self.assertEqual(
            fake.create_args, ("org-1", "proj-1", "skills", "skills volume")
        )
        self.assertEqual(fs.id, "file_system_abc")
        self.assertEqual(fs.name, "skills")
        self.assertTrue(fake.closed)

    def test_list_file_systems(self):
        fake = _FakeCloudApiClient()
        with patch(
            "tensorlake.sandbox.file_system._cloud_api_client",
            return_value=fake,
        ):
            result = list_file_systems()

        self.assertEqual(fake.list_args, ("org-1", "proj-1"))
        self.assertEqual([f.id for f in result], ["file_system_abc"])
        self.assertTrue(fake.closed)

    def test_delete_file_system(self):
        fake = _FakeCloudApiClient()
        with patch(
            "tensorlake.sandbox.file_system._cloud_api_client",
            return_value=fake,
        ):
            delete_file_system("file_system_abc")

        self.assertEqual(fake.delete_args, ("org-1", "proj-1", "file_system_abc"))
        self.assertTrue(fake.closed)

    def test_missing_project_context_raises(self):
        with patch.dict(os.environ, {"TENSORLAKE_ORGANIZATION_ID": ""}, clear=False):
            with self.assertRaises(SandboxError):
                list_file_systems()

    def test_create_requires_non_empty_name(self):
        with self.assertRaises(TypeError):
            create_file_system("")


if __name__ == "__main__":
    unittest.main()
