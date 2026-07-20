"""Network-free unit tests for the filesystem SDK.

The wire protocol (chunking, ingest, commit jobs, retries) lives in the Rust
cloud-sdk core and is tested there. These tests pin the Python wrapper: how
native results map to models, how native errors translate into the
filesystem exception hierarchy, and the idempotency-key discipline on
writes. The native client under `FilesystemClient._native._client` is
replaced with a scripted stub.
"""

import json
import unittest
from typing import Any, Dict, List, Optional, Tuple

try:
    from tensorlake._cloud_sdk import CloudApiClientError
except ImportError:
    from _cloud_sdk import CloudApiClientError
from tensorlake.filesystem import (
    FileEntry,
    FileNotFoundInFilesystemError,
    FilesystemAPIError,
    FilesystemClient,
    FilesystemError,
    FilesystemInfo,
    FilesystemNotFoundError,
    Snapshot,
)
from tensorlake.filesystem.client import mount_status_from_raw

_PROJECT = "proj_test"
_COMMIT = "c" * 40


def _client_with_stub(stub: "_StubNative") -> FilesystemClient:
    client = FilesystemClient(
        api_key="test-key",
        api_url="https://api.tensorlake.ai",
        organization_id="org_test",
        project_id=_PROJECT,
    )
    client._native._client = stub
    return client


class _StubNative:
    """Scripted stand-in for the Rust CloudApiClient filesystem surface.

    ``errors`` maps a method name to a (category, status, message) tuple to
    raise as CloudApiClientError instead of answering.
    """

    def __init__(
        self,
        meta: Optional[Dict[str, Any]] = None,
        repos: Optional[List[Dict[str, Any]]] = None,
        ref: Optional[Dict[str, Any]] = None,
        entries: Optional[List[Dict[str, Any]]] = None,
        file_bytes: bytes = b"",
        push_report: Optional[Dict[str, Any]] = None,
        errors: Optional[Dict[str, Tuple[str, Optional[int], str]]] = None,
    ):
        self.meta = meta or {
            "name": "my-fs",
            "full_name": f"{_PROJECT}/my-fs",
            "default_branch": "main",
            "status": "active",
            "kind": "filesystem",
        }
        self.repos = repos or []
        self.ref = ref or {
            "ref_name": "refs/heads/main",
            "oid": _COMMIT,
            "resolved_commit": _COMMIT,
            "generation": 3,
        }
        self.entries = entries or []
        self.file_bytes = file_bytes
        self.push_report = push_report or {
            "commit": _COMMIT,
            "tree": "t" * 40,
            "ref_name": "refs/heads/main",
            "created": True,
            "files": 1,
            "bytes_total": 1,
            "chunks_total": 1,
            "chunks_uploaded": 1,
            "bytes_uploaded": 1,
            "file_blob_oids": [],
        }
        self.errors = errors or {}
        self.calls: List[Tuple[str, tuple]] = []

    def _maybe_fail(self, method: str) -> None:
        if method in self.errors:
            raise CloudApiClientError(*self.errors[method])

    #: Branch reported by create — non-"main" simulates the binding adopting
    #: a pre-existing filesystem on a lost-response retry.
    create_branch = "main"

    def create_filesystem(self, project_id, name):
        self.calls.append(("create_filesystem", (project_id, name)))
        self._maybe_fail("create_filesystem")
        return json.dumps({"trace_id": "trace-1", "default_branch": self.create_branch})

    def filesystem_meta(self, project_id, name):
        self.calls.append(("filesystem_meta", (project_id, name)))
        self._maybe_fail("filesystem_meta")
        return json.dumps(self.meta)

    def list_filesystems(self, project_id):
        self.calls.append(("list_filesystems", (project_id,)))
        self._maybe_fail("list_filesystems")
        return json.dumps(
            {"project": project_id, "repos": self.repos, "next_after": None}
        )

    def delete_filesystem(self, project_id, name):
        self.calls.append(("delete_filesystem", (project_id, name)))
        self._maybe_fail("delete_filesystem")
        return "trace-2"

    def filesystem_ref_status(self, project_id, name, refspec):
        self.calls.append(("filesystem_ref_status", (project_id, name, refspec)))
        self._maybe_fail("filesystem_ref_status")
        return json.dumps(self.ref)

    def read_filesystem_file(self, project_id, name, path, version):
        self.calls.append(("read_filesystem_file", (project_id, name, path, version)))
        self._maybe_fail("read_filesystem_file")
        return self.file_bytes

    def list_filesystem_tree(self, project_id, name, dir_path, version):
        self.calls.append(
            ("list_filesystem_tree", (project_id, name, dir_path, version))
        )
        self._maybe_fail("list_filesystem_tree")
        return json.dumps({"entries": self.entries})

    def push_filesystem_files(
        self, project_id, name, files, deletes, message, branch, idempotency_key
    ):
        self.calls.append(
            (
                "push_filesystem_files",
                (project_id, name, files, deletes, message, branch, idempotency_key),
            )
        )
        self._maybe_fail("push_filesystem_files")
        return json.dumps(self.push_report)


class TestModels(unittest.TestCase):
    def test_models_parse_wire_shapes(self):
        info = FilesystemInfo.model_validate(
            {
                "name": "skills",
                "full_name": f"{_PROJECT}/skills",
                "default_branch": "main",
                "status": "active",
                "kind": "filesystem",
                "unknown_field": 1,
            }
        )
        self.assertEqual(info.name, "skills")
        entry = FileEntry.model_validate({"name": "d", "oid": "x", "mode": 0o40000})
        self.assertTrue(entry.is_dir)
        entry = FileEntry.model_validate({"name": "f", "oid": "y", "mode": 0o100644})
        self.assertFalse(entry.is_dir)


class TestFilesystemClient(unittest.TestCase):
    def test_lifecycle_calls_native_with_project_scope(self):
        stub = _StubNative(
            repos=[
                {
                    "name": "a",
                    "full_name": f"{_PROJECT}/a",
                    "default_branch": "main",
                    "status": "active",
                    "kind": "filesystem",
                }
            ]
        )
        client = _client_with_stub(stub)
        fs = client.create("my-fs")
        self.assertEqual(fs.name, "my-fs")
        infos = client.list()
        self.assertEqual([i.name for i in infos], ["a"])
        client.delete("my-fs")
        self.assertEqual(
            [c[0] for c in stub.calls],
            ["create_filesystem", "list_filesystems", "delete_filesystem"],
        )
        for _, args in stub.calls:
            self.assertEqual(args[0], _PROJECT)

    def test_get_missing_filesystem_raises_not_found(self):
        stub = _StubNative(
            errors={"filesystem_meta": ("remote_api", 404, "no such repo")}
        )
        client = _client_with_stub(stub)
        with self.assertRaises(FilesystemNotFoundError):
            client.get("nope")

    def test_get_non_filesystem_kind_raises_not_found(self):
        stub = _StubNative()
        stub.meta["kind"] = "repository"
        client = _client_with_stub(stub)
        with self.assertRaises(FilesystemNotFoundError):
            client.get("code-repo")

    def test_write_files_maps_push_report_and_sets_idempotency_key(self):
        stub = _StubNative()
        client = _client_with_stub(stub)
        fs = client.create("my-fs")
        snapshot = fs.write_files(
            {"a.txt": "hello", "b.bin": b"\x00\x01"}, deletes=["old.txt"]
        )
        self.assertIsInstance(snapshot, Snapshot)
        self.assertEqual(snapshot.commit, _COMMIT)
        self.assertTrue(snapshot.created)

        method, args = stub.calls[-1]
        self.assertEqual(method, "push_filesystem_files")
        _, name, files, deletes, _message, branch, idempotency_key = args
        self.assertEqual(name, "my-fs")
        # Strings are encoded to bytes; bytes pass through.
        self.assertEqual(dict(files), {"a.txt": b"hello", "b.bin": b"\x00\x01"})
        self.assertEqual(deletes, ["old.txt"])
        self.assertEqual(branch, "main")
        # A fresh stable key per logical write (the Rust core reuses it across
        # its retries so a lost response cannot double-commit).
        self.assertRegex(idempotency_key, r"^[0-9a-f]{32}$")

    def test_empty_write_rejected(self):
        client = _client_with_stub(_StubNative())
        fs = client.create("my-fs")
        with self.assertRaises(FilesystemError):
            fs.write_files({})

    def test_read_file_and_missing_file(self):
        stub = _StubNative(file_bytes=b"content")
        client = _client_with_stub(stub)
        fs = client.create("my-fs")
        self.assertEqual(fs.read_file("docs/a.txt"), b"content")
        self.assertEqual(stub.calls[-1][1], (_PROJECT, "my-fs", "docs/a.txt", "main"))

        stub.errors["read_filesystem_file"] = ("remote_api", 404, "not found")
        with self.assertRaises(FileNotFoundInFilesystemError):
            fs.read_file("docs/missing.txt")
        with self.assertRaises(FilesystemError):
            fs.read_file("//")

    def test_list_files_builds_paths(self):
        stub = _StubNative(
            entries=[
                {"name": "sub", "oid": "x", "mode": 0o40000},
                {"name": "a.txt", "oid": "y", "mode": 0o100644, "size": 3},
            ]
        )
        client = _client_with_stub(stub)
        fs = client.create("my-fs")
        entries = fs.list_files("docs")
        self.assertEqual([e.path for e in entries], ["docs/sub", "docs/a.txt"])
        self.assertTrue(entries[0].is_dir)
        self.assertEqual(entries[1].size, 3)

    def test_status_maps_head_and_generation(self):
        client = _client_with_stub(_StubNative())
        fs = client.create("my-fs")
        status = fs.status()
        self.assertEqual(status.head_commit, _COMMIT)
        self.assertEqual(status.generation, 3)
        self.assertEqual(status.default_branch, "main")

    def test_status_of_empty_filesystem(self):
        stub = _StubNative(
            ref={
                "ref_name": "refs/heads/main",
                "oid": None,
                "resolved_commit": None,
                "generation": 0,
            }
        )
        client = _client_with_stub(stub)
        fs = client.create("my-fs")
        self.assertIsNone(fs.status().head_commit)
        with self.assertRaises(FilesystemError):
            fs.snapshot("nothing yet")

    def test_status_swallows_only_ref_404(self):
        stub = _StubNative(
            errors={"filesystem_ref_status": ("remote_api", 404, "no ref")}
        )
        client = _client_with_stub(stub)
        fs = client.create("my-fs")
        self.assertIsNone(fs.status().head_commit)

        stub.errors["filesystem_ref_status"] = ("remote_api", 503, "unavailable")
        with self.assertRaises(FilesystemAPIError) as caught:
            fs.status()
        self.assertEqual(caught.exception.status_code, 503)

    def test_snapshot_pins_current_head(self):
        client = _client_with_stub(_StubNative())
        fs = client.create("my-fs")
        snapshot = fs.snapshot("pin")
        self.assertEqual(snapshot.commit, _COMMIT)
        self.assertFalse(snapshot.created)

    def test_create_seeds_branch_reported_by_binding(self):
        stub = _StubNative()
        stub.create_branch = "trunk"
        client = _client_with_stub(stub)
        fs = client.create("my-fs")
        fs.write_file("a.txt", b"x")
        # (project, name, files, deletes, message, branch, idempotency_key)
        self.assertEqual(stub.calls[-1][1][5], "trunk")

    def test_non_main_default_branch_followed_by_writes_and_reads(self):
        stub = _StubNative()
        stub.meta["default_branch"] = "trunk"
        client = _client_with_stub(stub)
        fs = client.get("my-fs")
        fs.write_file("a.txt", b"x")
        # (project, name, files, deletes, message, branch, idempotency_key)
        self.assertEqual(stub.calls[-1][1][5], "trunk")
        fs.read_file("a.txt")
        # (project, name, path, version)
        self.assertEqual(stub.calls[-1][1][3], "trunk")
        fs.list_files()
        self.assertEqual(stub.calls[-1][1][3], "trunk")
        # An explicit version always wins over the default branch.
        fs.read_file("a.txt", version="c" * 40)
        self.assertEqual(stub.calls[-1][1][3], "c" * 40)

    def test_empty_message_gets_default(self):
        stub = _StubNative()
        client = _client_with_stub(stub)
        fs = client.create("my-fs")
        # Explicit "" gets the default too — parity with the TypeScript SDK.
        snapshot = fs.write_file("a.txt", b"x", message="")
        message = stub.calls[-1][1][4]
        self.assertEqual(message, "write 1 file(s) via SDK")
        self.assertEqual(snapshot.message, "write 1 file(s) via SDK")

    def test_mount_status_from_raw_semantics(self):
        status = mount_status_from_raw(
            {
                "path": "",
                "mount_path": "/mnt/x",
                "mounted": None,
                "active": True,
                "filesystem": "",
            },
            local_path="/ignored",
        )
        # Empty path falls through to mount_path; a present-but-null
        # "mounted" means not mounted; empty filesystem becomes None.
        self.assertEqual(status.path, "/mnt/x")
        self.assertFalse(status.mounted)
        self.assertIsNone(status.filesystem)

        defaulted = mount_status_from_raw({}, local_path="/mnt/y")
        self.assertEqual(defaulted.path, "/mnt/y")
        self.assertTrue(defaulted.mounted)

    def test_error_translation_without_status(self):
        stub = _StubNative(
            errors={"push_filesystem_files": ("internal", None, "commit job failed")}
        )
        client = _client_with_stub(stub)
        fs = client.create("my-fs")
        with self.assertRaises(FilesystemError) as caught:
            fs.write_file("a.txt", b"x")
        self.assertNotIsInstance(caught.exception, FilesystemAPIError)
        self.assertIn("commit job failed", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
