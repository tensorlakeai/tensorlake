"""Tests for the Sandbox class-level API.

Covers the lifecycle API migration that moves create/connect factories
and suspend/resume/checkpoint lifecycle methods onto the Sandbox class
while keeping SandboxClient as a deprecated shim.
"""

import unittest
import warnings
from unittest.mock import MagicMock

from tensorlake.sandbox import (
    CreateSnapshotResponse,
    Sandbox,
    SandboxStatus,
    SnapshotInfo,
    SnapshotStatus,
)
from tensorlake.sandbox.client import SandboxClient
from tensorlake.sandbox.exceptions import SandboxError


class _FakeRustProxyClient:
    """Minimal proxy client stub so Sandbox.__init__ succeeds in tests."""

    def close(self):
        return None

    def base_url(self):
        return "http://localhost:9443"


def _make_sandbox_with_fake_lifecycle_client(
    *,
    sandbox_id: str = "sbx-1",
    name: str | None = None,
    status: SandboxStatus = SandboxStatus.RUNNING,
) -> tuple[Sandbox, MagicMock]:
    """Build a Sandbox with an in-memory proxy stub + a mock lifecycle client."""
    sandbox = Sandbox(
        identifier=sandbox_id,
        proxy_url="http://localhost:9443",
        api_key="k",
        _proxy_rust_client=_FakeRustProxyClient(),
    )
    sandbox._sandbox_id = sandbox_id
    sandbox._name = name
    sandbox._name_loaded = True

    client = MagicMock(spec=SandboxClient)
    info = MagicMock()
    info.status = status
    info.sandbox_id = sandbox_id
    info.routing_hint = None
    info.name = name
    client.get.return_value = info
    sandbox._lifecycle_client = client
    return sandbox, client


class TestSandboxFactories(unittest.TestCase):
    """Sandbox.create / Sandbox.connect / Sandbox.get_snapshot / Sandbox.delete_snapshot."""

    def test_create_delegates_to_client_create_and_connect(self):
        fake_client = MagicMock(spec=SandboxClient)
        sentinel = MagicMock(name="Sandbox handle")
        fake_client.create_and_connect.return_value = sentinel

        result = Sandbox.create(
            name="my-sbx",
            cpus=2.0,
            memory_mb=2048,
            _client=fake_client,
        )

        self.assertIs(result, sentinel)
        fake_client.create_and_connect.assert_called_once()
        call_kwargs = fake_client.create_and_connect.call_args.kwargs
        self.assertEqual(call_kwargs["name"], "my-sbx")
        self.assertEqual(call_kwargs["cpus"], 2.0)
        self.assertEqual(call_kwargs["memory_mb"], 2048)

    def test_create_from_snapshot_passes_snapshot_id(self):
        fake_client = MagicMock(spec=SandboxClient)
        fake_client.create_and_connect.return_value = MagicMock()

        Sandbox.create(snapshot_id="snap-42", _client=fake_client)

        self.assertEqual(
            fake_client.create_and_connect.call_args.kwargs["snapshot_id"],
            "snap-42",
        )

    def test_connect_delegates_to_client_connect(self):
        fake_client = MagicMock(spec=SandboxClient)
        sentinel = MagicMock(name="Sandbox handle")
        fake_client.connect.return_value = sentinel

        result = Sandbox.connect(sandbox_id="my-sbx-id", _client=fake_client)

        self.assertIs(result, sentinel)
        fake_client.connect.assert_called_once()
        call_kwargs = fake_client.connect.call_args.kwargs
        self.assertEqual(call_kwargs["sandbox_id"], "my-sbx-id")

    def test_get_snapshot_classmethod_delegates(self):
        fake_client = MagicMock(spec=SandboxClient)
        info = SnapshotInfo.model_validate_json(
            '{"id":"snap-1","namespace":"default","sandbox_id":"sbx-1",'
            '"base_image":"python:3.12","status":"completed",'
            '"snapshot_uri":"s3://snap-1.tar.zst"}'
        )
        fake_client.get_snapshot.return_value = info

        result = Sandbox.get_snapshot("snap-1", _client=fake_client)

        self.assertIs(result, info)
        fake_client.get_snapshot.assert_called_once_with("snap-1")

    def test_delete_snapshot_classmethod_delegates(self):
        fake_client = MagicMock(spec=SandboxClient)
        Sandbox.delete_snapshot("snap-1", _client=fake_client)
        fake_client.delete_snapshot.assert_called_once_with("snap-1")


class TestSandboxSuspend(unittest.TestCase):
    def test_suspend_blocks_until_suspended(self):
        sandbox, client = _make_sandbox_with_fake_lifecycle_client()
        client.get.side_effect = [
            MagicMock(status=SandboxStatus.RUNNING, sandbox_id="sbx-1"),
            MagicMock(status=SandboxStatus.SUSPENDED, sandbox_id="sbx-1"),
        ]

        sandbox.suspend(poll_interval=0)

        client.suspend.assert_called_once_with("sbx-1")
        self.assertEqual(client.get.call_count, 2)

    def test_suspend_wait_false_returns_immediately(self):
        sandbox, client = _make_sandbox_with_fake_lifecycle_client()

        sandbox.suspend(wait=False)

        client.suspend.assert_called_once_with("sbx-1")
        client.get.assert_not_called()

    def test_suspend_times_out(self):
        sandbox, client = _make_sandbox_with_fake_lifecycle_client()
        # Always Running → never reaches Suspended.
        client.get.return_value = MagicMock(
            status=SandboxStatus.RUNNING, sandbox_id="sbx-1"
        )

        with self.assertRaisesRegex(SandboxError, "did not reach Suspended"):
            sandbox.suspend(timeout=0.01, poll_interval=0)

    def test_suspend_raises_if_terminated_during_wait(self):
        sandbox, client = _make_sandbox_with_fake_lifecycle_client()
        client.get.return_value = MagicMock(
            status=SandboxStatus.TERMINATED, sandbox_id="sbx-1"
        )

        with self.assertRaisesRegex(SandboxError, "became terminated"):
            sandbox.suspend(poll_interval=0)

    def test_suspend_without_lifecycle_client_raises(self):
        sandbox, _ = _make_sandbox_with_fake_lifecycle_client()
        sandbox._lifecycle_client = None

        with self.assertRaisesRegex(SandboxError, "lifecycle client"):
            sandbox.suspend()


class TestSandboxResume(unittest.TestCase):
    def test_resume_fast_path_when_already_running(self):
        sandbox, client = _make_sandbox_with_fake_lifecycle_client(
            status=SandboxStatus.RUNNING
        )

        sandbox.resume(poll_interval=0)

        # Fast path: we peeked at status and returned without calling resume.
        client.resume.assert_not_called()

    def test_resume_blocks_until_running(self):
        sandbox, client = _make_sandbox_with_fake_lifecycle_client()
        # First get: fast-path check, reports Suspended → proceed to resume.
        # Second get: still Suspended (one poll round).
        # Third get: Running.
        client.get.side_effect = [
            MagicMock(status=SandboxStatus.SUSPENDED, sandbox_id="sbx-1"),
            MagicMock(status=SandboxStatus.SUSPENDED, sandbox_id="sbx-1"),
            MagicMock(status=SandboxStatus.RUNNING, sandbox_id="sbx-1"),
        ]

        sandbox.resume(poll_interval=0)

        client.resume.assert_called_once_with("sbx-1")

    def test_resume_wait_false_returns_immediately(self):
        sandbox, client = _make_sandbox_with_fake_lifecycle_client()

        sandbox.resume(wait=False)

        client.resume.assert_called_once_with("sbx-1")
        client.get.assert_not_called()

    def test_resume_times_out(self):
        sandbox, client = _make_sandbox_with_fake_lifecycle_client()
        client.get.return_value = MagicMock(
            status=SandboxStatus.SUSPENDED, sandbox_id="sbx-1"
        )

        with self.assertRaisesRegex(SandboxError, "did not reach Running"):
            sandbox.resume(timeout=0.01, poll_interval=0)


class TestSandboxCheckpoint(unittest.TestCase):
    def test_checkpoint_blocks_via_snapshot_and_wait(self):
        sandbox, client = _make_sandbox_with_fake_lifecycle_client()
        info = SnapshotInfo.model_validate_json(
            '{"id":"snap-1","namespace":"default","sandbox_id":"sbx-1",'
            '"base_image":"python:3.12","status":"completed",'
            '"snapshot_uri":"s3://snap-1.tar.zst"}'
        )
        client.snapshot_and_wait.return_value = info

        result = sandbox.checkpoint()

        self.assertIs(result, info)
        client.snapshot_and_wait.assert_called_once()
        kwargs = client.snapshot_and_wait.call_args.kwargs
        self.assertEqual(client.snapshot_and_wait.call_args.args, ("sbx-1",))
        self.assertEqual(kwargs.get("timeout"), 300)
        self.assertEqual(kwargs.get("poll_interval"), 1.0)

    def test_checkpoint_wait_false_returns_create_response(self):
        sandbox, client = _make_sandbox_with_fake_lifecycle_client()
        resp = CreateSnapshotResponse(
            snapshot_id="snap-1", status=SnapshotStatus.IN_PROGRESS
        )
        client.snapshot.return_value = resp

        result = sandbox.checkpoint(wait=False)

        self.assertIs(result, resp)
        client.snapshot.assert_called_once_with("sbx-1", content_mode=None)
        client.snapshot_and_wait.assert_not_called()


class TestSandboxListSnapshots(unittest.TestCase):
    def test_list_snapshots_filters_by_sandbox_id(self):
        sandbox, client = _make_sandbox_with_fake_lifecycle_client(sandbox_id="sbx-1")
        snap_for_this = SnapshotInfo.model_validate_json(
            '{"id":"snap-1","namespace":"default","sandbox_id":"sbx-1",'
            '"base_image":"python:3.12","status":"completed",'
            '"snapshot_uri":"s3://snap-1.tar.zst"}'
        )
        snap_other = SnapshotInfo.model_validate_json(
            '{"id":"snap-2","namespace":"default","sandbox_id":"sbx-2",'
            '"base_image":"python:3.12","status":"completed",'
            '"snapshot_uri":"s3://snap-2.tar.zst"}'
        )
        client.list_snapshots.return_value = [snap_for_this, snap_other]

        snaps = sandbox.list_snapshots()

        self.assertEqual([s.snapshot_id for s in snaps], ["snap-1"])


class TestSandboxClientDeprecation(unittest.TestCase):
    def setUp(self):
        import tensorlake.sandbox.client as client_module

        self._client_module = client_module
        client_module._sandbox_client_deprecation_warned = False

    def test_construction_emits_deprecation_warning(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            SandboxClient(api_url="http://localhost:8900", api_key="k")

        dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(len(dep_warnings), 1)
        self.assertIn("SandboxClient is deprecated", str(dep_warnings[0].message))

    def test_deprecation_warning_fires_only_once_per_process(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            SandboxClient(api_url="http://localhost:8900", api_key="k")
            SandboxClient(api_url="http://localhost:8900", api_key="k")
            SandboxClient(api_url="http://localhost:8900", api_key="k")

        dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(len(dep_warnings), 1)

    def test_internal_flag_suppresses_warning(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            SandboxClient(api_url="http://localhost:8900", api_key="k", _internal=True)

        dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(len(dep_warnings), 0)


if __name__ == "__main__":
    unittest.main()
