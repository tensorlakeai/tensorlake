"""Integration tests for sandbox lifecycle management APIs.

Requires a running Indexify server (localhost:8900/8901) and the
indexify-dataplane binary (set DATAPLANE_BIN env var).

Usage:
    export TENSORLAKE_API_URL=http://localhost:8900
    export DATAPLANE_BIN=/path/to/indexify-dataplane
    poetry run python tests/sandbox/test_lifecycle.py
"""

import os
import sys
import time
import unittest

from tensorlake.sandbox import (
    PoolInUseError,
    PoolNotFoundError,
    SandboxClient,
    SandboxNotFoundError,
    SandboxStatus,
)

from .testing import DataplaneProcessContextManager

# ---------------------------------------------------------------------------
# Module-level setup / teardown
# ---------------------------------------------------------------------------

_dataplane: DataplaneProcessContextManager | None = None
_client: SandboxClient | None = None

_SANDBOX_IMAGE = "docker.io/library/alpine:latest"
_SANDBOX_CPUS = 0.2
_SANDBOX_MEMORY_MB = 100
_SANDBOX_DISK_MB = 1024


def setUpModule():
    global _dataplane, _client

    api_url = os.environ.get("TENSORLAKE_API_URL", "http://localhost:8900")

    _dataplane = DataplaneProcessContextManager()
    _dataplane.start()

    _client = SandboxClient(api_url=api_url)


def tearDownModule():
    global _dataplane, _client

    if _client is not None:
        _client.close()
        _client = None

    if _dataplane is not None:
        _dataplane.stop()
        _dataplane = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _poll_sandbox_status(
    client: SandboxClient,
    sandbox_id: str,
    target: SandboxStatus,
    timeout: float = 60.0,
    interval: float = 1.0,
) -> SandboxStatus:
    """Poll until the sandbox reaches *target* status or times out."""
    deadline = time.time() + timeout
    status = None
    while time.time() < deadline:
        info = client.get(sandbox_id)
        status = info.status
        if status == target:
            return status
        time.sleep(interval)
    raise TimeoutError(
        f"Sandbox {sandbox_id} did not reach {target} within {timeout}s "
        f"(last status: {status})"
    )


# ---------------------------------------------------------------------------
# TestSandboxLifecycle
# ---------------------------------------------------------------------------


class TestSandboxLifecycle(unittest.TestCase):
    """CRUD lifecycle for individual sandboxes."""

    sandbox_id: str | None = None

    @classmethod
    def setUpClass(cls):
        assert _client is not None, "Module-level setup did not create client"

    @classmethod
    def tearDownClass(cls):
        if cls.sandbox_id and _client:
            try:
                _client.delete(cls.sandbox_id)
            except Exception:
                pass

    # Tests are numbered to enforce execution order within the class.

    def test_1_create_sandbox(self):
        resp = _client.create(
            image=_SANDBOX_IMAGE,
            cpus=_SANDBOX_CPUS,
            memory_mb=_SANDBOX_MEMORY_MB,
            ephemeral_disk_mb=_SANDBOX_DISK_MB,
        )
        self.assertIsNotNone(resp.sandbox_id)
        self.assertIn(resp.status, (SandboxStatus.PENDING, SandboxStatus.RUNNING))
        self.__class__.sandbox_id = resp.sandbox_id

    def test_2_get_sandbox(self):
        self.assertIsNotNone(self.__class__.sandbox_id, "Depends on test_1")
        info = _client.get(self.__class__.sandbox_id)
        self.assertEqual(info.sandbox_id, self.__class__.sandbox_id)
        self.assertIn(info.status, (SandboxStatus.PENDING, SandboxStatus.RUNNING))

    def test_3_list_sandboxes(self):
        self.assertIsNotNone(self.__class__.sandbox_id, "Depends on test_1")
        sandboxes = _client.list()
        ids = [s.sandbox_id for s in sandboxes]
        self.assertIn(self.__class__.sandbox_id, ids)

    def test_4_sandbox_transitions_to_running(self):
        self.assertIsNotNone(self.__class__.sandbox_id, "Depends on test_1")
        status = _poll_sandbox_status(
            _client, self.__class__.sandbox_id, SandboxStatus.RUNNING, timeout=60
        )
        self.assertEqual(status, SandboxStatus.RUNNING)

    def test_5_delete_sandbox(self):
        self.assertIsNotNone(self.__class__.sandbox_id, "Depends on test_1")
        _client.delete(self.__class__.sandbox_id)

    def test_6_get_terminated_sandbox(self):
        self.assertIsNotNone(self.__class__.sandbox_id, "Depends on test_5")
        info = _client.get(self.__class__.sandbox_id)
        self.assertEqual(info.status, SandboxStatus.TERMINATED)

    def test_7_delete_nonexistent_sandbox(self):
        with self.assertRaises(SandboxNotFoundError):
            _client.delete("nonexistent-sandbox-id-000")


# ---------------------------------------------------------------------------
# TestPoolLifecycle
# ---------------------------------------------------------------------------


class TestPoolLifecycle(unittest.TestCase):
    """CRUD lifecycle for sandbox pools."""

    pool_id: str | None = None

    @classmethod
    def tearDownClass(cls):
        if cls.pool_id and _client:
            try:
                _client.delete_pool(cls.pool_id)
            except Exception:
                pass

    def test_1_create_pool(self):
        resp = _client.create_pool(
            image=_SANDBOX_IMAGE,
            cpus=_SANDBOX_CPUS,
            memory_mb=_SANDBOX_MEMORY_MB,
            ephemeral_disk_mb=_SANDBOX_DISK_MB,
        )
        self.assertIsNotNone(resp.pool_id)
        self.__class__.pool_id = resp.pool_id

    def test_2_get_pool(self):
        self.assertIsNotNone(self.__class__.pool_id, "Depends on test_1")
        info = _client.get_pool(self.__class__.pool_id)
        self.assertEqual(info.pool_id, self.__class__.pool_id)
        self.assertEqual(info.image, _SANDBOX_IMAGE)
        self.assertAlmostEqual(info.resources.cpus, _SANDBOX_CPUS, places=2)
        self.assertEqual(info.resources.memory_mb, _SANDBOX_MEMORY_MB)

    def test_3_list_pools(self):
        self.assertIsNotNone(self.__class__.pool_id, "Depends on test_1")
        pools = _client.list_pools()
        ids = [p.pool_id for p in pools]
        self.assertIn(self.__class__.pool_id, ids)

    def test_4_update_pool(self):
        self.assertIsNotNone(self.__class__.pool_id, "Depends on test_1")
        updated = _client.update_pool(
            pool_id=self.__class__.pool_id,
            image=_SANDBOX_IMAGE,
            cpus=_SANDBOX_CPUS,
            memory_mb=200,
            ephemeral_disk_mb=_SANDBOX_DISK_MB,
            warm_containers=1,
        )
        self.assertEqual(updated.resources.memory_mb, 200)
        self.assertEqual(updated.warm_containers, 1)

    def test_5_delete_pool(self):
        self.assertIsNotNone(self.__class__.pool_id, "Depends on test_1")
        _client.delete_pool(self.__class__.pool_id)
        self.__class__.pool_id = None  # Prevent tearDownClass from double-deleting

    def test_6_delete_nonexistent_pool(self):
        with self.assertRaises(PoolNotFoundError):
            _client.delete_pool("nonexistent-pool-id-000")


# ---------------------------------------------------------------------------
# TestPoolWithSandboxes
# ---------------------------------------------------------------------------


class TestPoolWithSandboxes(unittest.TestCase):
    """Pool + sandbox interactions."""

    pool_id: str | None = None
    sandbox_id: str | None = None

    @classmethod
    def tearDownClass(cls):
        if cls.sandbox_id and _client:
            try:
                _client.delete(cls.sandbox_id)
            except Exception:
                pass
        if cls.pool_id and _client:
            try:
                _client.delete_pool(cls.pool_id)
            except Exception:
                pass

    def test_1_create_pool_with_warm_containers(self):
        resp = _client.create_pool(
            image=_SANDBOX_IMAGE,
            cpus=_SANDBOX_CPUS,
            memory_mb=_SANDBOX_MEMORY_MB,
            ephemeral_disk_mb=_SANDBOX_DISK_MB,
            warm_containers=2,
        )
        self.assertIsNotNone(resp.pool_id)
        self.__class__.pool_id = resp.pool_id

    def test_2_create_sandbox_from_pool(self):
        self.assertIsNotNone(self.__class__.pool_id, "Depends on test_1")
        resp = _client.create(
            pool_id=self.__class__.pool_id,
            cpus=_SANDBOX_CPUS,
            memory_mb=_SANDBOX_MEMORY_MB,
            ephemeral_disk_mb=_SANDBOX_DISK_MB,
        )
        self.assertIsNotNone(resp.sandbox_id)
        self.assertIn(resp.status, (SandboxStatus.PENDING, SandboxStatus.RUNNING))
        self.__class__.sandbox_id = resp.sandbox_id

    def test_3_sandbox_from_pool_reaches_running(self):
        self.assertIsNotNone(self.__class__.sandbox_id, "Depends on test_2")
        status = _poll_sandbox_status(
            _client, self.__class__.sandbox_id, SandboxStatus.RUNNING, timeout=60
        )
        self.assertEqual(status, SandboxStatus.RUNNING)

    def test_4_delete_pool_with_active_sandbox(self):
        self.assertIsNotNone(self.__class__.pool_id, "Depends on test_1")
        with self.assertRaises(PoolInUseError):
            _client.delete_pool(self.__class__.pool_id)

    def test_5_delete_sandbox_then_pool(self):
        self.assertIsNotNone(self.__class__.sandbox_id, "Depends on test_2")
        self.assertIsNotNone(self.__class__.pool_id, "Depends on test_1")

        _client.delete(self.__class__.sandbox_id)
        self.__class__.sandbox_id = None

        _client.delete_pool(self.__class__.pool_id)
        self.__class__.pool_id = None


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    unittest.main()
