"""Integration tests for sandbox lifecycle management APIs.

Requires a running Indexify server (localhost:8900/8901) and a running
indexify-dataplane process.

Usage:
    export TENSORLAKE_API_URL=http://localhost:8900
    poetry run python tests/sandbox/test_lifecycle.py
"""

import os
import time
import unittest

from tensorlake.sandbox import (
    PoolContainerInfo,
    PoolInUseError,
    PoolNotFoundError,
    SandboxClient,
    SandboxNotFoundError,
    SandboxStatus,
)

# ---------------------------------------------------------------------------
# Module-level setup / teardown
# ---------------------------------------------------------------------------

_client: SandboxClient | None = None

_SANDBOX_IMAGE = "docker.io/library/alpine:latest"
_SANDBOX_CPUS = 0.2
_SANDBOX_MEMORY_MB = 100
_SANDBOX_DISK_MB = 1024


def setUpModule():
    global _client
    api_url = os.environ.get("TENSORLAKE_API_URL", "http://localhost:8900")
    _client = SandboxClient(api_url=api_url)


def tearDownModule():
    global _client
    if _client is not None:
        _client.close()
        _client = None

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


def _poll_pool_containers(
    client: SandboxClient,
    pool_id: str,
    min_count: int,
    timeout: float = 60.0,
    interval: float = 1.0,
) -> list[PoolContainerInfo]:
    """Poll until the pool has at least *min_count* containers or times out."""
    deadline = time.time() + timeout
    containers = []
    while time.time() < deadline:
        detail = client.get_pool(pool_id)
        containers = detail.containers or []
        if len(containers) >= min_count:
            return containers
        time.sleep(interval)
    raise TimeoutError(
        f"Pool {pool_id} did not reach {min_count} containers within {timeout}s "
        f"(last count: {len(containers)})"
    )


def _get_pool_containers(
    client: SandboxClient, pool_id: str
) -> list[PoolContainerInfo]:
    """Return current containers for a pool."""
    detail = client.get_pool(pool_id)
    return detail.containers or []


def _warm_containers(containers: list[PoolContainerInfo]) -> list[PoolContainerInfo]:
    """Filter to warm (unclaimed) containers."""
    return [c for c in containers if c.sandbox_id is None]


def _claimed_containers(containers: list[PoolContainerInfo]) -> list[PoolContainerInfo]:
    """Filter to claimed (sandbox-assigned) containers."""
    return [c for c in containers if c.sandbox_id is not None]


# ---------------------------------------------------------------------------
# TestSandboxLifecycle
# ---------------------------------------------------------------------------


class TestSandboxLifecycle(unittest.TestCase):
    """Create a sandbox, verify it transitions to Running, delete it,
    verify it transitions to Terminated."""

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

    def test_1_create_sandbox(self):
        resp = _client.create(
            image=_SANDBOX_IMAGE,
            cpus=_SANDBOX_CPUS,
            memory_mb=_SANDBOX_MEMORY_MB,
            ephemeral_disk_mb=_SANDBOX_DISK_MB,
            entrypoint=["sleep", "300"],
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

    def test_6_sandbox_transitions_to_terminated(self):
        self.assertIsNotNone(self.__class__.sandbox_id, "Depends on test_5")
        status = _poll_sandbox_status(
            _client, self.__class__.sandbox_id, SandboxStatus.TERMINATED, timeout=30
        )
        self.assertEqual(status, SandboxStatus.TERMINATED)

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
            entrypoint=["sleep", "300"],
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
        self.__class__.pool_id = None

    def test_6_delete_nonexistent_pool(self):
        with self.assertRaises(PoolNotFoundError):
            _client.delete_pool("nonexistent-pool-id-000")


# ---------------------------------------------------------------------------
# TestPoolWithSandboxes
# ---------------------------------------------------------------------------


class TestPoolWithSandboxes(unittest.TestCase):
    """Create a sandbox from a pool, verify it runs, delete sandbox
    then pool."""

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

    def test_1_create_pool(self):
        resp = _client.create_pool(
            image=_SANDBOX_IMAGE,
            cpus=_SANDBOX_CPUS,
            memory_mb=_SANDBOX_MEMORY_MB,
            ephemeral_disk_mb=_SANDBOX_DISK_MB,
            entrypoint=["sleep", "300"],
            warm_containers=1,
        )
        self.assertIsNotNone(resp.pool_id)
        self.__class__.pool_id = resp.pool_id

    def test_2_create_sandbox_from_pool(self):
        self.assertIsNotNone(self.__class__.pool_id, "Depends on test_1")
        resp = _client.claim(self.__class__.pool_id)
        self.assertIsNotNone(resp.sandbox_id)
        self.assertIn(resp.status, (SandboxStatus.PENDING, SandboxStatus.RUNNING))
        self.__class__.sandbox_id = resp.sandbox_id

    def test_3_sandbox_from_pool_reaches_running(self):
        self.assertIsNotNone(self.__class__.sandbox_id, "Depends on test_2")
        status = _poll_sandbox_status(
            _client, self.__class__.sandbox_id, SandboxStatus.RUNNING, timeout=60
        )
        self.assertEqual(status, SandboxStatus.RUNNING)

    def test_4_cannot_delete_pool_with_active_sandbox(self):
        """Pool deletion should fail while a sandbox is running."""
        self.assertIsNotNone(self.__class__.pool_id, "Depends on test_1")
        self.assertIsNotNone(self.__class__.sandbox_id, "Depends on test_2")
        with self.assertRaises(PoolInUseError):
            _client.delete_pool(self.__class__.pool_id)

    def test_5_delete_sandbox_then_pool(self):
        self.assertIsNotNone(self.__class__.sandbox_id, "Depends on test_2")
        self.assertIsNotNone(self.__class__.pool_id, "Depends on test_1")

        _client.delete(self.__class__.sandbox_id)
        _poll_sandbox_status(
            _client, self.__class__.sandbox_id, SandboxStatus.TERMINATED, timeout=30
        )
        self.__class__.sandbox_id = None

        _client.delete_pool(self.__class__.pool_id)
        self.__class__.pool_id = None


# ---------------------------------------------------------------------------
# TestWarmContainers
# ---------------------------------------------------------------------------


class TestWarmContainers(unittest.TestCase):
    """Verify warm container behaviour.

    1. Pool with warm_containers=1 creates exactly one idle container.
    2. Creating a sandbox claims the warm container (sandbox_id set on it).
    3. A replacement warm container is created to maintain the warm count.
    4. Deleting the sandbox terminates its container in the pool.
    """

    pool_id: str | None = None
    sandbox_id: str | None = None
    warm_container_id: str | None = None

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

    def test_1_create_pool_with_one_warm_container(self):
        resp = _client.create_pool(
            image=_SANDBOX_IMAGE,
            cpus=_SANDBOX_CPUS,
            memory_mb=_SANDBOX_MEMORY_MB,
            ephemeral_disk_mb=_SANDBOX_DISK_MB,
            entrypoint=["sleep", "300"],
            warm_containers=1,
        )
        self.assertIsNotNone(resp.pool_id)
        self.__class__.pool_id = resp.pool_id

    def test_2_warm_container_is_created(self):
        """Exactly one warm container should be spun up, unassigned."""
        self.assertIsNotNone(self.__class__.pool_id, "Depends on test_1")
        containers = _poll_pool_containers(
            _client, self.__class__.pool_id, min_count=1, timeout=60
        )
        self.assertEqual(len(containers), 1)
        self.assertIsNone(containers[0].sandbox_id)
        self.__class__.warm_container_id = containers[0].id

    def test_3_sandbox_claims_warm_container(self):
        """Creating a sandbox from the pool should claim the warm container."""
        self.assertIsNotNone(self.__class__.pool_id, "Depends on test_1")
        self.assertIsNotNone(self.__class__.warm_container_id, "Depends on test_2")

        resp = _client.claim(self.__class__.pool_id)
        self.assertIsNotNone(resp.sandbox_id)
        self.__class__.sandbox_id = resp.sandbox_id

        status = _poll_sandbox_status(
            _client, resp.sandbox_id, SandboxStatus.RUNNING, timeout=60
        )
        self.assertEqual(status, SandboxStatus.RUNNING)

        # The original warm container should now have sandbox_id set.
        detail = _client.get_pool(self.__class__.pool_id)
        claimed = [
            c
            for c in (detail.containers or [])
            if c.id == self.__class__.warm_container_id
        ]
        self.assertEqual(len(claimed), 1, "Warm container should still exist")
        self.assertEqual(
            claimed[0].sandbox_id,
            self.__class__.sandbox_id,
            "Warm container should be claimed by the sandbox",
        )

    def test_4_replacement_warm_container_is_created(self):
        """Buffer reconciler should create a new warm container to
        maintain warm_containers=1 after one was claimed."""
        self.assertIsNotNone(self.__class__.pool_id, "Depends on test_1")
        self.assertIsNotNone(self.__class__.warm_container_id, "Depends on test_2")

        # Pool should now have 2 containers: 1 claimed + 1 new warm.
        containers = _poll_pool_containers(
            _client, self.__class__.pool_id, min_count=2, timeout=60
        )
        warm = _warm_containers(containers)
        self.assertGreaterEqual(
            len(warm), 1, "A replacement warm container should be created"
        )
        self.assertNotEqual(warm[0].id, self.__class__.warm_container_id)

    def test_5_delete_sandbox_removes_claimed_container(self):
        """Deleting the sandbox should terminate its claimed container."""
        self.assertIsNotNone(self.__class__.sandbox_id, "Depends on test_3")
        self.assertIsNotNone(self.__class__.pool_id, "Depends on test_1")

        _client.delete(self.__class__.sandbox_id)
        _poll_sandbox_status(
            _client, self.__class__.sandbox_id, SandboxStatus.TERMINATED, timeout=30
        )
        self.__class__.sandbox_id = None

        # Wait for the claimed container to be cleaned up. The pool should
        # converge back to exactly 1 warm container (warm_containers=1).
        deadline = time.time() + 30
        while time.time() < deadline:
            containers = _get_pool_containers(_client, self.__class__.pool_id)
            warm = _warm_containers(containers)
            claimed = _claimed_containers(containers)
            if len(claimed) == 0 and len(warm) == 1:
                break
            time.sleep(1)

        containers = _get_pool_containers(_client, self.__class__.pool_id)
        self.assertEqual(
            len(_claimed_containers(containers)),
            0,
            "Claimed container should be terminated after sandbox deletion",
        )
        self.assertEqual(
            len(_warm_containers(containers)),
            1,
            "Pool should have exactly 1 warm container after sandbox deletion",
        )

    def test_6_cleanup(self):
        if self.__class__.sandbox_id:
            _client.delete(self.__class__.sandbox_id)
            self.__class__.sandbox_id = None
        if self.__class__.pool_id:
            time.sleep(2)
            _client.delete_pool(self.__class__.pool_id)
            self.__class__.pool_id = None


# ---------------------------------------------------------------------------
# TestMaxContainers
# ---------------------------------------------------------------------------


class TestMaxContainers(unittest.TestCase):
    """Verify max_containers is respected.

    Create a pool with warm_containers=1 and max_containers=2.
    Create two sandboxes (1 claims warm, 1 creates on-demand = 2 total).
    The buffer reconciler should NOT create a replacement warm container
    because we are at max.
    """

    pool_id: str | None = None
    sandbox_ids: list[str]

    @classmethod
    def setUpClass(cls):
        cls.sandbox_ids = []

    @classmethod
    def tearDownClass(cls):
        for sid in cls.sandbox_ids:
            try:
                _client.delete(sid)
            except Exception:
                pass
        if cls.pool_id and _client:
            try:
                # Wait for sandbox termination before deleting pool.
                time.sleep(3)
                _client.delete_pool(cls.pool_id)
            except Exception:
                pass

    def test_1_create_pool_with_max(self):
        resp = _client.create_pool(
            image=_SANDBOX_IMAGE,
            cpus=_SANDBOX_CPUS,
            memory_mb=_SANDBOX_MEMORY_MB,
            ephemeral_disk_mb=_SANDBOX_DISK_MB,
            entrypoint=["sleep", "300"],
            warm_containers=1,
            max_containers=2,
        )
        self.assertIsNotNone(resp.pool_id)
        self.__class__.pool_id = resp.pool_id

    def test_2_warm_container_created(self):
        self.assertIsNotNone(self.__class__.pool_id, "Depends on test_1")
        containers = _poll_pool_containers(
            _client, self.__class__.pool_id, min_count=1, timeout=60
        )
        self.assertEqual(len(containers), 1)
        self.assertIsNone(containers[0].sandbox_id)

    def test_3_create_two_sandboxes(self):
        """First sandbox claims the warm container, second claims
        the replacement warm. Both should reach Running."""
        self.assertIsNotNone(self.__class__.pool_id, "Depends on test_1")

        # Sandbox 1 claims the initial warm container.
        resp1 = _client.claim(self.__class__.pool_id)
        self.assertIsNotNone(resp1.sandbox_id)
        self.__class__.sandbox_ids.append(resp1.sandbox_id)
        status = _poll_sandbox_status(
            _client, resp1.sandbox_id, SandboxStatus.RUNNING, timeout=60
        )
        self.assertEqual(status, SandboxStatus.RUNNING)

        # Wait for the reconciler to create a replacement warm container
        # before creating sandbox 2, so it can claim the warm rather than
        # needing an on-demand container.
        _poll_pool_containers(_client, self.__class__.pool_id, min_count=2, timeout=60)

        # Sandbox 2 claims the replacement warm container.
        resp2 = _client.claim(self.__class__.pool_id)
        self.assertIsNotNone(resp2.sandbox_id)
        self.__class__.sandbox_ids.append(resp2.sandbox_id)
        status = _poll_sandbox_status(
            _client, resp2.sandbox_id, SandboxStatus.RUNNING, timeout=60
        )
        self.assertEqual(status, SandboxStatus.RUNNING)

    def test_4_no_warm_replacement_at_max(self):
        """At max_containers=2 with 2 claimed, no replacement warm
        container should be created."""
        self.assertIsNotNone(self.__class__.pool_id, "Depends on test_1")

        # Give the reconciler several cycles to stabilize.
        time.sleep(5)

        containers = _get_pool_containers(_client, self.__class__.pool_id)
        self.assertEqual(
            len(containers),
            2,
            f"Pool should have exactly 2 containers (max), got {len(containers)}",
        )
        warm = _warm_containers(containers)
        self.assertEqual(
            len(warm),
            0,
            "No warm containers should exist when at max capacity",
        )

    def test_5_cleanup(self):
        for sid in list(self.__class__.sandbox_ids):
            _client.delete(sid)
        self.__class__.sandbox_ids.clear()
        if self.__class__.pool_id:
            # Wait for sandbox containers to terminate.
            time.sleep(3)
            _client.delete_pool(self.__class__.pool_id)
            self.__class__.pool_id = None


# ---------------------------------------------------------------------------
# TestPoolDeletion
# ---------------------------------------------------------------------------


class TestPoolDeletion(unittest.TestCase):
    """Verify that deleting a pool cleans up its warm containers."""

    pool_id: str | None = None

    @classmethod
    def tearDownClass(cls):
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
            entrypoint=["sleep", "300"],
            warm_containers=2,
        )
        self.assertIsNotNone(resp.pool_id)
        self.__class__.pool_id = resp.pool_id

    def test_2_warm_containers_exist(self):
        self.assertIsNotNone(self.__class__.pool_id, "Depends on test_1")
        containers = _poll_pool_containers(
            _client, self.__class__.pool_id, min_count=2, timeout=60
        )
        self.assertEqual(len(containers), 2)
        for c in containers:
            self.assertIsNone(c.sandbox_id)

    def test_3_delete_pool_cleans_up(self):
        """Deleting the pool should succeed and the pool should no longer
        exist."""
        self.assertIsNotNone(self.__class__.pool_id, "Depends on test_1")

        _client.delete_pool(self.__class__.pool_id)

        with self.assertRaises(PoolNotFoundError):
            _client.get_pool(self.__class__.pool_id)

        self.__class__.pool_id = None


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    # Run simple tests first, complex tests last.
    for cls in [
        TestSandboxLifecycle,
        TestPoolLifecycle,
        TestPoolWithSandboxes,
        TestWarmContainers,
        TestMaxContainers,
        TestPoolDeletion,
    ]:
        suite.addTests(loader.loadTestsFromTestCase(cls))
    runner = unittest.TextTestRunner(verbosity=2 if "-v" in sys.argv else 1)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
