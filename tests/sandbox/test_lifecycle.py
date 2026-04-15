"""Integration tests for sandbox lifecycle management APIs.

Runs against the Tensorlake cloud API (or the URL in TENSORLAKE_API_URL).
Requires TENSORLAKE_API_KEY to be set.

Usage:
    TENSORLAKE_API_KEY=... poetry run python tests/sandbox/test_lifecycle.py
    TENSORLAKE_API_URL=https://api.tensorlake.ai TENSORLAKE_API_KEY=... poetry run python tests/sandbox/test_lifecycle.py
"""

import os
import time
import unittest

from tensorlake.sandbox import (
    PoolContainerInfo,
    PoolInUseError,
    PoolNotFoundError,
    Sandbox,
    SandboxClient,
    SandboxNotFoundError,
    SandboxStatus,
)

_SANDBOX_IMAGE = "tensorlake/ubuntu-minimal"
_SANDBOX_CPUS = 1.0
_SANDBOX_MEMORY_MB = 1024
_SANDBOX_DISK_MB = 1024


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
# Base Test Class
# ---------------------------------------------------------------------------


class BaseSandboxTest(unittest.TestCase):
    client: SandboxClient

    @classmethod
    def setUpClass(cls):
        api_url = os.environ.get("TENSORLAKE_API_URL", "https://api.tensorlake.ai")
        cls.client = SandboxClient(api_url=api_url)

    @classmethod
    def tearDownClass(cls):
        cls.client.close()


# ---------------------------------------------------------------------------
# TestSandboxLifecycle
# ---------------------------------------------------------------------------


class TestSandboxLifecycle(BaseSandboxTest):
    """Create a sandbox, verify it transitions to Running, delete it,
    verify it transitions to Terminated."""

    sandbox_id: str | None = None

    @classmethod
    def tearDownClass(cls):
        if cls.sandbox_id:
            try:
                cls.client.delete(cls.sandbox_id)
            except Exception:
                pass
        super().tearDownClass()

    def test_1_create_sandbox(self):
        resp = self.client.create(
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
        info = self.client.get(self.__class__.sandbox_id)
        self.assertEqual(info.sandbox_id, self.__class__.sandbox_id)
        self.assertIn(info.status, (SandboxStatus.PENDING, SandboxStatus.RUNNING))

    def test_3_list_sandboxes(self):
        self.assertIsNotNone(self.__class__.sandbox_id, "Depends on test_1")
        sandboxes = self.client.list()
        ids = [s.sandbox_id for s in sandboxes]
        self.assertIn(self.__class__.sandbox_id, ids)

    def test_4_sandbox_transitions_to_running(self):
        self.assertIsNotNone(self.__class__.sandbox_id, "Depends on test_1")
        status = _poll_sandbox_status(
            self.client, self.__class__.sandbox_id, SandboxStatus.RUNNING, timeout=60
        )
        self.assertEqual(status, SandboxStatus.RUNNING)

    def test_5_delete_sandbox(self):
        self.assertIsNotNone(self.__class__.sandbox_id, "Depends on test_1")
        self.client.delete(self.__class__.sandbox_id)

    def test_6_sandbox_transitions_to_terminated(self):
        self.assertIsNotNone(self.__class__.sandbox_id, "Depends on test_5")
        status = _poll_sandbox_status(
            self.client, self.__class__.sandbox_id, SandboxStatus.TERMINATED, timeout=30
        )
        self.assertEqual(status, SandboxStatus.TERMINATED)

    def test_7_delete_nonexistent_sandbox(self):
        with self.assertRaises(SandboxNotFoundError):
            self.client.delete("nonexistent-sandbox-id-000")


# ---------------------------------------------------------------------------
# TestPoolLifecycle
# ---------------------------------------------------------------------------


class TestPoolLifecycle(BaseSandboxTest):
    """CRUD lifecycle for sandbox pools."""

    pool_id: str | None = None

    @classmethod
    def tearDownClass(cls):
        if cls.pool_id:
            try:
                cls.client.delete_pool(cls.pool_id)
            except Exception:
                pass
        super().tearDownClass()

    def test_1_create_pool(self):
        resp = self.client.create_pool(
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
        info = self.client.get_pool(self.__class__.pool_id)
        self.assertEqual(info.pool_id, self.__class__.pool_id)
        self.assertEqual(info.image, _SANDBOX_IMAGE)
        self.assertAlmostEqual(info.resources.cpus, _SANDBOX_CPUS, places=2)
        self.assertEqual(info.resources.memory_mb, _SANDBOX_MEMORY_MB)

    def test_3_list_pools(self):
        self.assertIsNotNone(self.__class__.pool_id, "Depends on test_1")
        pools = self.client.list_pools()
        ids = [p.pool_id for p in pools]
        self.assertIn(self.__class__.pool_id, ids)

    def test_4_update_pool(self):
        self.assertIsNotNone(self.__class__.pool_id, "Depends on test_1")
        updated = self.client.update_pool(
            pool_id=self.__class__.pool_id,
            image=_SANDBOX_IMAGE,
            cpus=_SANDBOX_CPUS,
            memory_mb=768,
            ephemeral_disk_mb=_SANDBOX_DISK_MB,
            warm_containers=1,
        )
        self.assertEqual(updated.resources.memory_mb, 768)
        self.assertEqual(updated.warm_containers, 1)

    def test_5_delete_pool(self):
        self.assertIsNotNone(self.__class__.pool_id, "Depends on test_1")
        self.client.delete_pool(self.__class__.pool_id)
        self.__class__.pool_id = None

    def test_6_delete_nonexistent_pool(self):
        with self.assertRaises(PoolNotFoundError):
            self.client.delete_pool("nonexistent-pool-id-000")


# ---------------------------------------------------------------------------
# TestPoolWithSandboxes
# ---------------------------------------------------------------------------


class TestPoolWithSandboxes(BaseSandboxTest):
    """Create a sandbox from a pool, verify it runs, delete sandbox
    then pool."""

    pool_id: str | None = None
    sandbox_id: str | None = None

    @classmethod
    def tearDownClass(cls):
        if cls.sandbox_id:
            try:
                cls.client.delete(cls.sandbox_id)
            except Exception:
                pass
        if cls.pool_id:
            try:
                cls.client.delete_pool(cls.pool_id)
            except Exception:
                pass
        super().tearDownClass()

    def test_1_create_pool(self):
        resp = self.client.create_pool(
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
        resp = self.client.claim(self.__class__.pool_id)
        self.assertIsNotNone(resp.sandbox_id)
        self.assertIn(resp.status, (SandboxStatus.PENDING, SandboxStatus.RUNNING))
        self.__class__.sandbox_id = resp.sandbox_id

    def test_3_sandbox_from_pool_reaches_running(self):
        self.assertIsNotNone(self.__class__.sandbox_id, "Depends on test_2")
        status = _poll_sandbox_status(
            self.client, self.__class__.sandbox_id, SandboxStatus.RUNNING, timeout=60
        )
        self.assertEqual(status, SandboxStatus.RUNNING)

    def test_4_cannot_delete_pool_with_active_sandbox(self):
        """Pool deletion should fail while a sandbox is running."""
        self.assertIsNotNone(self.__class__.pool_id, "Depends on test_1")
        self.assertIsNotNone(self.__class__.sandbox_id, "Depends on test_2")
        with self.assertRaises(PoolInUseError):
            self.client.delete_pool(self.__class__.pool_id)

    def test_5_delete_sandbox_then_pool(self):
        self.assertIsNotNone(self.__class__.sandbox_id, "Depends on test_2")
        self.assertIsNotNone(self.__class__.pool_id, "Depends on test_1")

        self.client.delete(self.__class__.sandbox_id)
        _poll_sandbox_status(
            self.client, self.__class__.sandbox_id, SandboxStatus.TERMINATED, timeout=30
        )
        self.__class__.sandbox_id = None

        self.client.delete_pool(self.__class__.pool_id)
        self.__class__.pool_id = None


# ---------------------------------------------------------------------------
# TestWarmContainers
# ---------------------------------------------------------------------------


class TestWarmContainers(BaseSandboxTest):
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
        if cls.sandbox_id:
            try:
                cls.client.delete(cls.sandbox_id)
            except Exception:
                pass
        if cls.pool_id:
            try:
                cls.client.delete_pool(cls.pool_id)
            except Exception:
                pass
        super().tearDownClass()

    def test_1_create_pool_with_one_warm_container(self):
        resp = self.client.create_pool(
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
            self.client, self.__class__.pool_id, min_count=1, timeout=60
        )
        self.assertEqual(len(containers), 1)
        self.assertIsNone(containers[0].sandbox_id)
        self.__class__.warm_container_id = containers[0].id

    def test_3_sandbox_claims_warm_container(self):
        """Creating a sandbox from the pool should claim the warm container."""
        self.assertIsNotNone(self.__class__.pool_id, "Depends on test_1")
        self.assertIsNotNone(self.__class__.warm_container_id, "Depends on test_2")

        resp = self.client.claim(self.__class__.pool_id)
        self.assertIsNotNone(resp.sandbox_id)
        self.__class__.sandbox_id = resp.sandbox_id

        status = _poll_sandbox_status(
            self.client, resp.sandbox_id, SandboxStatus.RUNNING, timeout=60
        )
        self.assertEqual(status, SandboxStatus.RUNNING)

        # The original warm container should now have sandbox_id set.
        detail = self.client.get_pool(self.__class__.pool_id)
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
            self.client, self.__class__.pool_id, min_count=2, timeout=60
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

        self.client.delete(self.__class__.sandbox_id)
        _poll_sandbox_status(
            self.client, self.__class__.sandbox_id, SandboxStatus.TERMINATED, timeout=30
        )
        self.__class__.sandbox_id = None

        # Wait for the claimed container to be cleaned up. The pool should
        # converge back to exactly 1 warm container (warm_containers=1).
        deadline = time.time() + 30
        while time.time() < deadline:
            containers = _get_pool_containers(self.client, self.__class__.pool_id)
            warm = _warm_containers(containers)
            claimed = _claimed_containers(containers)
            if len(claimed) == 0 and len(warm) == 1:
                break
            time.sleep(1)

        containers = _get_pool_containers(self.client, self.__class__.pool_id)
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
            self.client.delete(self.__class__.sandbox_id)
            self.__class__.sandbox_id = None
        if self.__class__.pool_id:
            time.sleep(2)
            self.client.delete_pool(self.__class__.pool_id)
            self.__class__.pool_id = None


# ---------------------------------------------------------------------------
# TestMaxContainers
# ---------------------------------------------------------------------------


class TestMaxContainers(BaseSandboxTest):
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
        super().setUpClass()
        cls.sandbox_ids = []

    @classmethod
    def tearDownClass(cls):
        for sid in cls.sandbox_ids:
            try:
                cls.client.delete(sid)
            except Exception:
                pass
        if cls.pool_id:
            try:
                # Wait for sandbox termination before deleting pool.
                time.sleep(3)
                cls.client.delete_pool(cls.pool_id)
            except Exception:
                pass
        super().tearDownClass()

    def test_1_create_pool_with_max(self):
        resp = self.client.create_pool(
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
            self.client, self.__class__.pool_id, min_count=1, timeout=60
        )
        self.assertEqual(len(containers), 1)
        self.assertIsNone(containers[0].sandbox_id)

    def test_3_create_two_sandboxes(self):
        """First sandbox claims the warm container, second claims
        the replacement warm. Both should reach Running."""
        self.assertIsNotNone(self.__class__.pool_id, "Depends on test_1")

        # Sandbox 1 claims the initial warm container.
        resp1 = self.client.claim(self.__class__.pool_id)
        self.assertIsNotNone(resp1.sandbox_id)
        self.__class__.sandbox_ids.append(resp1.sandbox_id)
        status = _poll_sandbox_status(
            self.client, resp1.sandbox_id, SandboxStatus.RUNNING, timeout=60
        )
        self.assertEqual(status, SandboxStatus.RUNNING)

        # Wait for the reconciler to create a replacement warm container
        # before creating sandbox 2, so it can claim the warm rather than
        # needing an on-demand container.
        _poll_pool_containers(
            self.client, self.__class__.pool_id, min_count=2, timeout=60
        )

        # Sandbox 2 claims the replacement warm container.
        resp2 = self.client.claim(self.__class__.pool_id)
        self.assertIsNotNone(resp2.sandbox_id)
        self.__class__.sandbox_ids.append(resp2.sandbox_id)
        status = _poll_sandbox_status(
            self.client, resp2.sandbox_id, SandboxStatus.RUNNING, timeout=60
        )
        self.assertEqual(status, SandboxStatus.RUNNING)

    def test_4_no_warm_replacement_at_max(self):
        """At max_containers=2 with 2 claimed, no replacement warm
        container should be created."""
        self.assertIsNotNone(self.__class__.pool_id, "Depends on test_1")

        # Give the reconciler several cycles to stabilize.
        time.sleep(5)

        containers = _get_pool_containers(self.client, self.__class__.pool_id)
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
            self.client.delete(sid)
        self.__class__.sandbox_ids.clear()
        if self.__class__.pool_id:
            # Wait for sandbox containers to terminate.
            time.sleep(3)
            self.client.delete_pool(self.__class__.pool_id)
            self.__class__.pool_id = None


# ---------------------------------------------------------------------------
# TestPoolDeletion
# ---------------------------------------------------------------------------


class TestPoolDeletion(BaseSandboxTest):
    """Verify that deleting a pool cleans up its warm containers."""

    pool_id: str | None = None

    @classmethod
    def tearDownClass(cls):
        if cls.pool_id:
            try:
                cls.client.delete_pool(cls.pool_id)
            except Exception:
                pass
        super().tearDownClass()

    def test_1_create_pool_with_warm_containers(self):
        resp = self.client.create_pool(
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
            self.client, self.__class__.pool_id, min_count=2, timeout=60
        )
        self.assertEqual(len(containers), 2)
        for c in containers:
            self.assertIsNone(c.sandbox_id)

    def test_3_delete_pool_cleans_up(self):
        """Deleting the pool should succeed and the pool should no longer
        exist."""
        self.assertIsNotNone(self.__class__.pool_id, "Depends on test_1")

        self.client.delete_pool(self.__class__.pool_id)

        with self.assertRaises(PoolNotFoundError):
            self.client.get_pool(self.__class__.pool_id)

        self.__class__.pool_id = None


# ---------------------------------------------------------------------------
# TestSandboxTimeout
# ---------------------------------------------------------------------------


class TestSandboxTimeout(BaseSandboxTest):
    """Verify that a sandbox claimed from a pool with a short timeout
    is automatically suspended after the timeout elapses.

    1. Create a pool with timeout_secs=30 and warm_containers=1.
    2. Wait for the warm container, then claim a sandbox.
    3. Verify the sandbox reaches Running.
    4. Wait 35 seconds for the timeout to expire.
    5. Verify the sandbox transitions to Suspended.
    """

    pool_id: str | None = None
    sandbox_id: str | None = None

    @classmethod
    def tearDownClass(cls):
        if cls.sandbox_id:
            try:
                cls.client.delete(cls.sandbox_id)
            except Exception:
                pass
        if cls.pool_id:
            try:
                cls.client.delete_pool(cls.pool_id)
            except Exception:
                pass
        super().tearDownClass()

    def test_1_create_pool_with_timeout(self):
        resp = self.client.create_pool(
            image=_SANDBOX_IMAGE,
            cpus=_SANDBOX_CPUS,
            memory_mb=_SANDBOX_MEMORY_MB,
            ephemeral_disk_mb=_SANDBOX_DISK_MB,
            entrypoint=["sleep", "300"],
            warm_containers=1,
            timeout_secs=30,
        )
        self.assertIsNotNone(resp.pool_id)
        self.__class__.pool_id = resp.pool_id

    def test_2_claim_sandbox_from_pool(self):
        """Wait for the warm container and claim a sandbox from the pool."""
        self.assertIsNotNone(self.__class__.pool_id, "Depends on test_1")

        _poll_pool_containers(
            self.client, self.__class__.pool_id, min_count=1, timeout=60
        )

        resp = self.client.claim(self.__class__.pool_id)
        self.assertIsNotNone(resp.sandbox_id)
        self.__class__.sandbox_id = resp.sandbox_id

    def test_3_sandbox_reaches_running(self):
        """The claimed sandbox should transition to Running."""
        self.assertIsNotNone(self.__class__.sandbox_id, "Depends on test_2")
        status = _poll_sandbox_status(
            self.client, self.__class__.sandbox_id, SandboxStatus.RUNNING, timeout=60
        )
        self.assertEqual(status, SandboxStatus.RUNNING)

    def test_4_sandbox_suspended_after_timeout(self):
        """Wait beyond the 30s timeout and verify the sandbox is suspended
        without a terminal outcome."""
        self.assertIsNotNone(self.__class__.sandbox_id, "Depends on test_3")

        # Wait long enough for the 30s timeout to expire.
        time.sleep(35)

        status = _poll_sandbox_status(
            self.client,
            self.__class__.sandbox_id,
            SandboxStatus.SUSPENDED,
            timeout=30,
        )
        self.assertEqual(status, SandboxStatus.SUSPENDED)

        info = self.client.get(self.__class__.sandbox_id)
        self.assertIsNone(
            info.outcome,
            "Suspended sandboxes should not report a terminal outcome",
        )

    def test_5_cleanup(self):
        if self.__class__.sandbox_id:
            self.client.delete(self.__class__.sandbox_id)
            self.__class__.sandbox_id = None
        if self.__class__.pool_id:
            time.sleep(2)
            self.client.delete_pool(self.__class__.pool_id)
            self.__class__.pool_id = None


# ---------------------------------------------------------------------------
# TestNamedSandboxIdentifier
# ---------------------------------------------------------------------------


class TestNamedSandboxIdentifier(BaseSandboxTest):
    """Verify that sandbox identifier and sandbox_id are distinct and that
    sandbox_id always carries the server-assigned UUID, never a human-readable
    name.

    Some tests below are expected to fail until both server and SDK changes are
    deployed (specifically test_4_connect_by_name_sandbox_id_is_uuid, which
    requires the SDK to resolve names to UUIDs on connect).
    """

    sandbox_id: str | None = None
    sandbox_name: str = "tl-integ-named-sbx"

    @classmethod
    def tearDownClass(cls):
        if cls.sandbox_id:
            try:
                cls.client.delete(cls.sandbox_id)
            except Exception:
                pass
        super().tearDownClass()

    def test_1_create_named_sandbox_returns_uuid(self):
        """create() with a name must return a sandbox_id that is the UUID,
        not the human-readable name."""
        resp = self.client.create(
            image=_SANDBOX_IMAGE,
            cpus=_SANDBOX_CPUS,
            memory_mb=_SANDBOX_MEMORY_MB,
            ephemeral_disk_mb=_SANDBOX_DISK_MB,
            entrypoint=["sleep", "300"],
            name=self.__class__.sandbox_name,
        )
        self.assertIsNotNone(resp.sandbox_id)
        self.assertNotEqual(
            resp.sandbox_id,
            self.__class__.sandbox_name,
            "sandbox_id returned by create() must be the UUID, not the name",
        )
        self.__class__.sandbox_id = resp.sandbox_id

    def test_2_get_by_name_returns_uuid(self):
        """get() with a name must return a SandboxInfo whose sandbox_id is
        the UUID, not the name, and whose name field matches the given name."""
        self.assertIsNotNone(self.__class__.sandbox_id, "Depends on test_1")
        info = self.client.get(self.__class__.sandbox_name)
        self.assertEqual(
            info.sandbox_id,
            self.__class__.sandbox_id,
            "sandbox_id from get(name) must equal the UUID returned by create()",
        )
        self.assertNotEqual(
            info.sandbox_id,
            self.__class__.sandbox_name,
            "sandbox_id must never equal the sandbox name",
        )
        self.assertEqual(
            info.name,
            self.__class__.sandbox_name,
            "name field must carry the human-readable name",
        )

    def test_3_connect_by_uuid_sandbox_id_matches(self):
        """Connecting by the UUID must yield a Sandbox whose sandbox_id equals
        that UUID."""
        self.assertIsNotNone(self.__class__.sandbox_id, "Depends on test_1")
        sandbox = self.client.connect(identifier=self.__class__.sandbox_id)
        try:
            self.assertEqual(
                sandbox.sandbox_id,
                self.__class__.sandbox_id,
                "sandbox_id must equal the UUID used to connect",
            )
        finally:
            sandbox.close()

    def test_4_connect_by_name_sandbox_id_is_uuid(self):
        """Connecting by name must yield a Sandbox whose sandbox_id is the
        server-assigned UUID, not the name.

        NOTE: This test is expected to fail until the SDK resolves sandbox
        names to UUIDs during connect(). It documents the target behaviour.
        """
        self.assertIsNotNone(self.__class__.sandbox_id, "Depends on test_1")
        sandbox = self.client.connect(identifier=self.__class__.sandbox_name)
        try:
            self.assertNotEqual(
                sandbox.sandbox_id,
                self.__class__.sandbox_name,
                "sandbox_id must be the UUID, not the name",
            )
            self.assertEqual(
                sandbox.sandbox_id,
                self.__class__.sandbox_id,
                "sandbox_id must equal the UUID returned by create()",
            )
        finally:
            sandbox.close()

    def test_5_cleanup(self):
        if self.__class__.sandbox_id:
            self.client.delete(self.__class__.sandbox_id)
            self.__class__.sandbox_id = None


# ---------------------------------------------------------------------------
# TestSandboxRun
# ---------------------------------------------------------------------------


class TestSandboxRun(BaseSandboxTest):
    """Integration tests for Sandbox.run() — the streaming process execution endpoint.

    A single Alpine sandbox is created for the whole class and shared across
    all test methods.  Each test is independent (no ordering dependencies).
    """

    sandbox_id: str | None = None
    sandbox: Sandbox | None = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        resp = cls.client.create(
            image=_SANDBOX_IMAGE,
            cpus=_SANDBOX_CPUS,
            memory_mb=_SANDBOX_MEMORY_MB,
            ephemeral_disk_mb=_SANDBOX_DISK_MB,
            entrypoint=["sleep", "300"],
        )
        cls.sandbox_id = resp.sandbox_id
        _poll_sandbox_status(
            cls.client, cls.sandbox_id, SandboxStatus.RUNNING, timeout=60
        )
        cls.sandbox = cls.client.connect(identifier=cls.sandbox_id)

    @classmethod
    def tearDownClass(cls):
        if cls.sandbox is not None:
            cls.sandbox.close()
            cls.sandbox = None
        if cls.sandbox_id:
            try:
                cls.client.delete(cls.sandbox_id)
            except Exception:
                pass
        super().tearDownClass()

    def test_captures_stdout(self):
        result = self.sandbox.run("echo", args=["hello world"])
        self.assertEqual(result.exit_code, 0, f"sandbox {self.sandbox_id}: exit_code")
        self.assertIn(
            "hello world", result.stdout, f"sandbox {self.sandbox_id}: stdout"
        )

    def test_captures_stderr(self):
        result = self.sandbox.run("sh", args=["-c", "echo error-output >&2"])
        self.assertEqual(result.exit_code, 0, f"sandbox {self.sandbox_id}: exit_code")
        self.assertIn(
            "error-output", result.stderr, f"sandbox {self.sandbox_id}: stderr"
        )
        self.assertEqual(
            result.stdout, "", f"sandbox {self.sandbox_id}: stdout should be empty"
        )

    def test_nonzero_exit_code(self):
        result = self.sandbox.run("sh", args=["-c", "exit 42"])
        self.assertEqual(result.exit_code, 42, f"sandbox {self.sandbox_id}: exit_code")

    def test_env_vars(self):
        result = self.sandbox.run(
            "sh",
            args=["-c", "echo $MY_VAR"],
            env={"MY_VAR": "streaming-test-value"},
        )
        self.assertEqual(result.exit_code, 0, f"sandbox {self.sandbox_id}: exit_code")
        self.assertIn(
            "streaming-test-value",
            result.stdout,
            f"sandbox {self.sandbox_id}: stdout",
        )

    def test_working_directory(self):
        result = self.sandbox.run("pwd", working_dir="/tmp")
        self.assertEqual(result.exit_code, 0, f"sandbox {self.sandbox_id}: exit_code")
        self.assertIn("/tmp", result.stdout, f"sandbox {self.sandbox_id}: stdout")

    def test_multiline_output(self):
        result = self.sandbox.run("sh", args=["-c", "printf 'a\\nb\\nc\\n'"])
        self.assertEqual(result.exit_code, 0, f"sandbox {self.sandbox_id}: exit_code")
        lines = result.stdout.splitlines()
        self.assertEqual(
            lines, ["a", "b", "c"], f"sandbox {self.sandbox_id}: stdout lines"
        )

    def test_stdout_and_stderr_independent(self):
        """Lines written to stdout and stderr must be routed to the correct field."""
        result = self.sandbox.run(
            "sh", args=["-c", "echo out-line; echo err-line >&2; echo out-line2"]
        )
        self.assertEqual(result.exit_code, 0, f"sandbox {self.sandbox_id}: exit_code")
        self.assertIn("out-line", result.stdout, f"sandbox {self.sandbox_id}: stdout")
        self.assertIn("out-line2", result.stdout, f"sandbox {self.sandbox_id}: stdout")
        self.assertIn("err-line", result.stderr, f"sandbox {self.sandbox_id}: stderr")
        self.assertNotIn(
            "err-line",
            result.stdout,
            f"sandbox {self.sandbox_id}: stdout should not contain stderr",
        )


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
        TestSandboxTimeout,
        TestNamedSandboxIdentifier,
        TestSandboxRun,
    ]:
        suite.addTests(loader.loadTestsFromTestCase(cls))
    runner = unittest.TextTestRunner(verbosity=2 if "-v" in sys.argv else 1)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
