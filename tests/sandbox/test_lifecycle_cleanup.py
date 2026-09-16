"""Pool cleanup waits only for the expected asynchronous deletion conflict."""

import unittest
from unittest.mock import MagicMock, patch

from tensorlake.sandbox import PoolInUseError, RemoteAPIError
from tests.sandbox.test_lifecycle import _delete_pool_after_sandbox_cleanup


class TestLifecycleCleanup(unittest.TestCase):
    def test_retries_pool_in_use_after_accepted_sandbox_deletion(self) -> None:
        client = MagicMock()
        client.delete_pool.side_effect = [PoolInUseError("pool", "still active"), None]
        with patch("tests.sandbox.test_lifecycle.time.sleep") as sleep:
            _delete_pool_after_sandbox_cleanup(client, "pool")
        self.assertEqual(client.delete_pool.call_count, 2)
        sleep.assert_called_once()

    def test_conflict_is_reported_when_cleanup_deadline_expires(self) -> None:
        client = MagicMock()
        client.delete_pool.side_effect = PoolInUseError("pool", "still active")
        with patch("tests.sandbox.test_lifecycle.time.monotonic", side_effect=[0, 30]):
            with self.assertRaises(PoolInUseError):
                _delete_pool_after_sandbox_cleanup(client, "pool")
        client.delete_pool.assert_called_once_with("pool")

    def test_unrelated_errors_are_not_retried(self) -> None:
        client = MagicMock()
        client.delete_pool.side_effect = RemoteAPIError(403, "forbidden")
        with self.assertRaises(RemoteAPIError):
            _delete_pool_after_sandbox_cleanup(client, "pool")
        client.delete_pool.assert_called_once_with("pool")
