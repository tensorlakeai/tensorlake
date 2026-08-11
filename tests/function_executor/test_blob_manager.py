import threading
import unittest
from unittest.mock import patch

from tensorlake.applications.internal_logger import InternalLogger
from tensorlake.function_executor.allocation_runner.allocation_state_wrapper import (
    AllocationStateWrapper,
)
from tensorlake.function_executor.allocation_runner.blob_manager import (
    AllocationBLOBManager,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    AllocationOutputBlob,
)
from tensorlake.function_executor.proto.status_pb2 import Status


class AllocationBLOBManagerTest(unittest.TestCase):
    def _manager(self) -> tuple[AllocationStateWrapper, AllocationBLOBManager]:
        state = AllocationStateWrapper()
        return state, AllocationBLOBManager(
            allocation_state=state,
            logger=InternalLogger(
                context={},
                destination=InternalLogger.LOG_FILE.NULL,
                as_cloud_event=False,
            ),
        )

    def test_blobless_error_response_unblocks_oldest_pending_request(self) -> None:
        state, manager = self._manager()
        initial_state = state.wait_for_update(last_seen_hash=None)
        self.assertIsNotNone(initial_state)
        errors: list[BaseException] = []

        def request_blob() -> None:
            try:
                manager.get_new_output_blob(size=10)
            except BaseException as error:
                errors.append(error)

        requester = threading.Thread(target=request_blob, daemon=True)
        requester.start()
        pending_state = state.wait_for_update(last_seen_hash=initial_state.sha256_hash)
        self.assertEqual(len(pending_state.output_blob_requests), 1)

        manager.deliver_output_blob(
            AllocationOutputBlob(
                status=Status(code=13, message="output BLOB creation failed")
            )
        )
        requester.join(timeout=1)

        self.assertFalse(requester.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], RuntimeError)
        completed_state = state.wait_for_update(
            last_seen_hash=pending_state.sha256_hash
        )
        self.assertEqual(len(completed_state.output_blob_requests), 0)

    def test_back_to_back_blobless_errors_unblock_distinct_requests(self) -> None:
        state, manager = self._manager()
        current_state = state.wait_for_update(last_seen_hash=None)
        self.assertIsNotNone(current_state)
        errors: list[BaseException] = []

        def request_blob() -> None:
            try:
                manager.get_new_output_blob(size=10)
            except BaseException as error:
                errors.append(error)

        requesters = [
            threading.Thread(target=request_blob, daemon=True) for _ in range(2)
        ]
        for requester in requesters:
            requester.start()
        while len(current_state.output_blob_requests) < 2:
            current_state = state.wait_for_update(
                last_seen_hash=current_state.sha256_hash
            )
            self.assertIsNotNone(current_state)

        failure = AllocationOutputBlob(
            status=Status(code=13, message="output BLOB creation failed")
        )
        manager.deliver_output_blob(failure)
        manager.deliver_output_blob(failure)
        for requester in requesters:
            requester.join(timeout=1)

        self.assertTrue(all(not requester.is_alive() for requester in requesters))
        self.assertEqual(len(errors), 2)
        self.assertTrue(all(isinstance(error, RuntimeError) for error in errors))
        completed_state = state.wait_for_update(
            last_seen_hash=current_state.sha256_hash
        )
        self.assertEqual(len(completed_state.output_blob_requests), 0)

    def test_state_reconciliation_failure_does_not_strand_requester(self) -> None:
        state, manager = self._manager()
        initial_state = state.wait_for_update(last_seen_hash=None)
        self.assertIsNotNone(initial_state)
        errors: list[BaseException] = []

        def request_blob() -> None:
            try:
                manager.get_new_output_blob(size=10)
            except BaseException as error:
                errors.append(error)

        requester = threading.Thread(target=request_blob, daemon=True)
        requester.start()
        pending_state = state.wait_for_update(last_seen_hash=initial_state.sha256_hash)
        self.assertEqual(len(pending_state.output_blob_requests), 1)
        blob_id = pending_state.output_blob_requests[0].id

        with patch.object(
            state,
            "remove_output_blob_request",
            side_effect=RuntimeError("state reconciliation failed"),
        ):
            manager.deliver_output_blob(
                AllocationOutputBlob(
                    status=Status(code=0),
                    blob={"id": blob_id},
                )
            )
        requester.join(timeout=1)

        self.assertFalse(requester.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], RuntimeError)
        self.assertIn("reconcile output BLOB", str(errors[0]))

    def test_state_publication_failure_removes_correlation_request(self) -> None:
        state, manager = self._manager()

        with patch.object(
            state,
            "add_output_blob_request",
            side_effect=RuntimeError("state publication failed"),
        ):
            with self.assertRaisesRegex(RuntimeError, "state publication failed"):
                manager.get_new_output_blob(size=10)

        self.assertEqual(manager._output_blob_requests, {})


if __name__ == "__main__":
    unittest.main()
