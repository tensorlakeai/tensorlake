import hashlib
import io
import json
import os
import sys
import threading
import time
import unittest
import uuid
import zipfile
from unittest.mock import MagicMock, patch

import grpc

from tensorlake.applications.internal_logger import InternalLogger
from tensorlake.applications.registry import restore_registry, snapshot_registry
from tensorlake.applications.runtime_hooks import (
    clear_await_future_hook,
    clear_coroutine_to_future_hook,
    clear_register_coroutine_hook,
    clear_run_future_hook,
    clear_wait_futures_hook,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    BLOB,
    INITIALIZATION_FAILURE_REASON_INTERNAL_ERROR,
    INITIALIZATION_OUTCOME_CODE_FAILURE,
    INITIALIZATION_OUTCOME_CODE_SUCCESS,
    Allocation,
    CreateAllocationRequest,
    FunctionInputs,
    FunctionRef,
    InitializeRequest,
    SerializedObject,
    SerializedObjectEncoding,
    SerializedObjectManifest,
)
from tensorlake.function_executor.service import Service


class _ContextAbort(Exception):
    def __init__(self, code: grpc.StatusCode, details: str):
        super().__init__(details)
        self.code = code
        self.details = details


def _context() -> MagicMock:
    context = MagicMock()
    context.is_active.return_value = True
    context.time_remaining.return_value = 5.0

    def abort(code: grpc.StatusCode, details: str) -> None:
        raise _ContextAbort(code, details)

    context.abort.side_effect = abort
    return context


def _allocation_request(allocation_id: str) -> CreateAllocationRequest:
    return CreateAllocationRequest(
        allocation=Allocation(
            request_id=f"request-{allocation_id}",
            function_call_id=f"call-{allocation_id}",
            allocation_id=allocation_id,
            inputs=FunctionInputs(
                args=[],
                arg_blobs=[],
                request_error_blob=BLOB(id=f"error-{allocation_id}"),
            ),
        )
    )


def _clear_runtime_hooks() -> None:
    clear_run_future_hook()
    clear_await_future_hook()
    clear_wait_futures_hook()
    clear_register_coroutine_hook()
    clear_coroutine_to_future_hook()


def _application_archive(module_name: str, function_name: str) -> bytes:
    manifest = {
        "functions": {
            function_name: {
                "name": function_name,
                "module_import_name": module_name,
            }
        }
    }
    source = "\n".join(
        [
            "from tensorlake.applications import function",
            "",
            "@function()",
            f"def {function_name}(value):",
            "    return value",
            "",
        ]
    ).encode()
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(".tensorlake_code_manifest.json", json.dumps(manifest))
        archive.writestr(f"{module_name}.py", source)
    return output.getvalue()


class TestServiceInitialization(unittest.TestCase):
    def setUp(self) -> None:
        self.registry_before = snapshot_registry()
        self.path_before = list(sys.path)
        self.imported_module_names: list[str] = []
        _clear_runtime_hooks()

    def tearDown(self) -> None:
        _clear_runtime_hooks()
        restore_registry(self.registry_before)
        for module_name in self.imported_module_names:
            sys.modules.pop(module_name, None)
        for path in list(sys.path):
            if path not in self.path_before and path.endswith(".zip"):
                sys.path.remove(path)
                try:
                    os.unlink(path)
                except FileNotFoundError:
                    pass
        sys.path[:] = self.path_before

    def test_allocation_before_initialization_is_failed_precondition(self) -> None:
        service = Service(
            InternalLogger(
                context={},
                destination=InternalLogger.LOG_FILE.NULL,
                as_cloud_event=False,
            )
        )

        with self.assertRaises(_ContextAbort) as raised:
            service.create_allocation(
                _allocation_request("before-initialization"),
                _context(),
            )

        self.assertEqual(raised.exception.code, grpc.StatusCode.FAILED_PRECONDITION)
        self.assertEqual(
            raised.exception.details,
            "Function Executor is not initialized",
        )

    def test_allocation_waits_for_initialization_in_progress(self) -> None:
        suffix = uuid.uuid4().hex
        module_name = f"function_executor_slow_init_{suffix}"
        self.imported_module_names.append(module_name)
        function_name = f"slow_init_function_{suffix}"
        archive = _application_archive(module_name, function_name)
        request = InitializeRequest(
            function=FunctionRef(
                namespace="test",
                application_name=function_name,
                application_version="v1",
                function_name=function_name,
            ),
            application_code=SerializedObject(
                manifest=SerializedObjectManifest(
                    encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_ZIP,
                    encoding_version=0,
                    size=len(archive),
                    metadata_size=0,
                    sha256_hash=hashlib.sha256(archive).hexdigest(),
                ),
                data=archive,
            ),
        )
        service = Service(
            InternalLogger(
                context={},
                destination=InternalLogger.LOG_FILE.NULL,
                as_cloud_event=False,
            )
        )
        initialization_entered = threading.Event()
        release_initialization = threading.Event()
        fake_blob_store = MagicMock()
        fake_server = MagicMock()
        fake_server.base_url = "http://localhost:12345"
        fake_thread = MagicMock()
        fake_client = MagicMock()
        fake_runner = MagicMock()
        real_thread = threading.Thread

        def create_blob_store(*_args, **_kwargs):
            initialization_entered.set()
            if not release_initialization.wait(timeout=5):
                raise RuntimeError("test did not release initialization")
            return fake_blob_store

        initialization_responses = []
        allocation_errors = []

        with (
            patch(
                "tensorlake.function_executor.service.BLOBStore",
                side_effect=create_blob_store,
            ),
            patch(
                "tensorlake.function_executor.service.RequestContextHTTPServer",
                return_value=fake_server,
            ),
            patch(
                "tensorlake.function_executor.service.RequestContextHTTPClient"
            ) as request_context_client,
            patch(
                "tensorlake.function_executor.service.threading.Thread",
                return_value=fake_thread,
            ),
            patch(
                "tensorlake.function_executor.service.AllocationRunner",
                return_value=fake_runner,
            ) as allocation_runner,
            patch("tensorlake.function_executor.service.setup_multiprocessing"),
        ):
            request_context_client.create_http_client.return_value = fake_client
            initialization_thread = real_thread(
                target=lambda: initialization_responses.append(
                    service.initialize(request, _context())
                )
            )
            initialization_thread.start()
            self.assertTrue(initialization_entered.wait(timeout=2))

            def create_allocation() -> None:
                try:
                    service.create_allocation(
                        _allocation_request("during-initialization"),
                        _context(),
                    )
                except BaseException as error:
                    allocation_errors.append(error)

            allocation_thread = real_thread(target=create_allocation)
            allocation_thread.start()
            self.assertTrue(allocation_thread.is_alive())
            allocation_runner.assert_not_called()

            release_initialization.set()
            initialization_thread.join(timeout=5)
            allocation_thread.join(timeout=5)

        self.assertFalse(initialization_thread.is_alive())
        self.assertFalse(allocation_thread.is_alive())
        self.assertEqual(allocation_errors, [])
        self.assertEqual(
            initialization_responses[0].outcome_code,
            INITIALIZATION_OUTCOME_CODE_SUCCESS,
        )
        allocation_runner.assert_called_once()
        fake_runner.run.assert_called_once_with()

    def test_initialization_is_visible_before_waiting_for_serial_lock(self) -> None:
        service = Service(
            InternalLogger(
                context={},
                destination=InternalLogger.LOG_FILE.NULL,
                as_cloud_event=False,
            )
        )
        lock_entered = threading.Event()
        release_lock = threading.Event()
        fake_runner = MagicMock()
        initialization_responses = []
        allocation_errors = []

        class BlockingInitializationLock:
            def __enter__(self):
                lock_entered.set()
                self.assert_released()

            def assert_released(self):
                if not release_lock.wait(timeout=5):
                    raise RuntimeError("test did not release initialization lock")

            def __exit__(self, *_args):
                return False

        def finish_initialization(_request, _context):
            service._function_ref = MagicMock()
            service._function_ref.function_name = "test"
            service._function = MagicMock()
            service._blob_store = MagicMock()
            service._request_context_http_server = MagicMock()
            service._request_context_http_server.base_url = "http://localhost:12345"
            service._request_context_http_client = MagicMock()
            return MagicMock(outcome_code=INITIALIZATION_OUTCOME_CODE_SUCCESS)

        service._initialization_lock = BlockingInitializationLock()
        with (
            patch.object(service, "_initialize", side_effect=finish_initialization),
            patch(
                "tensorlake.function_executor.service.AllocationRunner",
                return_value=fake_runner,
            ),
        ):
            initialization_thread = threading.Thread(
                target=lambda: initialization_responses.append(
                    service.initialize(MagicMock(), _context())
                )
            )
            initialization_thread.start()
            self.assertTrue(lock_entered.wait(timeout=2))

            allocation_thread = threading.Thread(
                target=lambda: self._create_allocation_recording_error(
                    service,
                    "initialization-lock-window",
                    allocation_errors,
                )
            )
            allocation_thread.start()
            self.assertTrue(allocation_thread.is_alive())

            release_lock.set()
            initialization_thread.join(timeout=5)
            allocation_thread.join(timeout=5)

        self.assertFalse(initialization_thread.is_alive())
        self.assertFalse(allocation_thread.is_alive())
        self.assertEqual(allocation_errors, [])
        self.assertEqual(
            initialization_responses[0].outcome_code,
            INITIALIZATION_OUTCOME_CODE_SUCCESS,
        )
        fake_runner.run.assert_called_once_with()

    def test_allocation_waits_for_queued_initialization_retry(self) -> None:
        service = Service(
            InternalLogger(
                context={},
                destination=InternalLogger.LOG_FILE.NULL,
                as_cloud_event=False,
            )
        )
        first_entered = threading.Event()
        release_first = threading.Event()
        second_entered = threading.Event()
        release_second = threading.Event()
        fake_runner = MagicMock()
        responses = []
        allocation_errors = []
        attempt = 0

        def initialize_attempt(_request, _context):
            nonlocal attempt
            attempt += 1
            if attempt == 1:
                first_entered.set()
                self.assertTrue(release_first.wait(timeout=5))
                return MagicMock(outcome_code=INITIALIZATION_OUTCOME_CODE_FAILURE)

            second_entered.set()
            self.assertTrue(release_second.wait(timeout=5))
            service._function_ref = MagicMock()
            service._function_ref.function_name = "test"
            service._function = MagicMock()
            service._blob_store = MagicMock()
            service._request_context_http_server = MagicMock()
            service._request_context_http_server.base_url = "http://localhost:12345"
            service._request_context_http_client = MagicMock()
            return MagicMock(outcome_code=INITIALIZATION_OUTCOME_CODE_SUCCESS)

        with (
            patch.object(service, "_initialize", side_effect=initialize_attempt),
            patch(
                "tensorlake.function_executor.service.AllocationRunner",
                return_value=fake_runner,
            ),
        ):
            first = threading.Thread(
                target=lambda: responses.append(
                    service.initialize(MagicMock(), _context())
                )
            )
            first.start()
            self.assertTrue(first_entered.wait(timeout=2))

            second = threading.Thread(
                target=lambda: responses.append(
                    service.initialize(MagicMock(), _context())
                )
            )
            second.start()
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                with service._initialization_condition:
                    if service._initialization_request_count == 2:
                        break
                time.sleep(0.01)
            else:
                self.fail("queued initialization retry was not published")

            allocation = threading.Thread(
                target=lambda: self._create_allocation_recording_error(
                    service,
                    "queued-initialization-retry",
                    allocation_errors,
                )
            )
            allocation.start()

            release_first.set()
            self.assertTrue(second_entered.wait(timeout=2))
            self.assertTrue(allocation.is_alive())
            fake_runner.run.assert_not_called()

            release_second.set()
            first.join(timeout=5)
            second.join(timeout=5)
            allocation.join(timeout=5)

        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertFalse(allocation.is_alive())
        self.assertEqual(allocation_errors, [])
        self.assertEqual(
            sorted(response.outcome_code for response in responses),
            sorted(
                [
                    INITIALIZATION_OUTCOME_CODE_FAILURE,
                    INITIALIZATION_OUTCOME_CODE_SUCCESS,
                ]
            ),
        )
        fake_runner.run.assert_called_once_with()

    def test_completed_initialization_rechecks_allocation_deadline(self) -> None:
        service = Service(
            InternalLogger(
                context={},
                destination=InternalLogger.LOG_FILE.NULL,
                as_cloud_event=False,
            )
        )
        service._function_ref = MagicMock()
        service._function = MagicMock()
        service._blob_store = MagicMock()
        service._request_context_http_server = MagicMock()
        service._request_context_http_client = MagicMock()
        context = _context()
        context.time_remaining.return_value = 0.0

        with self.assertRaises(_ContextAbort) as raised:
            service._wait_for_initialization(context)

        self.assertEqual(raised.exception.code, grpc.StatusCode.DEADLINE_EXCEEDED)

    def test_completed_initialization_rechecks_allocation_cancellation(self) -> None:
        service = Service(
            InternalLogger(
                context={},
                destination=InternalLogger.LOG_FILE.NULL,
                as_cloud_event=False,
            )
        )
        service._function_ref = MagicMock()
        service._function = MagicMock()
        service._blob_store = MagicMock()
        service._request_context_http_server = MagicMock()
        service._request_context_http_client = MagicMock()
        context = _context()
        context.is_active.return_value = False

        with self.assertRaises(_ContextAbort) as raised:
            service._wait_for_initialization(context)

        self.assertEqual(raised.exception.code, grpc.StatusCode.CANCELLED)

    def test_shutdown_rejects_allocation_constructed_during_shutdown(self) -> None:
        service = Service(
            InternalLogger(
                context={},
                destination=InternalLogger.LOG_FILE.NULL,
                as_cloud_event=False,
            )
        )
        service._function_ref = MagicMock()
        service._function_ref.function_name = "test"
        service._function = MagicMock()
        service._blob_store = MagicMock()
        service._request_context_http_server = MagicMock()
        service._request_context_http_server.base_url = "http://localhost:12345"
        service._request_context_http_client = MagicMock()
        construction_entered = threading.Event()
        release_construction = threading.Event()
        fake_runner = MagicMock()
        errors: list[BaseException] = []

        def construct_runner(*_args, **_kwargs):
            construction_entered.set()
            self.assertTrue(release_construction.wait(timeout=5))
            return fake_runner

        with patch(
            "tensorlake.function_executor.service.AllocationRunner",
            side_effect=construct_runner,
        ):
            thread = threading.Thread(
                target=lambda: self._create_allocation_recording_error(
                    service,
                    "shutdown-race",
                    errors,
                )
            )
            thread.start()
            self.assertTrue(construction_entered.wait(timeout=2))
            service.shutdown()
            release_construction.set()
            thread.join(timeout=5)

        self.assertFalse(thread.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], _ContextAbort)
        self.assertEqual(errors[0].code, grpc.StatusCode.UNAVAILABLE)
        fake_runner.run.assert_not_called()
        self.assertEqual(service._allocation_infos, {})

    def test_concurrent_duplicate_allocation_starts_exactly_one_runner(self) -> None:
        service = Service(
            InternalLogger(
                context={},
                destination=InternalLogger.LOG_FILE.NULL,
                as_cloud_event=False,
            )
        )
        service._function_ref = MagicMock()
        service._function_ref.function_name = "test"
        service._function = MagicMock()
        service._blob_store = MagicMock()
        service._request_context_http_server = MagicMock()
        service._request_context_http_server.base_url = "http://localhost:12345"
        service._request_context_http_client = MagicMock()
        construction_barrier = threading.Barrier(2)
        runners = [MagicMock(), MagicMock()]
        errors: list[BaseException] = []
        runner_index = 0
        runner_index_lock = threading.Lock()

        def construct_runner(*_args, **_kwargs):
            nonlocal runner_index
            with runner_index_lock:
                runner = runners[runner_index]
                runner_index += 1
            construction_barrier.wait(timeout=5)
            return runner

        with patch(
            "tensorlake.function_executor.service.AllocationRunner",
            side_effect=construct_runner,
        ):
            threads = [
                threading.Thread(
                    target=lambda: self._create_allocation_recording_error(
                        service,
                        "duplicate",
                        errors,
                    )
                )
                for _ in range(2)
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=5)

        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], _ContextAbort)
        self.assertEqual(errors[0].code, grpc.StatusCode.ALREADY_EXISTS)
        self.assertEqual(sum(runner.run.call_count for runner in runners), 1)
        self.assertEqual(list(service._allocation_infos), ["duplicate"])

    @staticmethod
    def _create_allocation_recording_error(
        service: Service,
        allocation_id: str,
        errors: list[BaseException],
    ) -> None:
        try:
            service.create_allocation(_allocation_request(allocation_id), _context())
        except BaseException as error:
            errors.append(error)

    def test_runtime_resource_failure_rolls_back_and_allows_retry(self) -> None:
        suffix = uuid.uuid4().hex
        module_name = f"function_executor_retry_{suffix}"
        self.imported_module_names.append(module_name)
        function_name = f"retry_function_{suffix}"
        archive = _application_archive(module_name, function_name)
        request = InitializeRequest(
            function=FunctionRef(
                namespace="test",
                application_name=function_name,
                application_version="v1",
                function_name=function_name,
            ),
            application_code=SerializedObject(
                manifest=SerializedObjectManifest(
                    encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_ZIP,
                    encoding_version=0,
                    size=len(archive),
                    metadata_size=0,
                    sha256_hash=hashlib.sha256(archive).hexdigest(),
                ),
                data=archive,
            ),
        )
        service = Service(
            InternalLogger(
                context={},
                destination=InternalLogger.LOG_FILE.NULL,
                as_cloud_event=False,
            )
        )
        fake_blob_store = MagicMock()
        fake_server = MagicMock()
        fake_server.base_url = "http://localhost:12345"
        fake_thread = MagicMock()
        fake_client = MagicMock()

        with (
            patch(
                "tensorlake.function_executor.service.BLOBStore",
                side_effect=[RuntimeError("blob store unavailable"), fake_blob_store],
            ),
            patch(
                "tensorlake.function_executor.service.RequestContextHTTPServer",
                return_value=fake_server,
            ),
            patch(
                "tensorlake.function_executor.service.RequestContextHTTPClient.create_http_client",
                return_value=fake_client,
            ),
            patch(
                "tensorlake.function_executor.service.threading.Thread",
                return_value=fake_thread,
            ),
            patch("tensorlake.function_executor.service.setup_multiprocessing"),
        ):
            failed = service.initialize(request, MagicMock())
            self.assertEqual(
                failed.outcome_code,
                INITIALIZATION_OUTCOME_CODE_FAILURE,
            )
            self.assertEqual(
                failed.failure_reason,
                INITIALIZATION_FAILURE_REASON_INTERNAL_ERROR,
            )
            self.assertNotIn(module_name, sys.modules)
            self.assertEqual(sys.path, self.path_before)

            initialized = service.initialize(request, MagicMock())
            self.assertEqual(
                initialized.outcome_code,
                INITIALIZATION_OUTCOME_CODE_SUCCESS,
            )
            fake_thread.start.assert_called_once_with()
