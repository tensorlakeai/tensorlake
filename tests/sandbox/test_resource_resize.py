"""Public create/update parity and resize forwarding through both Python APIs."""

import asyncio
import inspect
import json
import unittest
from unittest.mock import AsyncMock, MagicMock

from tensorlake._tracing import Traced
from tensorlake.sandbox import (
    CLEAR_NETWORK_POLICY,
    AsyncSandbox,
    AsyncSandboxClient,
    ResizeErrorReason,
    ResizeStatus,
    Sandbox,
    SandboxClient,
    SandboxError,
    SandboxInfo,
    SandboxResizeError,
)
from tensorlake.sandbox.client import (
    RustCloudSandboxClientError,
    _raise_as_sandbox_error,
)

INFO = {
    "id": "sb-1",
    "namespace": "default",
    "status": "running",
    "resources": {"cpus": 1.0, "memory_mb": 1024, "disk_mb": 1024},
    "resource_resize": {
        "generation": 7,
        "status": "pending",
        "error_message": None,
        "requested": {"cpus": 2.0, "memory_mb": 2048, "disk_mb": 1024},
    },
}


def client(async_=False):
    obj = (AsyncSandboxClient if async_ else SandboxClient).__new__(
        AsyncSandboxClient if async_ else SandboxClient
    )
    obj._rust_client = MagicMock()
    method = "update_sandbox_async" if async_ else "update_sandbox"
    mock = AsyncMock if async_ else MagicMock
    setattr(
        obj._rust_client, method, mock(return_value=("trace-resize", json.dumps(INFO)))
    )
    setattr(
        obj._rust_client,
        "wait_for_resource_resize" + ("_async" if async_ else ""),
        mock(return_value=("trace-wait", json.dumps(INFO))),
    )
    return obj


class TestResourceResize(unittest.TestCase):

    def test_resource_names_and_types_match_create(self):
        for cls, method in [
            (SandboxClient, "update_sandbox"),
            (AsyncSandboxClient, "update_sandbox"),
            (Sandbox, "update"),
            (AsyncSandbox, "update"),
        ]:
            with self.subTest(cls=cls, method=method):
                create = inspect.signature(cls.create).parameters
                update = inspect.signature(getattr(cls, method)).parameters
                for name in ("cpus", "memory_mb", "disk_mb"):
                    assert update[name].annotation == create[name].annotation
                    assert update[name].default is None
                assert update["wait"].default is True

    def test_future_resize_status_preserves_sandbox_info_and_wire_value(self):
        payload = {
            **INFO,
            "resource_resize": {**INFO["resource_resize"], "status": "reconciling"},
        }
        info = SandboxInfo.model_validate(payload)
        assert info.resource_resize.status == "reconciling"
        assert info.resources.memory_mb == 1024
        assert (
            json.loads(info.model_dump_json())["resource_resize"]["status"]
            == "reconciling"
        )

    def test_partial_targets_match_create_wire_shape_and_default_wait(self):
        for kwargs in [{"cpus": 2.0}, {"memory_mb": 1001}, {"disk_mb": 2048}]:
            with self.subTest(kwargs=kwargs):
                obj = client()
                result = obj.update_sandbox("named", **kwargs)
                call = obj._rust_client.update_sandbox.call_args.kwargs
                assert json.loads(call.pop("request_json")) == {"resources": kwargs}
                assert call == {
                    "sandbox_id": "named",
                    "wait": True,
                    "timeout_sec": 300,
                    "poll_interval_sec": 1.0,
                }
                assert result.resource_resize.generation == 7
                assert result.resource_resize.status is ResizeStatus.PENDING
                assert result.resources.memory_mb == 1024

    def test_invalid_values_are_rejected_before_native_serialization(self):
        for kwargs in [
            {"cpus": 1.5},
            {"cpus": float("nan")},
            {"cpus": float("inf")},
            {"cpus": True},
            {"cpus": 0},
            {"memory_mb": 1.5},
            {"memory_mb": True},
            {"memory_mb": -1},
            {"disk_mb": 0},
            {"disk_mb": 1.5},
            {"memory_mb": float("nan")},
            {"disk_mb": float("inf")},
            {"disk_mb": "2048"},
        ]:
            with self.subTest(kwargs=kwargs):
                obj = client()
                with self.assertRaisesRegex(SandboxError, next(iter(kwargs))):
                    obj.update_sandbox("sb-1", **kwargs)
                obj._rust_client.update_sandbox.assert_not_called()

    def test_integral_floats_match_create_coercion_without_rounding(self):
        for async_ in [False, True]:
            obj = client(async_)
            result = obj.update_sandbox("sb-1", memory_mb=2048.0, disk_mb=4096.0)
            if async_:
                asyncio.run(result)
            native = getattr(
                obj._rust_client, "update_sandbox_async" if async_ else "update_sandbox"
            )
            resources = json.loads(native.call_args.kwargs["request_json"])["resources"]
            assert resources == {"memory_mb": 2048, "disk_mb": 4096}
            assert all(isinstance(value, int) for value in resources.values())

    def test_resize_error_can_be_constructed_without_a_wire_payload_or_observation(
        self,
    ):
        error = SandboxResizeError(
            "sb-1",
            generation=7,
            reason=ResizeErrorReason.TIMEOUT,
            message="wait timed out",
        )
        assert error.reason is ResizeErrorReason.TIMEOUT
        assert error.confirmed_resources is None
        assert "last confirmed allocation: unavailable" in str(error)

    def test_resource_updates_must_be_standalone(self):
        for kwargs in [
            {"name": "new"},
            {"network": CLEAR_NETWORK_POLICY},
            {"exposed_ports": []},
            {"allow_unauthenticated_access": False},
        ]:
            with self.subTest(kwargs=kwargs):
                obj = client()
                with self.assertRaisesRegex(SandboxError, "cannot be combined"):
                    obj.update_sandbox("sb-1", memory_mb=2048, **kwargs)
                obj._rust_client.update_sandbox.assert_not_called()

    def test_async_partial_resize_and_generation_wait_forwarding(self):
        obj = client(True)
        result = asyncio.run(
            obj.update_sandbox(
                "sb-1", memory_mb=1001, wait=False, timeout=10, poll_interval=0.1
            )
        )
        call = obj._rust_client.update_sandbox_async.call_args.kwargs
        assert json.loads(call["request_json"]) == {"resources": {"memory_mb": 1001}}
        assert call["wait"] is False
        assert result.resource_resize.generation == 7
        asyncio.run(
            obj.wait_for_resource_resize("sb-1", 7, timeout=15, poll_interval=0.2)
        )
        obj._rust_client.wait_for_resource_resize_async.assert_awaited_once_with(
            sandbox_id="sb-1", generation=7, timeout_sec=15, poll_interval_sec=0.2
        )

    def test_sync_generation_wait_forwarding(self):
        obj = client()
        obj.wait_for_resource_resize("sb-1", 7, timeout=10)
        obj._rust_client.wait_for_resource_resize.assert_called_once_with(
            sandbox_id="sb-1", generation=7, timeout_sec=10, poll_interval_sec=1.0
        )

    def test_native_error_retains_generation_diagnostic_and_actual_allocation(self):
        for reason in ["failed", "timeout", "superseded"]:
            with self.subTest(reason=reason):
                payload = {
                    "sandbox_id": "sb-1",
                    "generation": 7,
                    "reason": reason,
                    "message": "ConfigurationError: below immutable boot memory",
                    "info": INFO,
                }
                with self.assertRaises(SandboxResizeError) as raised:
                    _raise_as_sandbox_error(
                        RustCloudSandboxClientError("resize", None, json.dumps(payload))
                    )
                assert raised.exception.reason is ResizeErrorReason(reason)
                assert raised.exception.confirmed_resources.memory_mb == 1024
                assert "1 CPUs, 1024 MiB memory, 1024 MiB disk" in str(raised.exception)
                assert raised.exception.generation == 7
                assert raised.exception.info.resources.memory_mb == 1024
                assert "below immutable boot memory" in str(raised.exception)

    def test_sandbox_objects_forward_resources_and_cache_result(self):
        for async_ in [False, True]:
            with self.subTest(async_=async_):
                cls = AsyncSandbox if async_ else Sandbox
                obj = cls.__new__(cls)
                obj._sandbox_id = "sb-1"
                obj._identifier = "named"
                obj._cached_info = SandboxInfo.model_validate(INFO)
                obj._lifecycle_client = MagicMock()
                mock = AsyncMock if async_ else MagicMock
                obj._lifecycle_client.update_sandbox = mock(
                    return_value=Traced("trace", SandboxInfo.model_validate(INFO))
                )
                obj._lifecycle_client.wait_for_resource_resize = mock(
                    return_value=Traced("trace", SandboxInfo.model_validate(INFO))
                )
                result = obj.update(cpus=2.0, memory_mb=2048, disk_mb=4096, wait=False)
                if async_:
                    result = asyncio.run(result)
                call = obj._lifecycle_client.update_sandbox.call_args.kwargs
                assert {
                    k: call[k] for k in ("cpus", "memory_mb", "disk_mb", "wait")
                } == {"cpus": 2.0, "memory_mb": 2048, "disk_mb": 4096, "wait": False}
                result = obj.wait_for_resource_resize(7, timeout=12)
                if async_:
                    result = asyncio.run(result)
                obj._lifecycle_client.wait_for_resource_resize.assert_called_once_with(
                    "sb-1", 7, timeout=12, poll_interval=1.0
                )
                assert obj._cached_info.resource_resize.generation == 7


if __name__ == "__main__":
    unittest.main()
