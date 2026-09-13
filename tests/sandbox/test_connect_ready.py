"""Connection readiness must not execute or replay user work."""

import json
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from tensorlake.sandbox import AsyncSandbox, Sandbox, SandboxError, SandboxStatus
from tensorlake.sandbox._connect import retryable_health_error
from tensorlake.sandbox.exceptions import RemoteAPIError


def info(
    status: SandboxStatus = SandboxStatus.RUNNING,
    route: str | None = "https://sandbox.test",
) -> SimpleNamespace:
    value = SimpleNamespace(status=status, sandbox_id="sandbox-id", sandbox_url=route)
    return SimpleNamespace(value=value, status=status, sandbox_id="sandbox-id")


def client_and_handle() -> tuple[MagicMock, MagicMock]:
    client = MagicMock()
    client._with_request_timeout.return_value = client
    client.get.return_value = info()
    handle = MagicMock()
    handle.sandbox_id = "sandbox-id"
    client.connect.return_value = handle
    return client, handle


class TestConnectReady(unittest.TestCase):
    def test_connect_waits_for_health_and_refreshes_routing(self) -> None:
        client, handle = client_and_handle()
        handle.health.side_effect = [
            RemoteAPIError(502, "proxy starting"),
            SimpleNamespace(healthy=True),
        ]
        with patch("tensorlake.sandbox.client.SandboxClient", return_value=client):
            result = Sandbox.connect("sandbox-id", request_timeout=2)
        assert result is handle
        assert handle.health.call_count == 2
        assert (
            client.connect.call_args.kwargs["_routing_info"]
            is client.get.return_value.value
        )
        assert client.connect.call_args.kwargs["request_timeout"] == 2
        handle.run.assert_not_called()

    def test_connect_resumes_even_without_a_suspended_proxy_url(self) -> None:
        client, handle = client_and_handle()
        client.get.side_effect = [info(SandboxStatus.SUSPENDED, None), info(), info()]
        with patch("tensorlake.sandbox.client.SandboxClient", return_value=client):
            assert Sandbox.connect("sandbox-id") is handle
        client.resume.assert_called_once_with("sandbox-id", wait=False)
        handle.health.assert_called_once()

    def test_concurrent_resume_waits_for_the_winner(self) -> None:
        client, handle = client_and_handle()
        client.get.side_effect = [
            info(SandboxStatus.SUSPENDED),
            info(SandboxStatus.PENDING),
            info(),
            info(),
        ]
        client.resume.side_effect = RemoteAPIError(
            400, "Can only resume a suspended sandbox"
        )
        with patch("tensorlake.sandbox.client.SandboxClient", return_value=client):
            assert Sandbox.connect("sandbox-id") is handle
        handle.health.assert_called_once()

    def test_resume_quota_failure_is_not_retried(self) -> None:
        client, handle = client_and_handle()
        client.get.return_value = info(SandboxStatus.SUSPENDED)
        client.resume.side_effect = RemoteAPIError(400, "namespace quota exceeded")
        with patch("tensorlake.sandbox.client.SandboxClient", return_value=client):
            with self.assertRaisesRegex(RemoteAPIError, "quota exceeded"):
                Sandbox.connect("sandbox-id")
        client.resume.assert_called_once()
        handle.health.assert_not_called()

    def test_passive_connect_does_not_read_or_wake(self) -> None:
        client, handle = client_and_handle()
        with patch("tensorlake.sandbox.client.SandboxClient", return_value=client):
            assert Sandbox.connect("sandbox-id", resume=False) is handle
        client.get.assert_not_called()
        client.resume.assert_not_called()
        handle.health.assert_not_called()

    def test_terminated_connect_never_resumes(self) -> None:
        client, handle = client_and_handle()
        client.get.return_value = info(SandboxStatus.TERMINATED)
        with patch("tensorlake.sandbox.client.SandboxClient", return_value=client):
            with self.assertRaisesRegex(SandboxError, "terminated"):
                Sandbox.connect("sandbox-id")
        client.resume.assert_not_called()
        handle.health.assert_not_called()

    def test_health_auth_failure_is_not_retried(self) -> None:
        client, handle = client_and_handle()
        handle.health.side_effect = RemoteAPIError(403, "forbidden")
        with patch("tensorlake.sandbox.client.SandboxClient", return_value=client):
            with self.assertRaisesRegex(RemoteAPIError, "forbidden"):
                Sandbox.connect("sandbox-id")
        handle.health.assert_called_once()

    def test_unready_connect_has_a_deadline(self) -> None:
        client, handle = client_and_handle()
        handle.health.side_effect = RemoteAPIError(503, "not ready")
        with patch("tensorlake.sandbox.client.SandboxClient", return_value=client):
            with self.assertRaisesRegex(SandboxError, "within 0.01s"):
                Sandbox.connect("sandbox-id", request_timeout=0.01)

    def test_unrelated_health_errors_are_not_retried(self) -> None:
        for status in [400, 401, 403, 404, 409, 422, 500]:
            with self.subTest(status=status):
                assert not retryable_health_error(
                    RemoteAPIError(status, "unrelated failure")
                )

    def test_transition_error_is_retryable_only_for_health(self) -> None:
        assert retryable_health_error(
            RemoteAPIError(400, json.dumps({"code": "SANDBOX_NOT_RUNNING"}))
        )

    def test_async_connect_resumes_and_probes_without_executing_work(self) -> None:
        import asyncio

        client, handle = client_and_handle()
        client.get = AsyncMock(
            side_effect=[info(SandboxStatus.SUSPENDED, None), info(), info()]
        )
        client.connect = AsyncMock(return_value=handle)
        client.resume = AsyncMock()
        handle.health = AsyncMock(return_value=SimpleNamespace(healthy=True))
        with patch(
            "tensorlake.sandbox.async_client.AsyncSandboxClient", return_value=client
        ):
            assert asyncio.run(AsyncSandbox.connect("sandbox-id")) is handle
        client.resume.assert_awaited_once_with("sandbox-id", wait=False)
        handle.health.assert_awaited_once()
        handle.run.assert_not_called()

    def test_async_connect_opt_out_is_passive(self) -> None:
        import asyncio

        client, handle = client_and_handle()
        client.get = AsyncMock()
        client.connect = AsyncMock(return_value=handle)
        handle.health = AsyncMock(return_value=SimpleNamespace(healthy=True))
        with patch(
            "tensorlake.sandbox.async_client.AsyncSandboxClient", return_value=client
        ):
            assert (
                asyncio.run(AsyncSandbox.connect("sandbox-id", resume=False)) is handle
            )
        client.get.assert_not_awaited()
        handle.health.assert_not_awaited()

    def test_stale_health_timeout_refreshes_route_and_preserves_command_timeout(
        self,
    ) -> None:
        from tensorlake.sandbox.exceptions import SandboxConnectionError

        client, handle = client_and_handle()
        old = info(route="https://old.sandbox.test")
        fresh = info(route="https://new.sandbox.test")
        client.get.side_effect = [old, fresh, fresh]
        handle.health.side_effect = [
            SandboxConnectionError("timed out"),
            SimpleNamespace(healthy=True),
        ]
        with patch("tensorlake.sandbox.client.SandboxClient", return_value=client):
            Sandbox.connect("session-name", request_timeout=90)
        probe_calls = client.connect.call_args_list[:2]
        assert all(call.kwargs["request_timeout"] <= 5 for call in probe_calls)
        assert probe_calls[1].kwargs["_routing_info"] is fresh.value
        assert client.connect.call_args.kwargs["request_timeout"] == 90
        assert client.get.call_args_list[0].args == ("session-name",)
        assert all(
            call.args == ("sandbox-id",) for call in client.get.call_args_list[1:]
        )
        handle.run.assert_not_called()

    def test_unhealthy_daemon_does_not_complete_connect(self) -> None:
        client, handle = client_and_handle()
        handle.health.side_effect = [
            SimpleNamespace(healthy=False),
            SimpleNamespace(healthy=True),
        ]
        with patch("tensorlake.sandbox.client.SandboxClient", return_value=client):
            Sandbox.connect("sandbox-id")
        assert handle.health.call_count == 2
