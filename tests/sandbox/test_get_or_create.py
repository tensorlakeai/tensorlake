"""Unit tests for ``Sandbox.get_or_create`` / ``AsyncSandbox.get_or_create``.

These tests mock the ``connect``/``create`` classmethods, so they run without
a server or the compiled cloud SDK.
"""

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

from tensorlake.sandbox import (
    AsyncSandbox,
    RemoteAPIError,
    Sandbox,
    SandboxError,
    SandboxNotFoundError,
    SandboxNotRoutableError,
    SandboxStatus,
)

_NAME = "agent-session-123"


def _sandbox_mock(status: SandboxStatus) -> MagicMock:
    sandbox = MagicMock(spec=Sandbox)
    sandbox.info.return_value = SimpleNamespace(status=status)
    return sandbox


def _async_sandbox_mock(status: SandboxStatus) -> MagicMock:
    sandbox = MagicMock(spec=AsyncSandbox)
    sandbox.info = AsyncMock(return_value=SimpleNamespace(status=status))
    sandbox.resume = AsyncMock()
    return sandbox


class TestGetOrCreate(unittest.TestCase):
    def test_existing_running_sandbox_is_returned(self):
        existing = _sandbox_mock(SandboxStatus.RUNNING)
        with (
            patch.object(Sandbox, "connect", return_value=existing) as connect,
            patch.object(Sandbox, "create") as create,
        ):
            result = Sandbox.get_or_create(_NAME)
        self.assertIs(result, existing)
        self.assertEqual(existing._bind_outcome, "attached")
        self.assertEqual(connect.call_count, 1)
        self.assertEqual(connect.call_args.args, (_NAME,))
        create.assert_not_called()
        existing.resume.assert_not_called()

    def test_existing_suspended_sandbox_is_resumed(self):
        existing = _sandbox_mock(SandboxStatus.SUSPENDED)
        with (
            patch.object(Sandbox, "connect", return_value=existing),
            patch.object(Sandbox, "create") as create,
        ):
            result = Sandbox.get_or_create(_NAME)
        self.assertIs(result, existing)
        self.assertEqual(existing._bind_outcome, "resumed")
        existing.resume.assert_called_once_with(timeout=300.0)
        create.assert_not_called()

    def test_request_timeout_is_passed_to_resume(self):
        # get_or_create(name, request_timeout=5) must not fall back to
        # resume()'s independent 300-second default.
        existing = _sandbox_mock(SandboxStatus.SUSPENDED)
        with (
            patch.object(Sandbox, "connect", return_value=existing),
            patch.object(Sandbox, "create"),
        ):
            Sandbox.get_or_create(_NAME, request_timeout=17.0)
        existing.resume.assert_called_once_with(timeout=17.0)

    def test_resume_false_skips_resume(self):
        existing = _sandbox_mock(SandboxStatus.SUSPENDED)
        with (
            patch.object(Sandbox, "connect", return_value=existing),
            patch.object(Sandbox, "create"),
        ):
            result = Sandbox.get_or_create(_NAME, resume=False)
        self.assertIs(result, existing)
        # No resume happened, so the outcome is a plain attach even though
        # the sandbox is suspended.
        self.assertEqual(existing._bind_outcome, "attached")
        existing.resume.assert_not_called()
        # The info() call stays even with resume=False: it is the existence
        # check when connect skipped resolution (explicit proxy_url), and it
        # is served from the connect cache otherwise.
        existing.info.assert_called_once_with()

    def test_unverified_connect_handle_missing_sandbox_is_created(self):
        # With an explicit proxy_url, connect skips resolution and returns a
        # handle for a name that may not exist. The 404 then surfaces from
        # info(); get_or_create must treat it as "name is free" and create.
        dead = MagicMock(spec=Sandbox)
        dead.info.side_effect = SandboxNotFoundError(_NAME)
        created = _sandbox_mock(SandboxStatus.RUNNING)
        with (
            patch.object(Sandbox, "connect", return_value=dead),
            patch.object(Sandbox, "create", return_value=created) as create,
        ):
            result = Sandbox.get_or_create(_NAME, proxy_url="http://proxy.example.test")
        self.assertIs(result, created)
        create.assert_called_once()
        self.assertEqual(create.call_args.kwargs["name"], _NAME)
        dead.resume.assert_not_called()

    def test_missing_sandbox_is_created(self):
        created = _sandbox_mock(SandboxStatus.RUNNING)
        with (
            patch.object(
                Sandbox, "connect", side_effect=SandboxNotFoundError(_NAME)
            ) as connect,
            patch.object(Sandbox, "create", return_value=created) as create,
        ):
            result = Sandbox.get_or_create(_NAME, image="my-agent")
        self.assertIs(result, created)
        self.assertEqual(created._bind_outcome, "created")
        self.assertEqual(connect.call_count, 1)
        create.assert_called_once()
        self.assertEqual(create.call_args.kwargs["name"], _NAME)
        self.assertEqual(create.call_args.kwargs["image"], "my-agent")
        # A fresh create is already running; no info/resume round-trip.
        created.info.assert_not_called()
        created.resume.assert_not_called()

    def test_lost_create_race_attaches_to_winner(self):
        winner = _sandbox_mock(SandboxStatus.RUNNING)
        with (
            patch.object(
                Sandbox,
                "connect",
                side_effect=[SandboxNotFoundError(_NAME), winner],
            ) as connect,
            patch.object(
                Sandbox,
                "create",
                side_effect=RemoteAPIError(409, "name is currently claimed"),
            ) as create,
            patch("tensorlake.sandbox.sandbox.time.sleep") as sleep,
        ):
            result = Sandbox.get_or_create(_NAME)
        self.assertIs(result, winner)
        self.assertEqual(connect.call_count, 2)
        self.assertEqual(create.call_count, 1)
        self.assertEqual(sleep.call_count, 1)

    def test_lost_race_winner_suspended_is_resumed(self):
        winner = _sandbox_mock(SandboxStatus.SUSPENDED)
        with (
            patch.object(
                Sandbox,
                "connect",
                side_effect=[SandboxNotFoundError(_NAME), winner],
            ),
            patch.object(
                Sandbox,
                "create",
                side_effect=RemoteAPIError(409, "name is currently claimed"),
            ),
            patch("tensorlake.sandbox.sandbox.time.sleep"),
        ):
            result = Sandbox.get_or_create(_NAME)
        self.assertIs(result, winner)
        winner.resume.assert_called_once_with(timeout=300.0)

    def test_suspending_sandbox_settles_before_resume(self):
        # The server rejects resume while a suspend is still in progress,
        # so get_or_create must wait until the sandbox is Suspended before
        # it calls resume().
        winner = _sandbox_mock(SandboxStatus.SUSPENDING)
        # info() serves the cached SUSPENDING info from connect(); the wait
        # loop polls the fresh ``status`` property instead.
        type(winner).status = PropertyMock(
            side_effect=[
                SandboxStatus.SUSPENDING,
                SandboxStatus.SUSPENDING,
                SandboxStatus.SUSPENDED,
            ]
        )
        with (
            patch.object(Sandbox, "connect", return_value=winner),
            patch.object(Sandbox, "create") as create,
            patch("tensorlake.sandbox.sandbox.time.sleep"),
        ):
            result = Sandbox.get_or_create(_NAME)
        self.assertIs(result, winner)
        winner.resume.assert_called_once_with(timeout=300.0)
        create.assert_not_called()

    def test_suspending_sandbox_resumed_elsewhere_refreshes_routing(self):
        # Another caller resumes the sandbox while this one waits for the
        # suspend to settle: skip resume() (the sandbox is already coming
        # back) and refresh proxy routing instead, because the routing from
        # before the suspend is stale.
        winner = _sandbox_mock(SandboxStatus.SUSPENDING)
        type(winner).status = PropertyMock(
            side_effect=[
                SandboxStatus.SUSPENDING,
                SandboxStatus.RUNNING,
            ]
        )
        running_info = SimpleNamespace(status=SandboxStatus.RUNNING)
        winner._fresh_running_info_for_rebind.return_value = running_info
        with (
            patch.object(Sandbox, "connect", return_value=winner),
            patch.object(Sandbox, "create"),
            patch("tensorlake.sandbox.sandbox.time.sleep"),
        ):
            result = Sandbox.get_or_create(_NAME)
        self.assertIs(result, winner)
        # The sandbox was suspended when found and is running on return; the
        # outcome describes what happened to the sandbox, not who sent the
        # resume request.
        self.assertEqual(winner._bind_outcome, "resumed")
        winner.resume.assert_not_called()
        winner._fresh_running_info_for_rebind.assert_called_once()
        winner._rebind_proxy.assert_called_once_with(running_info)

    def test_suspending_sandbox_terminates_then_name_is_recreated(self):
        # The sandbox dies while suspending: the name may be free again, so
        # the loop must retry and create.
        dying = _sandbox_mock(SandboxStatus.SUSPENDING)
        type(dying).status = PropertyMock(
            side_effect=[
                SandboxStatus.SUSPENDING,
                SandboxStatus.TERMINATED,
            ]
        )
        created = _sandbox_mock(SandboxStatus.RUNNING)
        with (
            patch.object(
                Sandbox,
                "connect",
                side_effect=[dying, SandboxNotFoundError(_NAME)],
            ),
            patch.object(Sandbox, "create", return_value=created) as create,
            patch("tensorlake.sandbox.sandbox.time.sleep"),
        ):
            result = Sandbox.get_or_create(_NAME)
        self.assertIs(result, created)
        dying.resume.assert_not_called()
        create.assert_called_once()
        self.assertEqual(create.call_args.kwargs["name"], _NAME)

    def test_pending_sandbox_is_waited_on(self):
        # Another caller claimed the name but its create is still starting:
        # get_or_create must block until Running and refresh proxy routing
        # instead of returning an unusable pending handle.
        pending = _sandbox_mock(SandboxStatus.PENDING)
        running_info = SimpleNamespace(status=SandboxStatus.RUNNING)
        pending._fresh_running_info_for_rebind.return_value = running_info
        with (
            patch.object(Sandbox, "connect", return_value=pending),
            patch.object(Sandbox, "create") as create,
        ):
            result = Sandbox.get_or_create(_NAME)
        self.assertIs(result, pending)
        self.assertEqual(pending._bind_outcome, "attached")
        create.assert_not_called()
        pending._fresh_running_info_for_rebind.assert_called_once()
        pending._rebind_proxy.assert_called_once_with(running_info)
        pending.resume.assert_not_called()

    def test_unroutable_pending_winner_is_waited_on(self):
        # The name's sandbox is still starting and has no sandbox_url, so
        # connect cannot build a proxy-backed handle. get_or_create must
        # poll until connect succeeds instead of leaking the routing error.
        running = _sandbox_mock(SandboxStatus.RUNNING)
        with (
            patch.object(
                Sandbox,
                "connect",
                side_effect=[
                    SandboxNotRoutableError("sb-1", SandboxStatus.PENDING),
                    running,
                ],
            ) as connect,
            patch.object(Sandbox, "create") as create,
            patch("tensorlake.sandbox.sandbox.time.sleep"),
        ):
            result = Sandbox.get_or_create(_NAME)
        self.assertIs(result, running)
        self.assertEqual(connect.call_count, 2)
        create.assert_not_called()
        running.resume.assert_not_called()

    def test_unroutable_winner_terminates_then_name_is_recreated(self):
        # The starting sandbox terminates before it becomes routable: the
        # name may be free again, so the loop must retry and create.
        created = _sandbox_mock(SandboxStatus.RUNNING)
        with (
            patch.object(
                Sandbox,
                "connect",
                side_effect=[
                    SandboxNotRoutableError("sb-1", SandboxStatus.PENDING),
                    SandboxNotRoutableError("sb-1", SandboxStatus.TERMINATED),
                    SandboxNotFoundError(_NAME),
                ],
            ),
            patch.object(Sandbox, "create", return_value=created) as create,
            patch("tensorlake.sandbox.sandbox.time.sleep"),
        ):
            result = Sandbox.get_or_create(_NAME)
        self.assertIs(result, created)
        create.assert_called_once()
        self.assertEqual(create.call_args.kwargs["name"], _NAME)

    def test_never_routable_winner_times_out(self):
        with (
            patch.object(
                Sandbox,
                "connect",
                side_effect=SandboxNotRoutableError("sb-1", SandboxStatus.PENDING),
            ),
            patch.object(Sandbox, "create") as create,
            patch("tensorlake.sandbox.sandbox.time.sleep"),
        ):
            with self.assertRaises(SandboxError) as ctx:
                Sandbox.get_or_create(_NAME, request_timeout=0.01)
        self.assertIn("connectable", str(ctx.exception))
        create.assert_not_called()

    def test_non_409_create_error_is_raised(self):
        with (
            patch.object(Sandbox, "connect", side_effect=SandboxNotFoundError(_NAME)),
            patch.object(
                Sandbox,
                "create",
                side_effect=RemoteAPIError(400, "Image 'nope' is not registered"),
            ),
        ):
            with self.assertRaises(RemoteAPIError) as ctx:
                Sandbox.get_or_create(_NAME, image="nope")
        self.assertEqual(ctx.exception.status_code, 400)

    def test_gives_up_when_name_stays_unattachable(self):
        # Every connect says "not found", every create says "name claimed":
        # the previous holder is stuck terminating. The loop must end with
        # a SandboxError instead of spinning forever.
        conflict = RemoteAPIError(409, "name is currently claimed")
        with (
            patch.object(
                Sandbox, "connect", side_effect=SandboxNotFoundError(_NAME)
            ) as connect,
            patch.object(Sandbox, "create", side_effect=conflict) as create,
            patch("tensorlake.sandbox.sandbox.time.sleep"),
        ):
            with self.assertRaises(SandboxError) as ctx:
                Sandbox.get_or_create(_NAME)
        self.assertIn(_NAME, str(ctx.exception))
        self.assertIs(ctx.exception.__cause__, conflict)
        self.assertEqual(connect.call_count, create.call_count)
        self.assertGreater(connect.call_count, 1)

    def test_pool_id_is_rejected(self):
        # A pool claim cannot carry a name on the wire, so a claimed sandbox
        # would be unnamed and every later get_or_create would claim another
        # one. The parameter must not exist.
        with (
            patch.object(Sandbox, "connect") as connect,
            patch.object(Sandbox, "create") as create,
        ):
            with self.assertRaises(TypeError):
                Sandbox.get_or_create(_NAME, pool_id="pool-1")
        connect.assert_not_called()
        create.assert_not_called()

    def test_connection_kwargs_are_forwarded(self):
        existing = _sandbox_mock(SandboxStatus.RUNNING)
        with patch.object(Sandbox, "connect", return_value=existing) as connect:
            Sandbox.get_or_create(
                _NAME,
                api_url="https://api.example.test",
                namespace="ns-1",
                request_timeout=17.0,
            )
        kwargs = connect.call_args.kwargs
        self.assertEqual(kwargs["api_url"], "https://api.example.test")
        self.assertEqual(kwargs["namespace"], "ns-1")
        self.assertEqual(kwargs["request_timeout"], 17.0)


class TestAsyncGetOrCreate(unittest.IsolatedAsyncioTestCase):
    async def test_existing_suspended_sandbox_is_resumed(self):
        existing = _async_sandbox_mock(SandboxStatus.SUSPENDED)
        with (
            patch.object(AsyncSandbox, "connect", AsyncMock(return_value=existing)),
            patch.object(AsyncSandbox, "create", AsyncMock()) as create,
        ):
            result = await AsyncSandbox.get_or_create(_NAME)
        self.assertIs(result, existing)
        self.assertEqual(existing._bind_outcome, "resumed")
        existing.resume.assert_awaited_once_with(timeout=300.0)
        create.assert_not_awaited()

    async def test_request_timeout_is_passed_to_resume(self):
        # Async mirror: the configured timeout must reach resume().
        existing = _async_sandbox_mock(SandboxStatus.SUSPENDED)
        with (
            patch.object(AsyncSandbox, "connect", AsyncMock(return_value=existing)),
            patch.object(AsyncSandbox, "create", AsyncMock()),
        ):
            await AsyncSandbox.get_or_create(_NAME, request_timeout=17.0)
        existing.resume.assert_awaited_once_with(timeout=17.0)

    async def test_suspending_sandbox_settles_before_resume(self):
        # Async mirror: wait until the suspend settles before resume().
        existing = _async_sandbox_mock(SandboxStatus.SUSPENDING)
        # info() serves the cached SUSPENDING info from connect(); the wait
        # loop awaits the fresh ``status()`` method instead.
        existing.status = AsyncMock(
            side_effect=[
                SandboxStatus.SUSPENDING,
                SandboxStatus.SUSPENDING,
                SandboxStatus.SUSPENDED,
            ]
        )
        with (
            patch.object(AsyncSandbox, "connect", AsyncMock(return_value=existing)),
            patch.object(AsyncSandbox, "create", AsyncMock()) as create,
            patch("tensorlake.sandbox.async_sandbox.asyncio.sleep", AsyncMock()),
        ):
            result = await AsyncSandbox.get_or_create(_NAME)
        self.assertIs(result, existing)
        self.assertEqual(existing._bind_outcome, "resumed")
        existing.resume.assert_awaited_once_with(timeout=300.0)
        create.assert_not_awaited()

    async def test_missing_sandbox_is_created(self):
        created = _async_sandbox_mock(SandboxStatus.RUNNING)
        with (
            patch.object(
                AsyncSandbox,
                "connect",
                AsyncMock(side_effect=SandboxNotFoundError(_NAME)),
            ),
            patch.object(
                AsyncSandbox, "create", AsyncMock(return_value=created)
            ) as create,
        ):
            result = await AsyncSandbox.get_or_create(_NAME, image="my-agent")
        self.assertIs(result, created)
        self.assertEqual(created._bind_outcome, "created")
        self.assertEqual(create.await_args.kwargs["name"], _NAME)

    async def test_unverified_connect_handle_missing_sandbox_is_created(self):
        dead = MagicMock(spec=AsyncSandbox)
        dead.info = AsyncMock(side_effect=SandboxNotFoundError(_NAME))
        dead.resume = AsyncMock()
        created = _async_sandbox_mock(SandboxStatus.RUNNING)
        with (
            patch.object(AsyncSandbox, "connect", AsyncMock(return_value=dead)),
            patch.object(
                AsyncSandbox, "create", AsyncMock(return_value=created)
            ) as create,
        ):
            result = await AsyncSandbox.get_or_create(
                _NAME, proxy_url="http://proxy.example.test"
            )
        self.assertIs(result, created)
        self.assertEqual(create.await_args.kwargs["name"], _NAME)
        dead.resume.assert_not_awaited()

    async def test_pending_sandbox_is_waited_on(self):
        pending = _async_sandbox_mock(SandboxStatus.PENDING)
        running_info = SimpleNamespace(status=SandboxStatus.RUNNING)
        pending._fresh_running_info_for_rebind = AsyncMock(return_value=running_info)
        with (
            patch.object(AsyncSandbox, "connect", AsyncMock(return_value=pending)),
            patch.object(AsyncSandbox, "create", AsyncMock()) as create,
        ):
            result = await AsyncSandbox.get_or_create(_NAME)
        self.assertIs(result, pending)
        self.assertEqual(pending._bind_outcome, "attached")
        create.assert_not_awaited()
        pending._fresh_running_info_for_rebind.assert_awaited_once()
        pending._rebind_proxy.assert_called_once_with(running_info)
        pending.resume.assert_not_awaited()

    async def test_unroutable_pending_winner_is_waited_on(self):
        running = _async_sandbox_mock(SandboxStatus.RUNNING)
        with (
            patch.object(
                AsyncSandbox,
                "connect",
                AsyncMock(
                    side_effect=[
                        SandboxNotRoutableError("sb-1", SandboxStatus.PENDING),
                        running,
                    ]
                ),
            ) as connect,
            patch.object(AsyncSandbox, "create", AsyncMock()) as create,
            patch("tensorlake.sandbox.async_sandbox.asyncio.sleep", AsyncMock()),
        ):
            result = await AsyncSandbox.get_or_create(_NAME)
        self.assertIs(result, running)
        self.assertEqual(connect.await_count, 2)
        create.assert_not_awaited()
        running.resume.assert_not_awaited()

    async def test_unroutable_winner_terminates_then_name_is_recreated(self):
        created = _async_sandbox_mock(SandboxStatus.RUNNING)
        with (
            patch.object(
                AsyncSandbox,
                "connect",
                AsyncMock(
                    side_effect=[
                        SandboxNotRoutableError("sb-1", SandboxStatus.PENDING),
                        SandboxNotRoutableError("sb-1", SandboxStatus.TERMINATED),
                        SandboxNotFoundError(_NAME),
                    ]
                ),
            ),
            patch.object(
                AsyncSandbox, "create", AsyncMock(return_value=created)
            ) as create,
            patch("tensorlake.sandbox.async_sandbox.asyncio.sleep", AsyncMock()),
        ):
            result = await AsyncSandbox.get_or_create(_NAME)
        self.assertIs(result, created)
        self.assertEqual(create.await_args.kwargs["name"], _NAME)

    async def test_never_routable_winner_times_out(self):
        with (
            patch.object(
                AsyncSandbox,
                "connect",
                AsyncMock(
                    side_effect=SandboxNotRoutableError("sb-1", SandboxStatus.PENDING)
                ),
            ),
            patch.object(AsyncSandbox, "create", AsyncMock()) as create,
            patch("tensorlake.sandbox.async_sandbox.asyncio.sleep", AsyncMock()),
        ):
            with self.assertRaises(SandboxError) as ctx:
                await AsyncSandbox.get_or_create(_NAME, request_timeout=0.01)
        self.assertIn("connectable", str(ctx.exception))
        create.assert_not_awaited()

    async def test_pool_id_is_rejected(self):
        with (
            patch.object(AsyncSandbox, "connect", AsyncMock()) as connect,
            patch.object(AsyncSandbox, "create", AsyncMock()) as create,
        ):
            with self.assertRaises(TypeError):
                await AsyncSandbox.get_or_create(_NAME, pool_id="pool-1")
        connect.assert_not_awaited()
        create.assert_not_awaited()

    async def test_lost_create_race_attaches_to_winner(self):
        winner = _async_sandbox_mock(SandboxStatus.RUNNING)
        with (
            patch.object(
                AsyncSandbox,
                "connect",
                AsyncMock(side_effect=[SandboxNotFoundError(_NAME), winner]),
            ) as connect,
            patch.object(
                AsyncSandbox,
                "create",
                AsyncMock(side_effect=RemoteAPIError(409, "name is currently claimed")),
            ),
            patch(
                "tensorlake.sandbox.async_sandbox.asyncio.sleep", AsyncMock()
            ) as sleep,
        ):
            result = await AsyncSandbox.get_or_create(_NAME)
        self.assertIs(result, winner)
        self.assertEqual(connect.await_count, 2)
        self.assertEqual(sleep.await_count, 1)


if __name__ == "__main__":
    unittest.main()
