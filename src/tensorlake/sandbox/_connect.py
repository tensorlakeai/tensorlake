"""Readiness for explicit connections; never retries a user command."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from typing import TYPE_CHECKING, Any, Awaitable, Callable, TypeVar

if TYPE_CHECKING:
    from .async_client import AsyncSandboxClient
    from .async_sandbox import AsyncSandbox
    from .client import SandboxClient
    from .sandbox import Sandbox

T = TypeVar("T")

from .exceptions import (
    RemoteAPIError,
    SandboxConnectionError,
    SandboxError,
    SandboxNotRoutableError,
)
from .models import SandboxStatus

logger = logging.getLogger(__name__)


def retryable_health_error(error: Exception) -> bool:
    # Health is a read-only request. An ambiguous transport failure is safe
    # to retry here, unlike a command whose execution may already have begun.
    if isinstance(error, SandboxConnectionError):
        return True
    if not isinstance(error, RemoteAPIError):
        return False
    if error.status_code in (502, 503, 504):
        return True
    if error.status_code != 400:
        return False
    try:
        payload = json.loads(error.message)
    except (TypeError, ValueError):
        return False
    return isinstance(payload, dict) and payload.get("code") == "SANDBOX_NOT_RUNNING"


def deadline_for(timeout: float) -> float:
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("request_timeout must be a finite positive number")
    return time.monotonic() + timeout


def check_terminal(info: Any) -> None:
    if info.status == SandboxStatus.TERMINATED:
        raise SandboxError(
            f"Sandbox {info.sandbox_id} is terminated; restart it explicitly"
        )


def _remaining(deadline: float, identifier: str, timeout: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise SandboxError(
            f"Sandbox {identifier} did not become ready within {timeout}s"
        )
    return remaining


def connect_ready(
    client: SandboxClient,
    identifier: str,
    timeout: float,
    *,
    request_timeout: float | None = None,
    **options: Any,
) -> Sandbox:
    deadline = deadline_for(timeout)
    next_log = 0.0
    canonical = identifier

    def call(operation: Callable[[SandboxClient], T]) -> T:
        scoped = client._with_request_timeout(_remaining(deadline, identifier, timeout))
        try:
            return operation(scoped)
        finally:
            if scoped is not client:
                scoped.close()

    while True:
        _remaining(deadline, identifier, timeout)
        if time.monotonic() >= next_log:
            logger.info("Waiting for sandbox %s to accept commands", identifier)
            next_log = time.monotonic() + 5
        info = call(lambda scoped: scoped.get(canonical))
        canonical = info.sandbox_id
        check_terminal(info)
        if info.status == SandboxStatus.SUSPENDED:
            try:
                call(lambda scoped: scoped.resume(info.sandbox_id, wait=False))
            except RemoteAPIError as error:
                # Preserve quota/auth errors unless another caller won resume.
                if error.status_code not in (400, 409):
                    raise
                current = call(lambda scoped: scoped.get(info.sandbox_id))
                if current.status not in (SandboxStatus.PENDING, SandboxStatus.RUNNING):
                    raise
            info = call(lambda scoped: scoped.get(info.sandbox_id))
            check_terminal(info)
        if info.status != SandboxStatus.RUNNING:
            time.sleep(min(0.2, max(0, deadline - time.monotonic())))
            continue
        probe = None
        try:
            probe = client.connect(
                identifier,
                request_timeout=min(5.0, _remaining(deadline, identifier, timeout)),
                _routing_info=info.value,
                **options,
            )
            healthy = probe.health().healthy
            fresh = call(lambda scoped: scoped.get(probe.sandbox_id))
            check_terminal(fresh)
            if healthy and fresh.status == SandboxStatus.RUNNING:
                _remaining(deadline, identifier, timeout)
                # Return fresh routing with the user's original timeout. The
                # remaining readiness budget must not constrain later commands.
                return client.connect(
                    fresh.sandbox_id,
                    request_timeout=request_timeout,
                    _routing_info=fresh.value,
                    **options,
                )
        except SandboxNotRoutableError:
            pass
        except (RemoteAPIError, SandboxConnectionError) as error:
            if not retryable_health_error(error):
                raise
        finally:
            if probe is not None:
                probe.close()
        time.sleep(min(0.2, max(0, deadline - time.monotonic())))


async def connect_ready_async(
    client: AsyncSandboxClient,
    identifier: str,
    timeout: float,
    *,
    request_timeout: float | None = None,
    **options: Any,
) -> AsyncSandbox:
    deadline = deadline_for(timeout)
    next_log = 0.0
    canonical = identifier

    async def call(operation: Callable[[AsyncSandboxClient], Awaitable[T]]) -> T:
        scoped = client._with_request_timeout(_remaining(deadline, identifier, timeout))
        try:
            return await operation(scoped)
        finally:
            if scoped is not client:
                await scoped.close()

    while True:
        _remaining(deadline, identifier, timeout)
        if time.monotonic() >= next_log:
            logger.info("Waiting for sandbox %s to accept commands", identifier)
            next_log = time.monotonic() + 5
        info = await call(lambda scoped: scoped.get(canonical))
        canonical = info.sandbox_id
        check_terminal(info)
        if info.status == SandboxStatus.SUSPENDED:
            try:
                await call(lambda scoped: scoped.resume(info.sandbox_id, wait=False))
            except RemoteAPIError as error:
                if error.status_code not in (400, 409):
                    raise
                current = await call(lambda scoped: scoped.get(info.sandbox_id))
                if current.status not in (SandboxStatus.PENDING, SandboxStatus.RUNNING):
                    raise
            info = await call(lambda scoped: scoped.get(info.sandbox_id))
            check_terminal(info)
        if info.status != SandboxStatus.RUNNING:
            await asyncio.sleep(min(0.2, max(0, deadline - time.monotonic())))
            continue
        probe = None
        try:
            probe = await client.connect(
                identifier,
                request_timeout=min(5.0, _remaining(deadline, identifier, timeout)),
                _routing_info=info.value,
                **options,
            )
            healthy = (await probe.health()).healthy
            fresh = await call(lambda scoped: scoped.get(probe.sandbox_id))
            check_terminal(fresh)
            if healthy and fresh.status == SandboxStatus.RUNNING:
                _remaining(deadline, identifier, timeout)
                return await client.connect(
                    fresh.sandbox_id,
                    request_timeout=request_timeout,
                    _routing_info=fresh.value,
                    **options,
                )
        except SandboxNotRoutableError:
            pass
        except (RemoteAPIError, SandboxConnectionError) as error:
            if not retryable_health_error(error):
                raise
        finally:
            if probe is not None:
                probe.close()
        await asyncio.sleep(min(0.2, max(0, deadline - time.monotonic())))
