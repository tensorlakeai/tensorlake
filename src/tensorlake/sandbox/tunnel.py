"""TCP tunnels backed by the shared Rust transport used by the CLI and SDKs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse, urlunparse

from .exceptions import SandboxError

try:
    from tensorlake._cloud_sdk import CloudTcpTunnel as _RustTcpTunnel
except ImportError:
    try:
        from _cloud_sdk import CloudTcpTunnel as _RustTcpTunnel
    except ImportError:
        _RustTcpTunnel = None

DEFAULT_LOCAL_HOST = "127.0.0.1"
DEFAULT_TUNNEL_CONNECT_TIMEOUT = 10.0


@dataclass(frozen=True)
class TunnelAddress:
    """Local address a tunnel listens on."""

    host: str
    port: int


def build_tunnel_ws_url(base_url: str, remote_port: int) -> str:
    """Return the WebSocket URL for a TCP tunnel to ``remote_port``."""
    parsed = urlparse(base_url)
    scheme = "wss" if parsed.scheme == "https" else "ws"
    return urlunparse(
        parsed._replace(
            scheme=scheme,
            path="/api/v1/tunnels/tcp",
            query=f"port={remote_port}",
            fragment="",
        )
    )


def _validate_port(port: Any, label: str, allow_zero: bool = False) -> int:
    if isinstance(port, bool) or not isinstance(port, int):
        raise SandboxError(f"{label} must be an integer, got {port!r}")
    if allow_zero and port == 0:
        return port
    if port < 1 or port > 65535:
        raise SandboxError(f"{label} must be between 1 and 65535, got {port}")
    return port


def _validate_timeout(seconds: Any) -> float:
    """Return ``seconds`` as a positive float.

    Zero is rejected because it would expire the connection deadline immediately.
    """
    try:
        value = float(seconds)
    except (TypeError, ValueError):
        raise SandboxError(f"timeout must be > 0 seconds, got {seconds!r}") from None
    if value != value or value <= 0:  # NaN, zero or negative
        raise SandboxError(f"timeout must be > 0 seconds, got {seconds!r}")
    return value


class _TunnelHandle:
    def __init__(
        self,
        *,
        base_url: str,
        ws_headers: dict[str, str],
        remote_port: int,
        local_host: str = DEFAULT_LOCAL_HOST,
        local_port: int | None = None,
        connect_timeout: float = DEFAULT_TUNNEL_CONNECT_TIMEOUT,
    ):
        self._remote_port = _validate_port(remote_port, "remote port")
        self._local_host = local_host
        self._local_port = _validate_port(
            self._remote_port if local_port is None else local_port,
            "local port",
            allow_zero=True,
        )
        self._args = (
            base_url,
            dict(ws_headers),
            self._remote_port,
            local_host,
            self._local_port,
            _validate_timeout(connect_timeout),
        )
        self._native = None
        self._closed = False

    @property
    def remote_port(self) -> int:
        return self._remote_port

    @property
    def local_host(self) -> str:
        return self._local_host

    @property
    def local_port(self) -> int:
        return self._local_port

    @property
    def closed(self) -> bool:
        return self._closed or (self._native is not None and self._native.closed)

    def address(self) -> TunnelAddress:
        return TunnelAddress(host=self._local_host, port=self._local_port)

    def _require_native(self) -> None:
        if _RustTcpTunnel is None:
            raise SandboxError(
                "TCP tunnels require the Tensorlake native extension; reinstall or rebuild the SDK"
            )


class TcpTunnel(_TunnelHandle):
    """Listen locally and forward connections into a sandbox using Rust.

    Create with ``Sandbox.create_tunnel``; close explicitly or use a context manager.
    """

    def __init__(
        self,
        *,
        base_url: str,
        ws_headers: dict[str, str],
        remote_port: int,
        local_host: str = DEFAULT_LOCAL_HOST,
        local_port: int | None = None,
        connect_timeout: float = DEFAULT_TUNNEL_CONNECT_TIMEOUT,
    ):
        super().__init__(
            base_url=base_url,
            ws_headers=ws_headers,
            remote_port=remote_port,
            local_host=local_host,
            local_port=local_port,
            connect_timeout=connect_timeout,
        )
        self._require_native()
        try:
            self._native = _RustTcpTunnel(*self._args)
        except Exception as e:
            raise SandboxError(str(e)) from e
        self._local_port = self._native.local_port

    @classmethod
    def listen(
        cls,
        *,
        base_url: str,
        ws_headers: dict[str, str],
        remote_port: int,
        local_host: str = DEFAULT_LOCAL_HOST,
        local_port: int | None = None,
        connect_timeout: float = DEFAULT_TUNNEL_CONNECT_TIMEOUT,
    ) -> "TcpTunnel":
        """Bind the local listener and start accepting connections."""
        return cls(
            base_url=base_url,
            ws_headers=ws_headers,
            remote_port=remote_port,
            local_host=local_host,
            local_port=local_port,
            connect_timeout=connect_timeout,
        )

    def close(self) -> None:
        """Stop listening and wait until all active connections are closed."""
        if self._native is not None:
            self._native.close()
        self._closed = True

    def __enter__(self) -> "TcpTunnel":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


class AsyncTcpTunnel(_TunnelHandle):
    """Asyncio interface to the same Rust transport as :class:`TcpTunnel`."""

    @classmethod
    async def listen(
        cls,
        *,
        base_url: str,
        ws_headers: dict[str, str],
        remote_port: int,
        local_host: str = DEFAULT_LOCAL_HOST,
        local_port: int | None = None,
        connect_timeout: float = DEFAULT_TUNNEL_CONNECT_TIMEOUT,
    ) -> "AsyncTcpTunnel":
        """Bind the local listener without blocking the Python event loop."""
        tunnel = cls(
            base_url=base_url,
            ws_headers=ws_headers,
            remote_port=remote_port,
            local_host=local_host,
            local_port=local_port,
            connect_timeout=connect_timeout,
        )
        tunnel._require_native()
        try:
            tunnel._native = await _RustTcpTunnel.listen_async(*tunnel._args)
        except Exception as e:
            raise SandboxError(str(e)) from e
        tunnel._local_port = tunnel._native.local_port
        return tunnel

    async def close(self) -> None:
        """Stop listening and wait until all active connections are closed."""
        self._closed = True
        if self._native is not None:
            await self._native.close_async()

    async def __aenter__(self) -> "AsyncTcpTunnel":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()
