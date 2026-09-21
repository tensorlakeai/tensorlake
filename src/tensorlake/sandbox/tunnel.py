"""TCP tunnels from a local port to a port inside a sandbox.

Provides :class:`TcpTunnel` (sync, thread-based) and :class:`AsyncTcpTunnel`
(asyncio-based). Each accepted local connection opens one WebSocket to the
sandbox proxy at ``/api/v1/tunnels/tcp?port=<remote_port>`` and relays raw
bytes in both directions as binary frames.
"""

from __future__ import annotations

import asyncio
import errno
import socket
import threading
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse, urlunparse

from .exceptions import SandboxConnectionError, SandboxError
from .pty import _prepare_async_ws_connect, _prepare_sync_ws_connect

try:
    import websocket
except Exception:  # pragma: no cover - exercised via runtime guard
    websocket = None

try:
    import websockets.asyncio.client as _async_ws_client
except Exception:  # pragma: no cover - exercised via runtime guard
    _async_ws_client = None

DEFAULT_LOCAL_HOST = "127.0.0.1"
DEFAULT_TUNNEL_CONNECT_TIMEOUT = 10.0
WEBSOCKET_KEEPALIVE_INTERVAL = 15.0
TUNNEL_BUFFER_SIZE = 16 * 1024
# How long a relay waits for the peer to finish a close handshake or drain
# buffered output before it aborts the connection.
TUNNEL_CLOSE_TIMEOUT = 5.0
# How long the sync accept loop pauses after the process runs out of file
# descriptors or buffers before it tries to accept again.
TUNNEL_ACCEPT_RETRY_DELAY = 0.5

# accept() errors that concern one incoming connection, not the listener.
_ACCEPT_TRANSIENT_ERRORS = (ConnectionAbortedError, ConnectionResetError)
# accept() errors caused by resource exhaustion. Retry after a pause.
_ACCEPT_RESOURCE_ERRNOS = frozenset(
    getattr(errno, name)
    for name in ("EMFILE", "ENFILE", "ENOBUFS", "ENOMEM")
    if hasattr(errno, name)
)

# WebSocket opcodes (RFC 6455). Defined here so the sync relay does not
# depend on ``websocket.ABNF`` being importable.
_WS_OPCODE_TEXT = 0x1
_WS_OPCODE_BINARY = 0x2
_WS_OPCODE_CLOSE = 0x8

# Read timeouts raised by websocket-client. ``socket.timeout`` is an alias of
# ``TimeoutError`` on Python 3.10+, but the library has its own exception type.
_WS_TIMEOUT_EXCEPTIONS: tuple[type[BaseException], ...] = (TimeoutError,) + (
    (websocket.WebSocketTimeoutException,) if websocket is not None else ()
)


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

    Zero is rejected: ``websocket.create_connection`` treats a zero timeout as
    non-blocking and fails at once, and ``websockets`` times out at once.
    """
    try:
        value = float(seconds)
    except (TypeError, ValueError):
        raise SandboxError(f"timeout must be > 0 seconds, got {seconds!r}") from None
    if value != value or value <= 0:  # NaN, zero or negative
        raise SandboxError(f"timeout must be > 0 seconds, got {seconds!r}")
    return value


def _socket_family(host: str) -> int:
    return socket.AF_INET6 if ":" in host else socket.AF_INET


def _set_nodelay(sock: socket.socket) -> None:
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    except OSError:
        pass


def _close_quietly(sock: socket.socket | None) -> None:
    if sock is None:
        return
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    try:
        sock.close()
    except OSError:
        pass


def _ws_shutdown_quietly(ws: Any) -> None:
    if ws is None:
        return
    # Shut the underlying socket down first so a reader blocked in recv()
    # on another thread wakes up now instead of at its next read timeout.
    sock = getattr(ws, "sock", None)
    if sock is not None:
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass
    try:
        ws.shutdown()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Sync tunnel
# ---------------------------------------------------------------------------


class _Relay:
    """One accepted local connection and its WebSocket."""

    def __init__(self, local_sock: socket.socket):
        self.local_sock: socket.socket | None = local_sock
        self.ws: Any = None
        self._closed = False
        self._closed_event = threading.Event()
        self._lock = threading.Lock()

    def attach_ws(self, ws: Any) -> bool:
        """Register ``ws`` with this relay.

        Returns ``False`` and shuts ``ws`` down if the relay was closed while
        the WebSocket was still connecting, so no connection outlives
        :meth:`TcpTunnel.close`.
        """
        with self._lock:
            if not self._closed:
                self.ws = ws
                return True
        _ws_shutdown_quietly(ws)
        return False

    def close(self) -> None:
        with self._lock:
            self._closed = True
            local_sock, self.local_sock = self.local_sock, None
            ws, self.ws = self.ws, None
        _close_quietly(local_sock)
        _ws_shutdown_quietly(ws)
        self._closed_event.set()

    def wait_closed(self, timeout: float) -> bool:
        """Block until :meth:`close` ran or ``timeout`` seconds passed."""
        return self._closed_event.wait(timeout)


class TcpTunnel:
    """Listen on a local TCP port and forward each connection into a sandbox.

    Create one with :meth:`Sandbox.create_tunnel`. Use it as a context manager
    or call :meth:`close` when done.
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
        self._remote_port = _validate_port(remote_port, "remote port")
        self._local_host = local_host
        requested_port = _validate_port(
            self._remote_port if local_port is None else local_port,
            "local port",
            allow_zero=True,
        )
        self._connect_timeout = _validate_timeout(connect_timeout)
        self._ws_url = build_tunnel_ws_url(base_url, self._remote_port)
        self._ws_headers = dict(ws_headers)

        self._relays: set[_Relay] = set()
        self._lock = threading.Lock()
        self._closed = False
        self._accept_thread: threading.Thread | None = None

        try:
            listener = socket.create_server(
                (self._local_host, requested_port),
                family=_socket_family(self._local_host),
            )
        except OSError as e:
            raise SandboxError(
                f"failed to bind local tunnel listener on "
                f"{self._local_host}:{requested_port}: {e}"
            ) from e
        self._listener: socket.socket | None = listener
        self._local_port = int(listener.getsockname()[1])

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
        if websocket is None:
            raise SandboxError(
                "websocket-client is required for TCP tunnels. "
                "Install the Tensorlake package with its runtime dependencies."
            )
        tunnel = cls(
            base_url=base_url,
            ws_headers=ws_headers,
            remote_port=remote_port,
            local_host=local_host,
            local_port=local_port,
            connect_timeout=connect_timeout,
        )
        tunnel._start()
        return tunnel

    # --- Public API ---

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
        return self._closed

    def address(self) -> TunnelAddress:
        return TunnelAddress(host=self._local_host, port=self._local_port)

    def __enter__(self) -> "TcpTunnel":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def close(self) -> None:
        """Stop listening and close every active connection."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            listener, self._listener = self._listener, None
            relays = list(self._relays)
            self._relays.clear()

        _close_quietly(listener)
        for relay in relays:
            relay.close()

        thread = self._accept_thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=1.0)

    # --- Internals ---

    def _start(self) -> None:
        self._accept_thread = threading.Thread(
            target=self._accept_loop,
            name=f"tensorlake-tunnel-{self._local_port}->{self._remote_port}",
            daemon=True,
        )
        self._accept_thread.start()

    def _accept_loop(self) -> None:
        listener = self._listener
        if listener is None:
            return
        while not self._closed:
            try:
                local_sock, _ = listener.accept()
            except _ACCEPT_TRANSIENT_ERRORS:
                # The peer went away before accept() returned. The listener
                # is fine; wait for the next connection.
                continue
            except OSError as e:
                if self._closed:
                    break
                if e.errno in _ACCEPT_RESOURCE_ERRNOS:
                    # Out of file descriptors or buffers. Retry after a pause
                    # instead of tearing the tunnel down.
                    time.sleep(TUNNEL_ACCEPT_RETRY_DELAY)
                    continue
                # The listener itself failed. Close the tunnel so ``closed``
                # reports the truth and active relays do not outlive it.
                self.close()
                break
            relay = _Relay(local_sock)
            with self._lock:
                if self._closed:
                    relay.close()
                    break
                self._relays.add(relay)
            threading.Thread(
                target=self._handle_connection,
                args=(relay,),
                name=f"tensorlake-tunnel-relay-{self._remote_port}",
                daemon=True,
            ).start()

    def _open_websocket(self) -> Any:
        header, connect_kwargs = _prepare_sync_ws_connect(self._ws_headers)
        try:
            ws = websocket.create_connection(
                self._ws_url,
                header=header,
                timeout=self._connect_timeout,
                enable_multithread=True,
                **connect_kwargs,
            )
        except Exception as e:
            raise SandboxConnectionError(
                f"tunnel websocket connection failed: {e}"
            ) from e
        # The connect timeout also governs reads. Switch to the keep-alive
        # interval so an idle relay pings instead of failing.
        try:
            ws.settimeout(WEBSOCKET_KEEPALIVE_INTERVAL)
        except Exception:
            pass
        return ws

    def _handle_connection(self, relay: _Relay) -> None:
        local_sock = relay.local_sock
        if local_sock is None:
            return
        _set_nodelay(local_sock)

        try:
            ws = self._open_websocket()
        except Exception:
            self._forget(relay)
            relay.close()
            return
        if not relay.attach_ws(ws):
            # The tunnel closed while the WebSocket was connecting. The relay
            # is already closed and forgotten, and the WebSocket is shut down.
            return

        writer = threading.Thread(
            target=self._pump_local_to_ws,
            args=(relay, local_sock, ws),
            name=f"tensorlake-tunnel-writer-{self._remote_port}",
            daemon=True,
        )
        writer.start()
        try:
            self._pump_ws_to_local(ws, local_sock)
        finally:
            self._forget(relay)
            relay.close()
            writer.join(timeout=1.0)

    def _forget(self, relay: _Relay) -> None:
        with self._lock:
            self._relays.discard(relay)

    @staticmethod
    def _pump_local_to_ws(relay: _Relay, local_sock: socket.socket, ws: Any) -> None:
        while True:
            try:
                chunk = local_sock.recv(TUNNEL_BUFFER_SIZE)
            except OSError:
                break
            if not chunk:
                break
            try:
                ws.send_binary(chunk)
            except Exception:
                # The websocket may still be readable after a send failure
                # (for example a send timeout). Close the whole relay so the
                # reader thread stops and the local client sees the failure
                # instead of writing into a socket nobody drains.
                relay.close()
                return
        # Local side finished. Tell the sandbox by sending a close frame;
        # the reader thread completes the handshake and closes the relay.
        try:
            ws.send_close()
        except Exception:
            relay.close()
            return
        # ``send_close`` only writes the frame. If the proxy never answers,
        # the reader would ping forever and keep this relay, its sockets and
        # its thread alive. Bound the handshake and force the close.
        if not relay.wait_closed(TUNNEL_CLOSE_TIMEOUT):
            relay.close()

    @staticmethod
    def _pump_ws_to_local(ws: Any, local_sock: socket.socket) -> None:
        # True after a keep-alive ping until any frame arrives. Two read
        # timeouts in a row with no frame in between mean the peer is gone.
        ping_unanswered = False
        while True:
            try:
                # Ask for control frames too so a pong counts as a sign of
                # life. websocket-client swallows them otherwise.
                opcode, data = ws.recv_data(control_frame=True)
            except _WS_TIMEOUT_EXCEPTIONS:
                if ping_unanswered:
                    # The peer did not answer the last ping within a whole
                    # keep-alive interval. Treat it as dead instead of
                    # waiting for TCP retransmission to give up.
                    return
                try:
                    ws.ping()
                except Exception:
                    return
                ping_unanswered = True
                continue
            except Exception:
                return

            ping_unanswered = False
            if opcode == _WS_OPCODE_BINARY:
                try:
                    local_sock.sendall(data)
                except OSError:
                    return
            elif opcode == _WS_OPCODE_CLOSE:
                return
            elif opcode == _WS_OPCODE_TEXT:
                # The tunnel protocol is binary only.
                return
            # Ping and pong frames only prove the peer is alive. websocket-client
            # already answered any ping.


# ---------------------------------------------------------------------------
# Async tunnel
# ---------------------------------------------------------------------------


async def _close_with_deadline(
    close: Awaitable[Any], abort: Callable[[], None], timeout: float
) -> None:
    """Await ``close`` for at most ``timeout`` seconds, then ``abort``."""
    try:
        await asyncio.wait_for(close, timeout)
    except asyncio.CancelledError:
        abort()
        raise
    except Exception:
        abort()


def _abort_ws_transport(ws: Any) -> None:
    """Drop the WebSocket transport without flushing buffered output."""
    transport = getattr(ws, "transport", None)
    if transport is None:
        return
    try:
        transport.abort()
    except Exception:
        pass


async def _close_ws(ws: Any) -> None:
    """Close ``ws`` gracefully, aborting if the proxy does not drain in time.

    ``websockets`` ``close()`` writes the close frame and then waits for the
    outgoing buffer to drain before it starts its own close timeout. If the
    proxy stops reading, that drain never completes.
    """
    await _close_with_deadline(
        ws.close(), lambda: _abort_ws_transport(ws), TUNNEL_CLOSE_TIMEOUT
    )


def _abort_writer(writer: asyncio.StreamWriter) -> None:
    """Close ``writer`` immediately, discarding any buffered output."""
    try:
        writer.transport.abort()
    except Exception:
        try:
            writer.close()
        except Exception:
            pass


async def _close_writer(writer: asyncio.StreamWriter) -> None:
    """Close ``writer`` gracefully, aborting if the peer does not drain in time.

    ``StreamWriter.close()`` flushes buffered output before the transport
    reports closed, so ``wait_closed()`` blocks for as long as the local client
    refuses to read.
    """
    writer.close()
    await _close_with_deadline(
        writer.wait_closed(), lambda: _abort_writer(writer), TUNNEL_CLOSE_TIMEOUT
    )


class AsyncTcpTunnel:
    """asyncio counterpart of :class:`TcpTunnel`.

    Create one with :meth:`AsyncSandbox.create_tunnel`. Use it as an async
    context manager or ``await tunnel.close()`` when done.
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
        self._remote_port = _validate_port(remote_port, "remote port")
        self._local_host = local_host
        self._requested_port = _validate_port(
            self._remote_port if local_port is None else local_port,
            "local port",
            allow_zero=True,
        )
        self._connect_timeout = _validate_timeout(connect_timeout)
        self._ws_url = build_tunnel_ws_url(base_url, self._remote_port)
        self._ws_headers = dict(ws_headers)

        self._server: asyncio.AbstractServer | None = None
        self._local_port = self._requested_port
        self._relays: set[asyncio.Task] = set()
        self._closed = False

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
        """Bind the local listener and start accepting connections."""
        if _async_ws_client is None:
            raise SandboxError(
                "websockets is required for async TCP tunnels. "
                "Install the Tensorlake package with its runtime dependencies."
            )
        tunnel = cls(
            base_url=base_url,
            ws_headers=ws_headers,
            remote_port=remote_port,
            local_host=local_host,
            local_port=local_port,
            connect_timeout=connect_timeout,
        )
        await tunnel._start()
        return tunnel

    # --- Public API ---

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
        return self._closed

    def address(self) -> TunnelAddress:
        return TunnelAddress(host=self._local_host, port=self._local_port)

    async def __aenter__(self) -> "AsyncTcpTunnel":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def close(self) -> None:
        """Stop listening and close every active connection."""
        if self._closed:
            return
        self._closed = True

        server, self._server = self._server, None
        if server is not None:
            server.close()

        relays = list(self._relays)
        self._relays.clear()
        for task in relays:
            task.cancel()
        if relays:
            await asyncio.gather(*relays, return_exceptions=True)

        if server is not None:
            try:
                await server.wait_closed()
            except Exception:
                pass

    # --- Internals ---

    async def _start(self) -> None:
        try:
            self._server = await asyncio.start_server(
                self._on_connection,
                host=self._local_host,
                port=self._requested_port,
                family=_socket_family(self._local_host),
            )
        except OSError as e:
            raise SandboxError(
                f"failed to bind local tunnel listener on "
                f"{self._local_host}:{self._requested_port}: {e}"
            ) from e
        sockets = self._server.sockets or ()
        if sockets:
            self._local_port = int(sockets[0].getsockname()[1])

    async def _on_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        if self._closed:
            _abort_writer(writer)
            return
        task = asyncio.current_task()
        if task is not None:
            self._relays.add(task)
        try:
            await self._relay(reader, writer)
        except asyncio.CancelledError:
            pass
        finally:
            if task is not None:
                self._relays.discard(task)
            if self._closed:
                # Forced shutdown: drop buffered output instead of waiting
                # for a local client that may never read it.
                _abort_writer(writer)
            else:
                await _close_writer(writer)

    async def _open_websocket(self) -> Any:
        ws_url, additional_headers, connect_kwargs = _prepare_async_ws_connect(
            self._ws_url, self._ws_headers
        )
        try:
            # ``open_timeout`` covers the TCP connect and the handshake.
            return await _async_ws_client.connect(
                ws_url,
                additional_headers=additional_headers,
                open_timeout=self._connect_timeout,
                ping_interval=WEBSOCKET_KEEPALIVE_INTERVAL,
                max_size=None,
                **connect_kwargs,
            )
        except (asyncio.TimeoutError, TimeoutError) as e:
            raise SandboxError(
                "timed out while connecting tunnel websocket after "
                f"{self._connect_timeout:.2f}s"
            ) from e
        except Exception as e:
            raise SandboxConnectionError(
                f"tunnel websocket connection failed: {e}"
            ) from e

    async def _relay(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        sock = writer.get_extra_info("socket")
        if sock is not None:
            _set_nodelay(sock)

        try:
            ws = await self._open_websocket()
        except Exception:
            return

        local_to_ws = asyncio.create_task(self._pump_local_to_ws(reader, ws))
        ws_to_local = asyncio.create_task(self._pump_ws_to_local(ws, writer))
        try:
            # ``local_to_ws`` closes the websocket when the local side is
            # done, so ``ws_to_local`` always ends. Waiting for it instead of
            # cancelling it delivers the frames the sandbox sent before it
            # answered the close.
            await ws_to_local
        finally:
            if not local_to_ws.done():
                local_to_ws.cancel()
            await asyncio.gather(local_to_ws, ws_to_local, return_exceptions=True)
            await _close_ws(ws)

    @staticmethod
    async def _pump_local_to_ws(reader: asyncio.StreamReader, ws: Any) -> None:
        try:
            while True:
                chunk = await reader.read(TUNNEL_BUFFER_SIZE)
                if not chunk:
                    return
                await ws.send(chunk)
        except Exception:
            return
        finally:
            # Local side finished or failed; close the websocket so the
            # sandbox sees EOF. The ws->local pump keeps delivering frames
            # received before the close completes, then ends.
            await _close_ws(ws)

    @staticmethod
    async def _pump_ws_to_local(ws: Any, writer: asyncio.StreamWriter) -> None:
        try:
            async for message in ws:
                if isinstance(message, str):
                    # The tunnel protocol is binary only.
                    return
                if writer.is_closing():
                    return
                writer.write(message)
                await writer.drain()
        except Exception:
            return
