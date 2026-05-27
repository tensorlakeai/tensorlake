"""Programmatic PTY handles for interactive sandbox sessions.

Provides :class:`Pty` (sync, thread-based) and :class:`AsyncPty` (asyncio-based)
for driving sandbox PTY sessions over WebSocket.
"""

from __future__ import annotations

import asyncio
import struct
import threading
from collections.abc import Callable
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import httpx

from tensorlake._tracing import USER_AGENT, inject_traceparent

from .exceptions import RemoteAPIError, SandboxConnectionError, SandboxError

try:
    import websocket
except Exception:  # pragma: no cover - exercised via runtime guard
    websocket = None

try:
    import websockets.asyncio.client as _async_ws_client
except Exception:  # pragma: no cover - exercised via runtime guard
    _async_ws_client = None

OP_DATA = 0x00
OP_RESIZE = 0x01
OP_READY = 0x02
OP_EXIT = 0x03


def _prepare_async_ws_connect(
    ws_url: str, ws_headers: dict[str, str]
) -> tuple[str, list[tuple[str, str]], dict[str, Any]]:
    """Split a custom Host header out of additional headers for websockets.connect.

    The websockets library appends headers from ``additional_headers`` without
    deduplicating, so passing ``Host`` there yields two Host headers in the
    upgrade request — which HTTP/1.1 proxies reject. The library builds its own
    Host header from the URI, so embed the override in the URI's netloc and
    pin the TCP target back to the original host/port via explicit kwargs.
    """
    headers = dict(ws_headers)
    host_override = headers.pop("Host", None)
    connect_kwargs: dict[str, Any] = {}
    if host_override is not None:
        parsed = urlparse(ws_url)
        if parsed.hostname:
            connect_kwargs["host"] = parsed.hostname
        if parsed.port:
            connect_kwargs["port"] = parsed.port
        ws_url = urlunparse(parsed._replace(netloc=host_override))
    return ws_url, list(headers.items()), connect_kwargs


def _ensure_token_query_param(ws_url: str, token: str) -> tuple[str, str]:
    parsed = urlparse(ws_url)
    query = parse_qs(parsed.query, keep_blank_values=True)
    token_values = query.get("token")
    auth_token = token_values[0] if token_values else token
    query["token"] = [auth_token]
    query_string = urlencode(query, doseq=True)
    return urlunparse(parsed._replace(query=query_string)), auth_token


def _parse_exit_from_close_reason(close_reason: str) -> int | None:
    """Return the exit code embedded in a ``exit:N`` close reason, else None."""
    if not close_reason.startswith("exit:"):
        return None
    try:
        return int(close_reason[5:])
    except ValueError:
        return -1


def _resize_frame(cols: int, rows: int) -> bytes:
    return struct.pack(">BHH", OP_RESIZE, cols, rows)


def _data_frame(data: str | bytes | bytearray) -> bytes:
    payload = data.encode("utf-8") if isinstance(data, str) else bytes(data)
    return bytes([OP_DATA]) + payload


def _parse_frame(data: bytes) -> tuple[int, bytes] | None:
    """Decode a binary frame. Returns (opcode, payload) or None for ignorable input."""
    if not data:
        return None
    opcode = data[0]
    if opcode == OP_DATA:
        return OP_DATA, data[1:]
    if opcode == OP_EXIT and len(data) >= 5:
        return OP_EXIT, data[1:5]
    return opcode, b""


class Pty:
    """High-level PTY session handle.

    A PTY handle owns the websocket connection used for interactive shell I/O
    and exposes simple methods like ``send_input()``, ``resize()``, ``wait()``,
    and ``kill()``.
    """

    def __init__(
        self,
        *,
        session_id: str,
        token: str,
        ws_url: str,
        ws_headers: dict[str, str],
        http_url: str,
        http_headers: dict[str, str],
        connect_timeout: float = 10.0,
    ):
        self.session_id = session_id
        self.token = token
        self._ws_url = ws_url
        self._ws_headers = ws_headers
        self._http_url = http_url
        self._http_headers = http_headers
        self._connect_timeout = connect_timeout

        self._lock = threading.RLock()
        self._ws = None
        self._reader_thread: threading.Thread | None = None
        self._disconnecting_sockets: set[object] = set()

        self._exit_code: int | None = None
        self._wait_error: Exception | None = None
        self._wait_event = threading.Event()

        self._data_handlers: list[Callable[[bytes], None]] = []
        self._exit_handlers: list[Callable[[int], None]] = []

    @property
    def connected(self) -> bool:
        with self._lock:
            return self._ws is not None

    def on_data(self, handler: Callable[[bytes], None]) -> "Pty":
        with self._lock:
            self._data_handlers.append(handler)
        return self

    def on_exit(self, handler: Callable[[int], None]) -> "Pty":
        with self._lock:
            self._exit_handlers.append(handler)
            exit_code = self._exit_code
        if exit_code is not None:
            handler(exit_code)
        return self

    def connect(self) -> "Pty":
        if websocket is None:
            raise SandboxError(
                "websocket-client is required for PTY sessions. "
                "Install the Tensorlake package with its runtime dependencies."
            )

        with self._lock:
            if self._wait_event.is_set() and self._exit_code is not None:
                raise SandboxError("PTY session has already exited")
            if self._ws is not None:
                return self

            try:
                ws = websocket.create_connection(
                    self._ws_url,
                    header=[
                        f"{key}: {value}" for key, value in self._ws_headers.items()
                    ],
                    timeout=self._connect_timeout,
                    enable_multithread=True,
                )
            except Exception as e:
                raise SandboxConnectionError(
                    f"PTY websocket connection failed: {e}"
                ) from e

            self._ws = ws

        self._reader_thread = threading.Thread(
            target=self._reader_loop,
            args=(ws,),
            name=f"tensorlake-pty-{self.session_id}",
            daemon=True,
        )
        self._reader_thread.start()
        self._send_binary(bytes([OP_READY]))
        return self

    def send_input(self, data: str | bytes | bytearray) -> None:
        self._send_binary(_data_frame(data))

    def resize(self, cols: int, rows: int) -> None:
        self._send_binary(_resize_frame(cols, rows))

    def disconnect(self, code: int = 1000, reason: str = "client disconnect") -> None:
        with self._lock:
            ws = self._ws
            self._ws = None
            if ws is not None:
                self._disconnecting_sockets.add(ws)
        if ws is not None:
            try:
                ws.close(status=code, reason=reason)
            except TypeError:
                ws.close()
            except Exception:
                pass

    def wait(self, timeout: float | None = None) -> int:
        if not self._wait_event.wait(timeout):
            raise TimeoutError("PTY session did not exit before the timeout")
        if self._wait_error is not None:
            raise self._wait_error
        return self._exit_code if self._exit_code is not None else -1

    def kill(self) -> None:
        try:
            response = httpx.delete(
                self._http_url,
                headers=inject_traceparent(
                    {**self._http_headers, "User-Agent": USER_AGENT}
                ),
                timeout=self._connect_timeout,
            )
        except httpx.HTTPError as e:
            raise SandboxConnectionError(f"Failed to kill PTY session: {e}") from e

        if response.is_success:
            return

        raise RemoteAPIError(response.status_code, response.text)

    def _send_binary(self, payload: bytes) -> None:
        with self._lock:
            ws = self._ws
        if ws is None:
            raise SandboxError("PTY is not connected")

        try:
            ws.send_binary(payload)
        except Exception as e:
            raise SandboxConnectionError(f"PTY websocket send failed: {e}") from e

    def _reader_loop(self, ws) -> None:
        close_error: Exception | None = None

        try:
            while True:
                try:
                    frame = ws.recv()
                except Exception as e:
                    close_error = e
                    break

                if frame is None:
                    break
                if isinstance(frame, str):
                    continue

                parsed = _parse_frame(bytes(frame))
                if parsed is None:
                    continue

                opcode, payload = parsed
                if opcode == OP_DATA:
                    with self._lock:
                        handlers = list(self._data_handlers)
                    for handler in handlers:
                        handler(payload)
                    continue

                if opcode == OP_EXIT and len(payload) == 4:
                    exit_code = struct.unpack(">i", payload)[0]
                    self._finish(exit_code)
                    break
        finally:
            with self._lock:
                if self._ws is ws:
                    self._ws = None
                intentional_disconnect = ws in self._disconnecting_sockets
                if intentional_disconnect:
                    self._disconnecting_sockets.discard(ws)

            if not self._wait_event.is_set():
                close_reason = getattr(ws, "close_reason", "") or ""
                close_code = getattr(ws, "close_status_code", None)

                exit_code = _parse_exit_from_close_reason(close_reason)
                if exit_code is not None:
                    self._finish(exit_code)
                elif intentional_disconnect:
                    pass
                elif close_reason == "session terminated":
                    self._fail(SandboxError("PTY session terminated"))
                elif close_error is not None:
                    self._fail(
                        SandboxConnectionError(
                            f"PTY websocket closed unexpectedly: {close_error}"
                        )
                    )
                else:
                    self._fail(
                        SandboxError(
                            f"PTY websocket closed unexpectedly: {close_code} {close_reason}".strip()
                        )
                    )

    def _finish(self, exit_code: int) -> None:
        with self._lock:
            if self._wait_event.is_set():
                return
            self._exit_code = exit_code
            handlers = list(self._exit_handlers)
            self._wait_event.set()

        for handler in handlers:
            handler(exit_code)

    def _fail(self, error: Exception) -> None:
        with self._lock:
            if self._wait_event.is_set():
                return
            self._wait_error = error
            self._wait_event.set()


class AsyncPty:
    """Async high-level PTY session handle.

    Mirrors :class:`Pty` but uses asyncio primitives and the ``websockets``
    library, so it integrates with an asyncio event loop instead of owning
    a background thread.
    """

    def __init__(
        self,
        *,
        session_id: str,
        token: str,
        ws_url: str,
        ws_headers: dict[str, str],
        http_url: str,
        http_headers: dict[str, str],
        connect_timeout: float = 10.0,
    ):
        self.session_id = session_id
        self.token = token
        self._ws_url = ws_url
        self._ws_headers = ws_headers
        self._http_url = http_url
        self._http_headers = http_headers
        self._connect_timeout = connect_timeout

        self._lock = asyncio.Lock()
        self._ws = None
        self._reader_task: asyncio.Task | None = None
        self._disconnecting_sockets: set[int] = set()

        self._exit_code: int | None = None
        self._wait_error: Exception | None = None
        self._wait_event = asyncio.Event()

        self._data_handlers: list[Callable[[bytes], None]] = []
        self._exit_handlers: list[Callable[[int], None]] = []

    @property
    def connected(self) -> bool:
        return self._ws is not None

    def on_data(self, handler: Callable[[bytes], None]) -> "AsyncPty":
        self._data_handlers.append(handler)
        return self

    def on_exit(self, handler: Callable[[int], None]) -> "AsyncPty":
        self._exit_handlers.append(handler)
        if self._exit_code is not None:
            handler(self._exit_code)
        return self

    async def connect(self) -> "AsyncPty":
        if _async_ws_client is None:
            raise SandboxError(
                "websockets is required for AsyncPty sessions. "
                "Install the Tensorlake package with its runtime dependencies."
            )

        async with self._lock:
            if self._wait_event.is_set() and self._exit_code is not None:
                raise SandboxError("PTY session has already exited")
            if self._ws is not None:
                return self

            try:
                ws_url, additional_headers, connect_kwargs = (
                    _prepare_async_ws_connect(self._ws_url, self._ws_headers)
                )
                ws = await asyncio.wait_for(
                    _async_ws_client.connect(
                        ws_url,
                        additional_headers=additional_headers,
                        open_timeout=self._connect_timeout,
                        **connect_kwargs,
                    ),
                    timeout=self._connect_timeout,
                )
            except Exception as e:
                raise SandboxConnectionError(
                    f"PTY websocket connection failed: {e}"
                ) from e

            self._ws = ws

        self._reader_task = asyncio.create_task(
            self._reader_loop(ws),
            name=f"tensorlake-async-pty-{self.session_id}",
        )
        await self._send_binary(bytes([OP_READY]))
        return self

    async def send_input(self, data: str | bytes | bytearray) -> None:
        await self._send_binary(_data_frame(data))

    async def resize(self, cols: int, rows: int) -> None:
        await self._send_binary(_resize_frame(cols, rows))

    async def disconnect(
        self, code: int = 1000, reason: str = "client disconnect"
    ) -> None:
        async with self._lock:
            ws = self._ws
            self._ws = None
            if ws is not None:
                self._disconnecting_sockets.add(id(ws))
        if ws is not None:
            try:
                await ws.close(code=code, reason=reason)
            except Exception:
                pass

    async def wait(self, timeout: float | None = None) -> int:
        try:
            if timeout is None:
                await self._wait_event.wait()
            else:
                await asyncio.wait_for(self._wait_event.wait(), timeout=timeout)
        except asyncio.TimeoutError as e:
            raise TimeoutError("PTY session did not exit before the timeout") from e
        if self._wait_error is not None:
            raise self._wait_error
        return self._exit_code if self._exit_code is not None else -1

    async def kill(self) -> None:
        headers = inject_traceparent({**self._http_headers, "User-Agent": USER_AGENT})
        try:
            async with httpx.AsyncClient(timeout=self._connect_timeout) as client:
                response = await client.delete(self._http_url, headers=headers)
        except httpx.HTTPError as e:
            raise SandboxConnectionError(f"Failed to kill PTY session: {e}") from e

        if response.is_success:
            return

        raise RemoteAPIError(response.status_code, response.text)

    async def _send_binary(self, payload: bytes) -> None:
        ws = self._ws
        if ws is None:
            raise SandboxError("PTY is not connected")

        try:
            await ws.send(payload)
        except Exception as e:
            raise SandboxConnectionError(f"PTY websocket send failed: {e}") from e

    async def _reader_loop(self, ws) -> None:
        close_error: Exception | None = None

        try:
            async for frame in ws:
                if isinstance(frame, str):
                    continue

                parsed = _parse_frame(bytes(frame))
                if parsed is None:
                    continue

                opcode, payload = parsed
                if opcode == OP_DATA:
                    for handler in list(self._data_handlers):
                        handler(payload)
                    continue

                if opcode == OP_EXIT and len(payload) == 4:
                    exit_code = struct.unpack(">i", payload)[0]
                    self._finish(exit_code)
                    return
        except Exception as e:
            close_error = e
        finally:
            if self._ws is ws:
                self._ws = None
            intentional_disconnect = id(ws) in self._disconnecting_sockets
            self._disconnecting_sockets.discard(id(ws))

            if not self._wait_event.is_set():
                close_reason = ""
                close_code = None
                close_obj = getattr(ws, "close_code", None)
                if close_obj is not None:
                    close_code = close_obj
                reason_obj = getattr(ws, "close_reason", None)
                if reason_obj:
                    close_reason = reason_obj

                exit_code = _parse_exit_from_close_reason(close_reason)
                if exit_code is not None:
                    self._finish(exit_code)
                elif intentional_disconnect:
                    pass
                elif close_reason == "session terminated":
                    self._fail(SandboxError("PTY session terminated"))
                elif close_error is not None:
                    self._fail(
                        SandboxConnectionError(
                            f"PTY websocket closed unexpectedly: {close_error}"
                        )
                    )
                else:
                    self._fail(
                        SandboxError(
                            f"PTY websocket closed unexpectedly: {close_code} {close_reason}".strip()
                        )
                    )

    def _finish(self, exit_code: int) -> None:
        if self._wait_event.is_set():
            return
        self._exit_code = exit_code
        handlers = list(self._exit_handlers)
        self._wait_event.set()
        for handler in handlers:
            handler(exit_code)

    def _fail(self, error: Exception) -> None:
        if self._wait_event.is_set():
            return
        self._wait_error = error
        self._wait_event.set()


def build_pty_connection(
    *,
    session_id: str,
    token: str,
    ws_url: str,
    http_url: str,
    ws_headers: dict[str, str],
    http_headers: dict[str, str],
    connect_timeout: float,
) -> Pty:
    ws_url_with_query, auth_token = _ensure_token_query_param(ws_url, token)

    full_ws_headers = dict(ws_headers)
    full_ws_headers["X-PTY-Token"] = auth_token

    return Pty(
        session_id=session_id,
        token=auth_token,
        ws_url=ws_url_with_query,
        ws_headers=full_ws_headers,
        http_url=http_url,
        http_headers=http_headers,
        connect_timeout=connect_timeout,
    )


def build_async_pty_connection(
    *,
    session_id: str,
    token: str,
    ws_url: str,
    http_url: str,
    ws_headers: dict[str, str],
    http_headers: dict[str, str],
    connect_timeout: float,
) -> AsyncPty:
    ws_url_with_query, auth_token = _ensure_token_query_param(ws_url, token)

    full_ws_headers = dict(ws_headers)
    full_ws_headers["X-PTY-Token"] = auth_token

    return AsyncPty(
        session_id=session_id,
        token=auth_token,
        ws_url=ws_url_with_query,
        ws_headers=full_ws_headers,
        http_url=http_url,
        http_headers=http_headers,
        connect_timeout=connect_timeout,
    )
