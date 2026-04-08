"""Programmatic PTY handle for interactive sandbox sessions."""

from __future__ import annotations

import struct
import threading
from collections.abc import Callable
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import httpx

from .exceptions import RemoteAPIError, SandboxConnectionError, SandboxError

try:
    import websocket
except Exception:  # pragma: no cover - exercised via runtime guard
    websocket = None

OP_DATA = 0x00
OP_RESIZE = 0x01
OP_READY = 0x02
OP_EXIT = 0x03


def _ensure_token_query_param(ws_url: str, token: str) -> tuple[str, str]:
    parsed = urlparse(ws_url)
    query = parse_qs(parsed.query, keep_blank_values=True)
    token_values = query.get("token")
    auth_token = token_values[0] if token_values else token
    query["token"] = [auth_token]
    query_string = urlencode(query, doseq=True)
    return urlunparse(parsed._replace(query=query_string)), auth_token


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
                    header=[f"{key}: {value}" for key, value in self._ws_headers.items()],
                    timeout=self._connect_timeout,
                    enable_multithread=True,
                )
            except Exception as e:
                raise SandboxConnectionError(f"PTY websocket connection failed: {e}") from e

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
        payload = data.encode("utf-8") if isinstance(data, str) else bytes(data)
        self._send_binary(bytes([OP_DATA]) + payload)

    def resize(self, cols: int, rows: int) -> None:
        frame = struct.pack(">BHH", OP_RESIZE, cols, rows)
        self._send_binary(frame)

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
                headers=self._http_headers,
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

                data = bytes(frame)
                if not data:
                    continue

                opcode = data[0]
                if opcode == OP_DATA:
                    payload = data[1:]
                    with self._lock:
                        handlers = list(self._data_handlers)
                    for handler in handlers:
                        handler(payload)
                    continue

                if opcode == OP_EXIT and len(data) >= 5:
                    exit_code = struct.unpack(">i", data[1:5])[0]
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

                if close_reason.startswith("exit:"):
                    try:
                        self._finish(int(close_reason[5:]))
                    except ValueError:
                        self._finish(-1)
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
