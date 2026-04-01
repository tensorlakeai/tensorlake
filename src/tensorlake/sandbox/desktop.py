"""Desktop control wrapper for sandbox VNC sessions."""

from __future__ import annotations

from collections.abc import Sequence

from .exceptions import RemoteAPIError, SandboxConnectionError, SandboxError

try:
    from tensorlake._cloud_sdk import (
        CloudSandboxClientError as RustCloudSandboxClientError,
    )
except Exception:
    try:
        from _cloud_sdk import CloudSandboxClientError as RustCloudSandboxClientError
    except Exception:
        RustCloudSandboxClientError = None


def _parse_rust_client_error_fields(
    e: Exception,
) -> tuple[str | None, int | None, str]:
    kind: str | None = None
    status_code: int | None = None
    message = str(e)

    if len(e.args) == 3:
        kind, status_code, message = e.args
    elif len(e.args) == 1 and isinstance(e.args[0], tuple) and len(e.args[0]) == 3:
        kind, status_code, message = e.args[0]

    return kind, status_code, message


def _raise_as_sandbox_error(e: Exception) -> None:
    if isinstance(e, SandboxError):
        raise

    if (
        RustCloudSandboxClientError is not None
        and isinstance(e, RustCloudSandboxClientError)
        and len(e.args) > 0
    ):
        kind, status_code, message = _parse_rust_client_error_fields(e)
        if kind == "connection":
            raise SandboxConnectionError(message) from None
        if status_code is not None:
            raise RemoteAPIError(status_code, message) from None
        raise SandboxError(message) from None

    raise SandboxError(str(e)) from e


class Desktop:
    """Programmatic desktop control for a sandbox VNC session."""

    def __init__(self, rust_client):
        self._rust_client = rust_client

    def __enter__(self) -> "Desktop":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def width(self) -> int:
        try:
            return int(self._rust_client.width)
        except Exception as e:
            _raise_as_sandbox_error(e)

    @property
    def height(self) -> int:
        try:
            return int(self._rust_client.height)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def close(self) -> None:
        try:
            self._rust_client.close()
        except Exception as e:
            _raise_as_sandbox_error(e)

    def screenshot(self, timeout: float = 5.0) -> bytes:
        try:
            return bytes(self._rust_client.screenshot_png(timeout))
        except Exception as e:
            _raise_as_sandbox_error(e)

    def move_mouse(self, x: int, y: int) -> None:
        try:
            self._rust_client.move_mouse(x, y)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def mouse_press(
        self, button: str = "left", x: int | None = None, y: int | None = None
    ) -> None:
        try:
            self._rust_client.mouse_press(button, x, y)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def mouse_release(
        self, button: str = "left", x: int | None = None, y: int | None = None
    ) -> None:
        try:
            self._rust_client.mouse_release(button, x, y)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def click(
        self, x: int | None = None, y: int | None = None, button: str = "left"
    ) -> None:
        try:
            self._rust_client.click(button, x, y)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def double_click(
        self,
        x: int | None = None,
        y: int | None = None,
        button: str = "left",
        delay_ms: int = 50,
    ) -> None:
        try:
            self._rust_client.double_click(button, x, y, delay_ms)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def left_click(self, x: int | None = None, y: int | None = None) -> None:
        self.click(x=x, y=y, button="left")

    def middle_click(self, x: int | None = None, y: int | None = None) -> None:
        self.click(x=x, y=y, button="middle")

    def right_click(self, x: int | None = None, y: int | None = None) -> None:
        self.click(x=x, y=y, button="right")

    def key_down(self, key: str) -> None:
        try:
            self._rust_client.key_down(key)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def key_up(self, key: str) -> None:
        try:
            self._rust_client.key_up(key)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def press(self, key: str | Sequence[str]) -> None:
        try:
            if isinstance(key, str):
                keys = [key]
            else:
                keys = list(key)
            self._rust_client.press(keys)
        except Exception as e:
            _raise_as_sandbox_error(e)

    def type_text(self, text: str) -> None:
        try:
            self._rust_client.type_text(text)
        except Exception as e:
            _raise_as_sandbox_error(e)
