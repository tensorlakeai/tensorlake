"""Reader termination releases the socket independently of remote close timing."""

import asyncio
import struct
import threading
import unittest

from websockets.exceptions import ConnectionClosed
from websockets.sync.server import serve

from tensorlake.sandbox import AsyncPty, Pty
from tensorlake.sandbox.pty import OP_EXIT, OP_READY


def _options():
    return dict(
        session_id="cleanup-test",
        token="test-token",
        ws_url="ws://localhost/pty",
        ws_headers={},
        http_url="http://localhost/pty",
        http_headers={},
    )


class _Socket:
    def __init__(self, frames, close_error=False):
        self.frames = iter(frames)
        self.closed = False
        self.close_error = close_error

    def recv(self):
        return next(self.frames, None)

    def close(self):
        self.closed = True
        if self.close_error:
            raise OSError("close failed")


class _AsyncSocket(_Socket):
    def __aiter__(self):
        return self

    async def __anext__(self):
        frame = next(self.frames, None)
        if frame is None:
            await asyncio.Event().wait()
        return frame

    async def close(self):
        super().close()


class _ClosedAsyncSocket(_AsyncSocket):
    async def __anext__(self):
        raise StopAsyncIteration


def _failing_exit_handler(exit_code):
    raise ValueError("exit callback failed")


class _ExitServer:
    """A real peer that waits for client close after sending a PTY exit."""

    def __init__(self):
        self.closed_by_client = threading.Event()
        self.ready = None
        self.server = serve(self.handle, "127.0.0.1", 0)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def handle(self, socket):
        self.ready = socket.recv(timeout=5)
        socket.send(bytes([OP_EXIT]) + struct.pack(">i", 7))
        try:
            socket.recv(timeout=5)
        except ConnectionClosed:
            self.closed_by_client.set()

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.server.shutdown()
        self.thread.join(timeout=5)

    @property
    def url(self):
        return f"ws://127.0.0.1:{self.server.socket.getsockname()[1]}/pty"


class TestPtySocketCleanup(unittest.TestCase):
    def test_close_reason_callback_error_still_closes_socket(self):
        socket = _Socket([])
        socket.close_reason = "exit:7"
        pty = Pty(**_options())
        pty._ws = socket
        pty.on_exit(_failing_exit_handler)
        with self.assertRaisesRegex(ValueError, "exit callback failed"):
            pty._reader_loop(socket)
        self.assertTrue(socket.closed)
        self.assertEqual(pty.wait(timeout=0), 7)

    def test_real_peer_observes_close_after_exit(self):
        with _ExitServer() as server:
            pty = Pty(**{**_options(), "ws_url": server.url})
            try:
                pty.connect()
                self.assertEqual(pty.wait(timeout=5), 7)
                self.assertTrue(server.closed_by_client.wait(timeout=5))
                self.assertEqual(server.ready, bytes([OP_READY]))
            finally:
                pty.disconnect()

    def test_exit_frame_closes_socket(self):
        socket = _Socket([bytes([OP_EXIT]) + struct.pack(">i", 7)])
        pty = Pty(**_options())
        pty._ws = socket
        pty._reader_loop(socket)
        self.assertTrue(socket.closed)
        self.assertFalse(pty.connected)
        self.assertEqual(pty.wait(timeout=0), 7)

    def test_close_error_does_not_replace_exit_result(self):
        socket = _Socket([bytes([OP_EXIT]) + struct.pack(">i", 2)], close_error=True)
        pty = Pty(**_options())
        pty._ws = socket
        pty._reader_loop(socket)
        self.assertTrue(socket.closed)
        self.assertEqual(pty.wait(timeout=0), 2)


class TestAsyncPtySocketCleanup(unittest.IsolatedAsyncioTestCase):
    async def test_close_reason_callback_error_still_closes_socket(self):
        socket = _ClosedAsyncSocket([])
        socket.close_reason = "exit:7"
        pty = AsyncPty(**_options())
        pty._ws = socket
        pty.on_exit(_failing_exit_handler)
        with self.assertRaisesRegex(ValueError, "exit callback failed"):
            await pty._reader_loop(socket)
        self.assertTrue(socket.closed)
        self.assertEqual(await pty.wait(timeout=0.1), 7)

    async def test_real_peer_observes_close_after_exit(self):
        with _ExitServer() as server:
            pty = AsyncPty(**{**_options(), "ws_url": server.url})
            try:
                await pty.connect()
                self.assertEqual(await pty.wait(timeout=5), 7)
                self.assertTrue(
                    await asyncio.to_thread(server.closed_by_client.wait, 5)
                )
                self.assertEqual(server.ready, bytes([OP_READY]))
            finally:
                await pty.disconnect()

    async def test_exit_frame_closes_socket(self):
        socket = _AsyncSocket([bytes([OP_EXIT]) + struct.pack(">i", 7)])
        pty = AsyncPty(**_options())
        pty._ws = socket
        await pty._reader_loop(socket)
        self.assertTrue(socket.closed)
        self.assertFalse(pty.connected)
        self.assertEqual(await pty.wait(timeout=0.1), 7)

    async def test_close_error_does_not_replace_exit_result(self):
        socket = _AsyncSocket(
            [bytes([OP_EXIT]) + struct.pack(">i", 2)], close_error=True
        )
        pty = AsyncPty(**_options())
        pty._ws = socket
        await pty._reader_loop(socket)
        self.assertTrue(socket.closed)
        self.assertEqual(await pty.wait(timeout=0.1), 2)

    async def test_reader_cancellation_closes_socket(self):
        socket = _AsyncSocket([])
        pty = AsyncPty(**_options())
        pty._ws = socket
        reader = asyncio.create_task(pty._reader_loop(socket))
        await asyncio.sleep(0)
        reader.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await reader
        self.assertTrue(socket.closed)
        self.assertFalse(pty.connected)
